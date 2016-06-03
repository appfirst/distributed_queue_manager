from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.socket_wrapper import SocketWrapper #@UnresolvedImport
from afqueue.common.zmq_utilities import ZmqUtilities #@UnresolvedImport
from afqueue.messages import client_messages, peer_messages, system_messages #@UnresolvedImport
from afqueue.messages import client_response_codes #@UnresolvedImport
from afqueue.messages.base_message import BaseMessage #@UnresolvedImport
import afqueue.messages.message_types as message_types #@UnresolvedImport
import time, random
import zmq, bson #@UnresolvedImport
from afqueue.config_files import queue_defines as defines #@UnresolvedImport
import os

from queue import Empty as EmptyQueueException #@UnresolvedImport
from afqueue.common import client_exchange_types #@UnresolvedImport
from afqueue.source.shared_memory_manager import SharedMemoryManager, RedisConnectionFailureException, RedisConnectionUnknownException #@UnresolvedImport


class DataWorkerThread:
        
    def __init__(self, thread_name, remote_ip_address, qm_id_string, qm_start_up_time, config, redis_connection_string,
                 worker_router_port, action_in_queue, action_out_queue):

        # Assign.
        self._thread_name = thread_name
        self.remote_ip_address = remote_ip_address
        self.qm_id_string = qm_id_string
        self.qm_start_up_time = qm_start_up_time
        self.config = config
        self.redis_connection_string = redis_connection_string
        self.worker_router_port = worker_router_port
        self.action_in_queue = action_in_queue
        self.action_out_queue = action_out_queue
        
        # Capture configuration data we need.
        self.poll_interval = self.config.manager_data_worker_poll_interval
        self.no_activity_sleep_interval = self.config.manager_data_worker_no_activity_sleep
        self.shared_memory_validation_check_count = self.config.manager_data_worker_shared_memory_validation_check_count
        self.status_report_send_time_interval = self.config.manager_data_worker_status_report_send_time_interval
        
        # Denote running.
        self.is_running = True
        
        # Create space for our shared memory manager; we will create this when we enter our run loop.
        self.shared_memory_manager = None
                
        # Local memory copies of shared memory; synchronized against shared memory when requested by the main thread.
        self._current_exchange_wrapper_dict = dict()
        self._current_data_queue_wrapper_dict = dict()
        self._current_pecking_order_list = list()        
        self._current_queue_lock_owner_dict = dict()      
        self._current_ordered_queue_owners_dict = dict()
        self._current_push_rejection_queue_name_set = set()
        self._current_routing_key_rejection_list = list()  
        self._current_accepting_data_owner_id_list = list()
        self._current_frozen_push_queue_set = set()
        self._current_frozen_pull_queue_set = set()
        
        # Remote shared memory trackers.
        self._pqm_queue_access_dictionary = dict()
        self._pqm_access_order_list = list()
        
        # Status report trackers.
        self.last_status_report_send_time = int(time.time())
        self.queue_data_pushed_status_report = dict()
        self.queue_data_popped_status_report = dict()
        self.queue_data_requeued_status_report = dict()
        self.synchronization_status_report = list()
        self.updated_remote_shared_memory_connections_status_report = dict()
            
    
    def run_logic(self):
        """
        This thread is responsible for doing data related work - putting data into or taking data out of queues.
        Note that this thread will push requests onto the notification queue as necessary (to signal queue creation, data request from other QMs, etc).
        This worker creates a REQ socket and connects to the data broker's ROUTER socket.
        An initial READY request message is sent to out signaling the worker is ready for work.
        Data to work on is sent back in the reply message.
        Work is completed and sent back in the next REQ message.
        The data broker replies to the REQ message with more work. 
        The architecture allows for an arbitrary number of these worker threads connected to the data broker.
        """
                
        # Send a system message to notify we have started our thread.
        self.action_out_queue.put(system_messages.SystemThreadStateMessage(self._thread_name, True, os.getpid()))    
        
        # Connect to shared memory.
        self.shared_memory_manager = SharedMemoryManager(self.redis_connection_string)
        
        # Create the request socket.
        connection_string = ZmqUtilities.get_socket_connection_string("localhost", self.worker_router_port)
        zmq_context = zmq.Context()
        self.request_socket = zmq_context.socket(zmq.REQ)
        self.request_socket.setsockopt(zmq.IDENTITY, self._thread_name.encode())
        self.request_socket.setsockopt(zmq.LINGER, 0)
        self.request_socket.connect(connection_string)   
        
        # Send a system message to notify we have opened our socket.
        self.action_out_queue.put(system_messages.SystemSocketStateMessage(self._thread_name, True, "Request connected to {0}".format(connection_string)))
    
        # Use a poller so we can receive with timeouts (receive is blocking; we can poll with a timeout).
        # This will allow us to see if we should shut down the socket in between polls.
        poller = zmq.Poller()
        poller.register(self.request_socket, zmq.POLLIN)
    
        # Tell the broker we are ready for work
        self.request_socket.send(b"READY")
    
        # Enter our loop.
        while self.is_running == True:   
            
            try:
                
                # Send a status report if our interval has passed.
                current_time_stamp = int(time.time())
                if current_time_stamp > self.status_report_send_time_interval + self.last_status_report_send_time:
                    self._send_status_report()
                    self.last_status_report_send_time = current_time_stamp

                # Poll on receiving with our time out.
                if poller.poll(self.poll_interval):
                       
                    # Always allow the action queue to respond first; it might update our exchanges, queues, etc.
                    self._consume_action_queue()
                
                    # Get the message; initialize to not handled.
                    full_raw_message = SocketWrapper.pull_message(self.request_socket)
                    message_handled = False
                    
                    # Pull the reply tag and raw message data out of the full raw message.
                    reply_id_tag = full_raw_message[0]
                    raw_message = full_raw_message[2:]             
                    
                    # Forward; if the message wasn't handled, notify.
                    if message_handled == self._handle(raw_message, reply_id_tag):
                        self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Unable to handle message: {0}".format(raw_message))) 
                    
                else:
                                            
                    # If we had no network messages, consume our action queue.
                    # If we found no messages in the action queue, sleep an interval since we did no work this loop iteration.
                    if self._consume_action_queue() > 0:
                        time.sleep(self.no_activity_sleep_interval)      
            
            except KeyboardInterrupt:
                
                # Ignore keyboard interrupts; the main thread will handle as desired. 
                pass                               
                    
            except zmq.ZMQError:
                
                # When we get an exception, log it and break from our loop.
                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "ZMQ error raised while processing request: " + ExceptionFormatter.get_message())) 
                break
    
            except:
                
                # When we get an exception, log it and break from our loop.
                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "General exception raised while processing request: " + ExceptionFormatter.get_message())) 
                break
                                    
        # Clean up the socket.
        self.request_socket.setsockopt(zmq.LINGER, 0)
        self.request_socket.close()
        poller.unregister(self.request_socket)
    
        # Shut down our main context.
        zmq_context.term()
        
        # Send the latest status report.
        self._send_status_report()
        
        # Send a system message to notify we have closed the socket.
        self.action_out_queue.put(system_messages.SystemSocketStateMessage(self._thread_name, False, "Request"))
        
        # Send a system message to notify we have shut down.
        self.action_out_queue.put(system_messages.SystemThreadStateMessage(self._thread_name, False))
                
                
    def _consume_action_queue(self):
        """
        Consumes all messages in the action_in queue for this thread.
        Returns the total number of messages found in the queue during this call.
        """
        
        # Initialize tracker.
        message_count = 0
        
        # Respond to requests put this thread's action queue.
        while True:
               
            try:
                             
                # Get a new message; initialize to not being handled. 
                # Note that we will be routed to our empty message exception if no messages are waiting.
                message = self.action_in_queue.get(False)
                message_count += 1  
                message_handled = False
    
                # Based on the message type, handle.
                if message.get_type() == message_types.SYSTEM_STOP_THREAD:
                    self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, "Shutting down per main thread request"))
                    message_handled = True
                    self.is_running = False
                        
                elif message.get_type() == message_types.SYSTEM_UPDATE_DATA_WORKER_CONTROL_DATA:
                    message_handled = self._update_control_data(message)
                        
                elif message.get_type() == message_types.SYSTEM_UPDATE_DATA_WORKER_SETUP_DATA:
                    message_handled = self._update_setup_data(message)
                        
                elif message.get_type() == message_types.SYSTEM_UPDATE_SHARED_MEMORY_CONNECTIONS:
                    message_handled = self._update_shared_memory_connections(message)
        
                elif message.get_type() == message_types.SYSTEM_SET_PQM_QUEUE_ACCESS_DATA:
                    message_handled = self._set_pqm_queue_access_data(message)
                    
                elif message.get_type() == message_types.SYSTEM_PUSH_LOCAL_QUEUE_DATA:
                    message_handled = self._push_local_queue_data(message)
                    
                # If the message hasn't been handled, notify.
                if message_handled == False:
                    self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Action queue message was not handled: {0}".format(message)))
                        
            except EmptyQueueException:
                
                # We are looping as we read from our queue object without waiting; as soon as we hit an empty message, we should break from the loop.
                break
            
            except:
                
                # When we get an exception, log it and break from our loop.
                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Action queue message processing raised exception: " + ExceptionFormatter.get_message()))
                self.is_running = False
                break   
        
        # Return the number of messages we saw.
        return message_count
    
        
    def _get_data_queue_wrapper(self, queue_name, synchronize_on_fail = True):
        """
        Returns the data queue wrapper with the name specified.
        If the lookup fails, synchronization with shared memory will take place if the flag is set.
        Note that the entire data queue wrapper is synchronized if the flag is set.
        """
        
        # Search local memory first for the exchange.
        data_queue_wrapper = self._current_data_queue_wrapper_dict.get(queue_name, None)
        
        # If the doesn't exist, synchronize with shared memory if we are supposed to.
        if data_queue_wrapper == None and synchronize_on_fail == True:
            self._current_data_queue_wrapper_dict = self.shared_memory_manager.get_queue_wrapper_dict()

        # Return the wrapper.
        return data_queue_wrapper
    
        
    def _get_exchange_wrapper(self, exchange_name, synchronize_on_fail = True):
        """
        Returns the exchange wrapper with the name specified.
        If the exchange wrapper lookup fails, synchronization with shared memory will take place if the flag is set.
        Note that synchronization only takes place on the top level object in this method.
        """
        
        # Search local memory first for the exchange.  
        exchange_wrapper = self._current_exchange_wrapper_dict.get(exchange_name, None)
        
        # If the exchange doesn't exist, synchronize with shared memory if we are supposed to.
        if exchange_wrapper == None and synchronize_on_fail == True:
            exchange_wrapper = self.shared_memory_manager.get_exchange_wrapper(exchange_name)
            self._current_exchange_wrapper_dict[exchange_name] = exchange_wrapper

        # Return the wrapper.
        return exchange_wrapper
    

    def _handle(self, raw_message, reply_id_tag):
        
        try:  

            # Translate and capture handler based on message type.
            # If the message type doesn't match any of our definitions, return out False from the method - the message could not be handled.
            raw_message_type = BaseMessage.get_raw_message_type(raw_message)
            if raw_message_type == message_types.CLIENT_DATA_PUSH_REQUEST:
                decoded_message = client_messages.ClientDataPushRequestMessage.create_from_received(raw_message)
                handler_method = self._handle_client_data_push_request

            elif raw_message_type == message_types.CLIENT_DATA_PULL_REQUEST:
                decoded_message = client_messages.ClientDataPullRequestMessage.create_from_received(raw_message)
                handler_method = self._handle_client_data_pull_request

            elif raw_message_type == message_types.CLIENT_DATA_STORE_REQUEST:
                decoded_message = client_messages.ClientDataStoreRequestMessage.create_from_received(raw_message)
                handler_method = self._handle_client_data_store_request

            elif raw_message_type == message_types.CLIENT_DATA_RETRIEVE_REQUEST:
                decoded_message = client_messages.ClientDataRetrieveRequestMessage.create_from_received(raw_message)
                handler_method = self._handle_client_data_retrieve_request

            elif raw_message_type == message_types.CLIENT_GET_DATA_STORES_REQUEST:
                decoded_message = client_messages.ClientGetDataStoresRequestMessage.create_from_received(raw_message)
                handler_method = self._handle_client_get_data_stores_request

            elif raw_message_type == message_types.CLIENT_GET_PECKING_ORDER_REQUEST:
                decoded_message = client_messages.ClientGetPeckingOrderRequestMessage.create_from_received(raw_message)
                handler_method = self._handle_client_get_pecking_order_request

            elif raw_message_type == message_types.CLIENT_DECLARE_EXCHANGES_REQUEST:
                decoded_message = client_messages.ClientDeclareExchangesRequestMessage.create_from_received(raw_message)
                handler_method = self._handle_client_declare_exchanges_request

            elif raw_message_type == message_types.CLIENT_DECLARE_QUEUES_REQUEST:
                decoded_message = client_messages.ClientDeclareQueuesRequestMessage.create_from_received(raw_message)
                handler_method = self._handle_client_declare_queues_request
                
            elif raw_message_type == message_types.CLIENT_LOCK_QUEUES_REQUEST:
                decoded_message = client_messages.ClientLockQueuesRequestMessage.create_from_received(raw_message)
                handler_method = self._handle_client_lock_queues_request

            elif raw_message_type == message_types.CLIENT_UNLOCK_QUEUES_REQUEST:
                decoded_message = client_messages.ClientUnlockQueuesRequestMessage.create_from_received(raw_message)
                handler_method = self._handle_client_unlock_queues_request

            elif raw_message_type == message_types.CLIENT_DELETE_QUEUES_REQUEST:
                decoded_message = client_messages.ClientDeleteQueuesRequestMessage.create_from_received(raw_message)
                handler_method = self._handle_client_delete_queues_request

            elif raw_message_type == message_types.CLIENT_REQUEUE_DATA_REQUEST:                
                decoded_message = client_messages.ClientRequeueDataRequestMessage.create_from_received(raw_message)
                handler_method = self._handle_client_requeue_data_request

            elif raw_message_type == message_types.PEER_ONLINE_HANDSHAKE_REQUEST:
                decoded_message = raw_message
                handler_method = self._handle_peer_online_handshake_request
            
            else:                
                return False
            
            # If the decoded message is not none, send it to the handler we set.
            if decoded_message != None:
                return handler_method(decoded_message, reply_id_tag)
            
            # If the decoded message is None, there was an error in decoding.
            # We must manually send an error back along the request socket.
            else:
                reply_message = client_messages.ClientDataPushReplyMessage(reply_id_tag, client_response_codes.MALFORMED_MESSAGE, "")
                reply_message.send(self.request_socket)
        
        except:
            
            raise ExceptionFormatter.get_full_exception() 
    
    
    def _pull_remote_queue_data(self, pqm_id_string, queue_name, request_count):
        """
        Will attempt to pull the requested amount of data for the given queue name from the given PQM ID string's shared memory.
        Will return two items:
            1) The list of data retrieved (can be an empty list on failure or no data available)
            2) The amount of data remaining in the queue queried.
        """
                                    
        # Ask the remote storage for data.
        # If we get a connection error, remove this PQM from remote tracking and continue.  
        try:
            
            # Query.
            remote_data_list, remote_remaining_data_count = self.shared_memory_manager.query_remote_shared_memory(pqm_id_string, self.shared_memory_manager.pop_queue_data, queue_name, request_count)
                                     
            # Return.
            return remote_data_list, remote_remaining_data_count
                
        except RedisConnectionFailureException:
            self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "PQM's shared memory connection could not be established during remote pull.  PQM ID: {0}".format(pqm_id_string)))
            return list(), 0
                
        except RedisConnectionUnknownException:
            self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "PQM's shared memory connection was unknown during remote pull.  PQM ID: {0}".format(pqm_id_string)))
            return list(), 0
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
    
    def _pull_direct_data(self, queue_name, request_count, results_dictionary):
        """
        Will perform a standard data pull (used for distributed queue pulls).
        Will attempt to pull the full requested count from the queue name given, attempting to fulfill the request locally first.
        Stores results in the given results dictionary, keyed off queue name, valued off a tuple of (data_list, remaining_data_count).
        """
        
        try:
            
            # Attempt a local pull first.
            # Get the data requested.
            data_list, remaining_data_count = self.shared_memory_manager.pop_queue_data(queue_name, request_count)
            
            # If we didn't get all the data requested, check PQMs.
            request_count = request_count - len(data_list)
            if request_count > 0:
                
                # Check PQM accessible queues to see if a PQM's shared memory can fulfill our request.
                # Note that we pop the accessible PQMs from the access list as we process.
                #    The shared memory access will tell us how much data is remaining; if more data is remaining, we will push the PQM back onto the end of the access list.
                
                # If the queue name is in our accessible queues dictionary.
                if queue_name in list(self._pqm_queue_access_dictionary.keys()):
                    
                    # Get the PQM list.
                    pqm_id_string_list = self._pqm_queue_access_dictionary[queue_name]
                    while len(pqm_id_string_list):
                        
                        # Pop the first PQM ID string regardless of the result
                        # We won't want to check it for this queue again until the next access update from the main thread.
                        pqm_id_string = pqm_id_string_list.pop(0)
                                                                  
                        # Perform the remote pull. 
                        remote_data_list, remote_remaining_data_count = self._pull_remote_queue_data(pqm_id_string, queue_name, request_count)
                                                            
                        # Extend the remote data into our data list; decrement our remaining request amount by how much data we just got
                        data_list.extend(remote_data_list)
                        request_count -= len(remote_data_list)

                        # If there is remaining data, push this PQM back to the end of the list.
                        if remote_remaining_data_count > 0:
                            remaining_data_count += remote_remaining_data_count
                            pqm_id_string_list.append(pqm_id_string)
                        
                        # If we have all of our requested data, stop processing.
                        if request_count <= 0:
                            
                            # If we still have PQM ID strings left in our list, we are not sure there is no data left in remote shared memory.
                            # Ensure the remaining data count is set to 1.
                            if len(pqm_id_string_list) > 0:
                                remaining_data_count += 1
                            break
                        
            # If we got data, include in our response.
            popped_data_count = len(data_list)
            if popped_data_count > 0:
                
                # Store.
                results_dictionary[queue_name] = (data_list, remaining_data_count)           
                                
                # Increment our status tracker.
                if queue_name not in list(self.queue_data_popped_status_report.keys()):
                    self.queue_data_popped_status_report[queue_name] = 0
                self.queue_data_popped_status_report[queue_name] += popped_data_count
                
        except:
            
            raise ExceptionFormatter.get_full_exception()
         
        
    def _pull_ordered_data(self, owner_id_string_list, queue_name, request_count, results_dictionary):
        """
        Will perform a data pull from an ordered queue.
        Will attempt to pull the full requested count from the queue name given, attempting to fulfill the request locally first.
        Stores results in the given results dictionary, keyed off queue name, valued off a tuple of (data_list, remaining_data_count).
        """
        
        try:
            
            # Return out immediately if we have no owners.
            if len(owner_id_string_list) == 0:
                return 
            
            # Track the data we've pulled and how much data is remaining in our last queried shared memory.
            pulled_data_list = list()
            remaining_data_count = 0
            
            # Track the owner ID strings of the shared memories which we have exhausted during the pull process.
            exhausted_owner_id_string_list = list()
            
            # Pull from each element in the owner ID list until we have retrieved all data.
            for owner_id_string in owner_id_string_list:
                
                # Pull locally if this is our own ID string.
                if owner_id_string == self.qm_id_string:
                    current_pulled_data_list, remaining_data_count = self.shared_memory_manager.pop_queue_data(queue_name, request_count)
                else:
                    current_pulled_data_list, remaining_data_count = self._pull_remote_queue_data(owner_id_string, queue_name, request_count) 

                # Extend the current pulled data into our data list; decrement our remaining request amount by how much data we just got.
                pulled_data_list.extend(current_pulled_data_list)
                request_count -= len(current_pulled_data_list)

                # If there was no remaining data, track that this owner in our exhausted list.
                if remaining_data_count == 0:
                    exhausted_owner_id_string_list.append(owner_id_string)
                  
                # If we have all of our requested data, stop processing.
                if request_count <= 0:
                    
                    # If we didn't get through all the owners in the owner ID list, ensure we respond with some data remaining.
                    # We aren't sure there is no more data left.
                    if owner_id_string != owner_id_string_list[-1]:
                        remaining_data_count += 1
                    break
                            
            # Include the data pulled in our result and status information.
            popped_data_count = len(pulled_data_list)
            if popped_data_count > 0:
            
                # Store.
                results_dictionary[queue_name] = (pulled_data_list, remaining_data_count)           
                                
                # Increment our status tracker.
                if queue_name not in list(self.queue_data_popped_status_report.keys()):
                    self.queue_data_popped_status_report[queue_name] = 0
                self.queue_data_popped_status_report[queue_name] += popped_data_count
                
            # Handle our exhausted owners if we have an option to remove at least one owner.
            if len(exhausted_owner_id_string_list) > 0 and len(owner_id_string_list) > 1:
                
                # The data worker threads (DWTs) must notify the main thread of all exhausted queues.
                # If this QM is a slave, it will notify the master.
                # The master QM will check the queues, update the owner list as appropriate, and send updates to the slaves / DWTs.
                self.action_out_queue.put(system_messages.SystemOrderedQueueExhaustedOwnersMessage(self._thread_name, queue_name, exhausted_owner_id_string_list))  
        
                # Remove locally.
                # This will prevent this thread from querying these owners again, until the next owners update from the main thread.
                # Note that the master QM will figure out these owners have no more data and update the slaves, who will update their worker threads.
                for owner_id_string in exhausted_owner_id_string_list:
                    if owner_id_string != owner_id_string_list[-1]:
                        owner_id_string_list.remove(owner_id_string)                

        except:
            
            raise ExceptionFormatter.get_full_exception()
            
    
    def _handle_client_get_data_stores_request(self, request_message, reply_id_tag):
        """
        Handles a client's request to get the most recent list of data stores in the QM network which are accepting data.
        """
        
        try:
            
            # Create our trackers.
            accepting_store_list = list()
            rejecting_store_list = list()
            
            # Determine the redis connection string we'll send the peer; we have to replace "localhost" with our remote IP address.
            if "localhost" in self.redis_connection_string:
                redis_connection_string = self.redis_connection_string.replace("localhost", self.remote_ip_address)
            else:
                redis_connection_string = self.redis_connection_string
            
            # Check ourself.
            if self.qm_id_string in self._current_accepting_data_owner_id_list:
                accepting_store_list.append(redis_connection_string)
            else:
                rejecting_store_list.append(redis_connection_string)
                                
            # Go through all PQMs.
            for qm_id_string, connection_information in list(self.shared_memory_manager._pqm_redis_connection_information_dict.items()):
                if qm_id_string in self._current_accepting_data_owner_id_list:
                    accepting_store_list.append(connection_information)
                else:
                    rejecting_store_list.append(connection_information)
                    
            # Form a reply
            response_code = client_response_codes.GET_DATA_STORES_SUCCESS
            reply_message = client_messages.ClientGetDataStoresReplyMessage(reply_id_tag, accepting_store_list, rejecting_store_list, response_code)
            reply_message.send(self.request_socket)
            
            notification = "Sent latest data stores list to client.  Accepting: {0}, Rejecting: {1}.".format(accepting_store_list, rejecting_store_list)
            self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, notification))
            
            # Denote the message has been handled.
            return True
             
        except:
            
            raise ExceptionFormatter.get_full_exception()
            
    
    def _handle_client_get_pecking_order_request(self, request_message, reply_id_tag):
        """
        Handles a client's request to get the most recent list of data stores in the QM network which are accepting data.
        """        
        try:            
            # Pull the current pecking order.
            pecking_order_list = self.shared_memory_manager.get_pecking_order_list()
                                
            # Form a reply.
            response_code = client_response_codes.GET_DATA_PECKING_ORDER_SUCCESS
            reply_message = client_messages.ClientGetPeckingOrderReplyMessage(reply_id_tag, pecking_order_list, response_code)
            reply_message.send(self.request_socket)
            
            # Notify.
            notification = "Sent latest pecking order list to client: {0}.".format(pecking_order_list)
            self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, notification))
            
            # Denote the message has been handled.
            return True
             
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
    
    def _handle_client_data_retrieve_request(self, request_message, reply_id_tag):
        """
        Handles a client's request to retrieve data.
        """
        
        try:
                    
            # Default to a successful retrieval.
            response_code = client_response_codes.DATA_RETRIEVE_SUCCESS
            
            # x
            return_dict = dict()
            for full_key in request_message.full_key_get_list:
                
                # Get the custom and location key from the parameter.
                custom_key, location_key = ZmqUtilities.get_store_and_location_keys(full_key)
                
                # Query local if the location is our own QM ID string.
                if location_key == self.qm_id_string:
                    return_dict[full_key] = self.shared_memory_manager.get_value(custom_key)
                    
                # Query remote if the location is not our own QM ID string.
                else:
                    
                    # Query.
                    success_flag, data = self._retrieve_data_in_remote_shared_memory(location_key, custom_key)
                    
                    # If we got an error, handle.
                    if success_flag == False:
                    
                        # Set the response code to failure.
                        response_code = client_response_codes.DATA_RETRIEVE_FAIL_REMOTE
    
                        # Log.
                        error_string = "*** Remote retrieval failed: {0} : {1} ***".format(location_key, custom_key)
                        self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, error_string))  
                        
                        # Break - we are always assigning all data to the same remote location so we have no reason to try again.
                        break
                
                    # If we didn't get an error, we succeeded in pulling.
                    else:                        
                        return_dict[full_key] = data
            
            # Remove each remote key.
            for full_key in request_message.full_key_delete_list:
                
                # Get the custom and location key from the parameter.
                custom_key, location_key = ZmqUtilities.get_store_and_location_keys(full_key)
                
                # Query local if the location is our own QM ID string.
                if location_key == self.qm_id_string:
                    self.shared_memory_manager.delete_value(custom_key)
                    
                # Query remote if the location is not our own QM ID string.
                else:
                    
                    # Query.
                    result = self._delete_data_in_remote_shared_memory(location_key, custom_key)
                    if result == False:
                    
                        # Set the response code to failure.
                        response_code = client_response_codes.DATA_DELETE_FAIL_REMOTE
    
                        # Log.
                        error_string = "*** Remote delete failed: {0} : {1} ***".format(location_key, custom_key)
                        self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, error_string))  
                        
                        # Break - we are always assigning all data to the same remote location so we have no reason to try again.
                        break
        
            # Form a reply
            reply_message = client_messages.ClientDataRetrieveReplyMessage(reply_id_tag, return_dict, response_code)
            reply_message.send(self.request_socket)
            
            notification = "Retrieved data"
            self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, notification))
            
            # Denote the message has been handled.
            return True
             
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
        
    def _handle_client_data_store_request(self, request_message, reply_id_tag):
        """
        Handles a client's request to store data.
        """
        
        try:
            # Get expire time.
            expire_seconds = request_message.expire_seconds
            if expire_seconds <= 0:
                expire_seconds = None
            
            # If we have no QMs which can accept data, reject the storage request.
            accepting_count = len(self._current_accepting_data_owner_id_list)
            if accepting_count == 0:
                
                # Form a reply.
                response_code = client_response_codes.DATA_STORE_FAIL_NO_FREE_SPACE
                reply_message = client_messages.ClientDataStoreReplyMessage(reply_id_tag, "", response_code)
                reply_message.send(self.request_socket)
                
                # Notify the main thread.
                notification = "Storage failure for keys {0} due to no QMs capable of accepting data.".format(list(request_message.store_key_to_data_dict.keys()))
                self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, notification))
            
            # If we can accept data, find a suitable QM and store.
            else:
                        
                # Default to a successful storage.
                response_code = client_response_codes.DATA_STORE_SUCCESS
                
                # Find a random QM which can accept data.
                index = int(random.random() * accepting_count)
                remote_qm_id_string = self._current_accepting_data_owner_id_list[index]
    
                # Go through all keys.
                for key, data in list(request_message.store_key_to_data_dict.items()):
                    
                    # If the location key points to our local QM, store locally.
                    if remote_qm_id_string == self.qm_id_string:                    
                        self.shared_memory_manager.store_value(key, data, expire_seconds)
                        
                    # If the location key points to a remote QM, store remotely.
                    else:

                        # Push.  If we failed, we must remove the queue lock information for this queue so we do not keep failing.
                        result = self._store_data_in_remote_shared_memory(remote_qm_id_string, key, data, expire_seconds)
                        if result == False:
                        
                            # Set the response code to failure.
                            response_code = client_response_codes.DATA_STORE_FAIL_REMOTE
        
                            # Log.
                            error_string = "*** Ordered remote store failed: {0} : {1} ***".format(remote_qm_id_string, key)
                            self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, error_string))  
                            
                            # Break - we are always assigning all data to the same remote location so we have no reason to try again.
                            break
                
                # Form a reply.
                reply_message = client_messages.ClientDataStoreReplyMessage(reply_id_tag, remote_qm_id_string, response_code)
                reply_message.send(self.request_socket)
                    
                # Notify the main thread.
                notification = "Stored data to key: {0}".format(remote_qm_id_string)
                self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, notification))
            
            # Denote the message has been handled.
            return True
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
            
        
    def _handle_client_data_pull_request(self, request_message, reply_id_tag):
        """
        Handles a client's request for data.
        Will attempt to gather the amount of requested data from each queue provided in the request.
        Will return an error code for any queue requested which does not exist.
        Updates the original request with results sent the client and forwards to the main thread.
        """
        
        try:
                        
            # We have to pull data from each queue in the pull request' queue name
            queue_name_list = request_message.get_queue_name_list()
                        
            # Loop through the queue names, tracking our data and data count per queue name.
            # Note that we convert our queue name list to a set to guarantee we don't pull on the same queue twice.
            queue_data_pulled_dict = dict()
            response_code_dict = dict()
            for queue_name in set(queue_name_list):
                
                # If the queue is on the frozen list, we must reject pulling data from it.
                if queue_name in self._current_frozen_pull_queue_set:
                    self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, "Rejected pull due to freeze: {0}".format(queue_name)))
                    response_code_dict[queue_name] = client_response_codes.PUSH_TO_QUEUE_ON_FROZEN_LIST
                    
                # If the queue name exists, attempt to pull data from it.
                # Note that we will synchronize if we fail to find the queue to ensure we have the latest data.
                elif self._get_data_queue_wrapper(queue_name, True) != None:
                #elif queue_name in self._current_data_queue_wrapper_dict.keys():
                    
                    # Determine where we should be making this pull.  
                    owner_id_list = self._current_ordered_queue_owners_dict.get(queue_name, None)
                    
                    # Pull: If there is an owner ID list, pull as ordered; if not, pull as direct.
                    if owner_id_list == None:
                        self._pull_direct_data(queue_name, request_message.request_count, queue_data_pulled_dict)
                    else:
                        self._pull_ordered_data(owner_id_list, queue_name, request_message.request_count, queue_data_pulled_dict)
                
                # If the queue name does not exist, denote.
                else:
                    response_code_dict[queue_name] = client_response_codes.QUEUE_DOES_NOT_EXIST
                    
            # Form a reply
            status_message = ""
            reply_message = client_messages.ClientDataPullReplyMessage(reply_id_tag, queue_data_pulled_dict, response_code_dict, status_message)
            reply_message.send(self.request_socket)
            
            # Denote the message has been handled.
            return True
             
        except:
            
            raise ExceptionFormatter.get_full_exception() 
                
          
    def _handle_client_data_push_request(self, request_message, reply_id_tag):
        """
        Handles a push request.
        """
        
        try:
                
            # Get the exchange; synchronize on failure.
            exchange = self._get_exchange_wrapper(request_message.exchange_name, True)
            #exchange = self._current_exchange_wrapper_dict.get(request_message.exchange_name)
            if exchange != None:
    
                # Get the list of queue names which satisfy the routing key requirements for the exchange.
                queue_name_list = exchange.get_routed_queue_names(request_message.routing_key)
                
                # Convert our queue name list to a set.  
                queue_name_set = set(queue_name_list)
                
                # If any of the queues in the set are on our frozen push list, we must reject the entire message.
                response_code = None
                if len(queue_name_set.intersection(self._current_frozen_push_queue_set)) > 0:
                    self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, "Rejected push due to freeze: {0}".format(queue_name_set)))
                    response_code = client_response_codes.PUSH_TO_QUEUE_ON_FROZEN_LIST
                
                # If there are no QMs accepting data in the system, we must reject any data sent to queues on our shared memory overflow rejection list.
                elif len(self._current_accepting_data_owner_id_list) == 0 and len(queue_name_set.intersection(self.config.manager_shared_memory_overflow_queue_rejection_set)) > 0:
                    self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, "Rejected push due to overflow: {0}".format(queue_name_set)))
                    response_code = client_response_codes.PUSH_TO_QUEUE_ON_OVERFLOW_LIST
                
                # Determine if any of the queues this routing key would push to are currently on our rejection list.
                # Set a failed push response code if we have a rejected queue in our list.
                elif len(queue_name_set.intersection(self._current_push_rejection_queue_name_set)) > 0:
                    
                    # When we have rejected queues, it's because at least one QM is full.
                    # If all QMs are full, we could potentially get into a deadlock situation, where data can't travel between queues because all queues are locked.
                    # The overflow clause above makes sure more data doesn't come into the system in the defined overflow queues.
                    # If the configuration file allows us to process the non-overflow queues, we shouldn't reject this data.
                    # Check our state.
                    if len(self._current_accepting_data_owner_id_list) > 0 or self.config.manager_shared_memory_overflow_allow_all_other_queues_on_rejection == False:                  
                        response_code = client_response_codes.PUSH_TO_QUEUE_ON_REJECTION_LIST
                        self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, "Rejected push due to rejection: {0}".format(queue_name_set)))
                    
                # Push to each queue if we are cleared to do so.
                if response_code == None: 
                    
                    # Assume success; any failures will overwrite this response code.
                    response_code = client_response_codes.DATA_PUSH_SUCCESS
                    
                    # Attempt to push to all queues supplied by the routing key.
                    for queue_name in queue_name_list:
                            
                        # Test for an ordered queue owner; if there is no entry, we have a distributed queue.
                        owner_id_list = self._current_ordered_queue_owners_dict.get(queue_name, None)
                        
                        # If we found an owner list, we have an ordered queue.
                        # Test if we aren't the owner; set a remote ID string if not.
                        remote_qm_id_string = None
                        if owner_id_list != None:
                            
                            # If there are no owners, we have no place to put this data; notify.
                            if len(owner_id_list) == 0:

                                # Set the response code to failure.
                                response_code = client_response_codes.PUSH_TO_QUEUE_FAILED

                                # Log.
                                error_string = "*** No ordered queue owners; push failed: {0} ***".format(queue_name)
                                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, error_string))  
                    
                                # Continue; we don't want to include failure information in our status tracker.
                                continue
                        
                            elif owner_id_list[-1] != self.qm_id_string:
                                remote_qm_id_string = owner_id_list[-1]
                                
                        # If we didn't have an owner ID list, this is a distributed queue.
                        # If we don't have room locally, we have to use a remote push location.
                        elif self.qm_id_string not in self._current_accepting_data_owner_id_list and len(self._current_accepting_data_owner_id_list) > 0:
                                                        
                            # Find a random QM.
                            random_qm_list = list(self._current_accepting_data_owner_id_list)
                            random.shuffle(random_qm_list) 
                            remote_qm_id_string = random_qm_list[-1]
                            
                        # If we found no remote QM, push locally.
                        if remote_qm_id_string == None:
                            self.shared_memory_manager.push_queue_data_list(queue_name, [request_message.data_tuple_dumped,])
                                
                        # If we found a remote QM, push remotely.
                        else:
                                                            
                            # Push.  If we failed, we must remove the queue lock information for this queue so we do not keep failing.
                            result = self._push_data_to_remote_shared_memory(remote_qm_id_string, queue_name, [request_message.data_tuple_dumped,])
                            if result == False:
                            
                                # Set the response code to failure.
                                response_code = client_response_codes.PUSH_TO_QUEUE_FAILED

                                # Log.
                                error_string = "*** Ordered remote push failed: {0} : {1} ***".format(remote_qm_id_string, queue_name)
                                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, error_string))  
                    
                                # Continue; we don't want to include failure information in our status tracker.
                                continue
                                
                        # Increment our pushed status tracker.
                        if queue_name not in list(self.queue_data_pushed_status_report.keys()):
                            self.queue_data_pushed_status_report[queue_name] = 0
                        self.queue_data_pushed_status_report[queue_name] += 1                        
                                                                                
            else:
                
                # If the exchange doesn't exist, return in a response.
                response_code = client_response_codes.EXCHANGE_DOES_NOT_EXIST
                
            # Create the reply message.
            status_message = ""
            reply_message = client_messages.ClientDataPushReplyMessage(reply_id_tag, response_code, status_message)
            reply_message.send(self.request_socket)
            
            # Denote the message has been handled.
            return True
             
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _handle_client_declare_exchanges_request(self, request_message, reply_id_tag):
        """
        Handles a client's exchange declaration request.  
        Will attempt to create and add all exchanges which don't currently exist in shared memory.
        Will create a reply with the response code for each exchange desired to be created.
        Note that synchronization (thread-lock) will occur for any additions required.
        """
        
        try:
            
            # Always synchronize exchanges during an exchange declaration.
            if len(request_message.client_exchange_list) > 0:
                self._current_exchange_wrapper_dict = self.shared_memory_manager.get_exchange_wrapper_dict()
            
            # We need to break the exchanges in the request into three categories: does not exist, exists and type match, exists and type mismatch.            
            exchange_name_type_match_set = set()
            exchange_name_type_mismatch_set = set()
            exchange_name_does_not_exist_set = set()
            for client_exchange in request_message.client_exchange_list:
                # exchange = self._get_exchange_wrapper(client_exchange.name, True)
                exchange = self._current_exchange_wrapper_dict.get(client_exchange.name, None)

                if exchange == None:
                    exchange_name_does_not_exist_set.add(client_exchange.name)
                elif exchange.type != client_exchange.type:
                    exchange_name_type_mismatch_set.add(client_exchange.name)
                else:
                    exchange_name_type_match_set.add(client_exchange.name)
                    
            # We have to further process exchanges which do not exist.
            exchange_name_created_set = set()
            exchange_name_failed_set = set()
            if len(exchange_name_does_not_exist_set) > 0:
                
                # Create a new list of client exchanges which match those which do not exist.
                declare_client_exchange_list = list()
                for client_exchange in request_message.client_exchange_list:
                    if client_exchange.name in exchange_name_does_not_exist_set:
                        declare_client_exchange_list.append(client_exchange)
                        
                # Update the list of exchanges in the original message and pass it along to the main thread.
                request_message.client_exchange_list = declare_client_exchange_list
                self.action_out_queue.put(request_message)
                
                # Wait until all the new exchanges have been created or our timeout has been reached.
                if self._validate_shared_memory(self._validate_exchange_names_exist, True, 
                                                defines.WORKER_VALIDATE_DECLARATION_TIMEOUT, exchange_name_does_not_exist_set) == True:
                    
                    # If we succeeded, all exchanges were created.
                    exchange_name_created_set = exchange_name_does_not_exist_set
                    
                else:
                    
                    # If we failed, we have to track which exchanges were created and which timed out.
                    exchange_name_created_set, exchange_name_failed_set = self.shared_memory_manager.which_exchange_wrapper_names_exist(exchange_name_does_not_exist_set)
        
            # Create the response dictionary.
            response_code_dict = dict()
            for exchange_name in exchange_name_created_set: 
                response_code_dict[exchange_name] = client_response_codes.EXCHANGE_DECLARE_SUCCESS_CREATED
            for exchange_name in exchange_name_type_match_set: 
                response_code_dict[exchange_name] = client_response_codes.EXCHANGE_DECLARE_SUCCESS_EXISTS
            for exchange_name in exchange_name_type_mismatch_set: 
                response_code_dict[exchange_name] = client_response_codes.EXCHANGE_DECLARE_FAILURE_TYPE_MISMATCH
            for exchange_name in exchange_name_failed_set: 
                response_code_dict[exchange_name] = client_response_codes.OPERATION_TIMED_OUT
            
            # Reply. 
            status_message = ""
            reply_message = client_messages.ClientDeclareExchangesReplyMessage(reply_id_tag, response_code_dict, status_message)
            reply_message.send(self.request_socket)
            
            # Notify.
            response_string = ""
            for exchange_name, response_code in list(response_code_dict.items()):
                response_string += "{0}:{1}; ".format(exchange_name, client_response_codes.convert_response_code_to_string(response_code))
            notification = "Client exchange declaration request received.  Exchange count: {0}.  Response: {1}.".format(len(request_message.client_exchange_list), response_string)
            self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, notification))
                
            # Denote the message has been handled.
            return True
             
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
            
    def _handle_client_declare_queues_request(self, request_message, reply_id_tag):
        """
        Handles a client's queue declaration request.  
        Will ensure all queues given in the request are both created and attached to their designated exchanges.
        Will create a reply with the response code for each queue processed.
        Note that synchronization (thread-lock) will occur for any additions/attachments required.
        """
        
        try:
            
            # Always synchronize exchanges and queues during an exchange declaration.
            if len(request_message.client_queue_list) > 0:
                self._current_exchange_wrapper_dict = self.shared_memory_manager.get_exchange_wrapper_dict()
                self._current_data_queue_wrapper_dict = self.shared_memory_manager.get_queue_wrapper_dict()
                        
            # Test for exchange failures first.
            declare_client_queue_list = list()
            queue_name_not_provided_set = set()
            exchange_does_not_exist_queue_name_set = set()
            direct_exchange_already_attached_queue_name_set = set()
            exchange_type_does_not_accept_routing_keys_queue_name_set = set()
            exchange_type_requires_routing_key_queue_name_set = set()
            exchange_has_queue_attached_under_different_routing_key_set = set()
            queue_already_exists_and_is_attached_queue_name_set = set()
            for client_queue in request_message.client_queue_list:
                # Ensure we were given a queue name.
                if client_queue.name == None:
                    queue_name_not_provided_set.add(None)
                    continue
                
                # Get the current exchange.
                exchange = self._get_exchange_wrapper(client_queue.exchange_name, True)
                #exchange = self._current_exchange_wrapper_dict.get(client_queue.exchange_name, None)
                
                # If an exchange doesn't exist, reject.
                if exchange == None:
                    exchange_does_not_exist_queue_name_set.add(client_queue.name)
                    
                # If the exchange is direct and a routing key is specified, reject.
                elif exchange.get_type() == client_exchange_types.DIRECT and client_queue.routing_key != "":
                    exchange_type_does_not_accept_routing_keys_queue_name_set.add(client_queue.name)
                    
                # If the exchange is fan out and a routing key is specified, reject.
                elif exchange.get_type() == client_exchange_types.FANOUT and client_queue.routing_key != "":
                    exchange_type_does_not_accept_routing_keys_queue_name_set.add(client_queue.name)
                    
                # If the exchange is topic and a routing key is not specified, reject.
                elif exchange.get_type() == client_exchange_types.TOPIC and client_queue.routing_key == "":
                    exchange_type_requires_routing_key_queue_name_set.add(client_queue.name)
                
                # If the exchange is direct and already has a queue bound to it and it is not this queue, reject.
                elif exchange.get_type() == client_exchange_types.DIRECT and exchange.get_queue_count() > 0 and exchange.is_queue_attached(client_queue.name) == False:
                    direct_exchange_already_attached_queue_name_set.add(client_queue.name)
                    
                # If the exchange has the queue attached but under a different routing key, reject.
                #elif exchange.is_queue_attached(client_queue.name) == True and exchange.is_queue_attached_to_routing_key(client_queue.routing_key, client_queue.name) == False:
                #    exchange_has_queue_attached_under_different_routing_key_set.add(client_queue.name)
                    
                # If we have made it to this point, the exchange is valid.
                # If the queue is already attached to the exchange and the queue exists in memory, accept.
                elif exchange.is_queue_attached_to_routing_key(client_queue.routing_key, client_queue.name) == True and client_queue.name in list(self._current_data_queue_wrapper_dict.keys()):
                    queue_already_exists_and_is_attached_queue_name_set.add(client_queue.name)
                    
                # If the last test failed, it means either the queue isn't attached to the exchange or it doesn't exist.  
                # We must declare either way.
                else:
                    declare_client_queue_list.append(client_queue)
            
            # We will need to forward the request if we have any new exchanges.
            declaration_successfull_queue_name_set = set()            
            declaration_timed_out_queue_name_set = set()
            if len(declare_client_queue_list) > 0:
                
                # Update the list of queues in the original message and pass it along to the main thread.
                request_message.client_queue_list = declare_client_queue_list
                self.action_out_queue.put(request_message)
                
                # Wait until all the new exchanges have been created or our timeout has been reached.
                if self._validate_shared_memory(self._validate_client_queue_list, True, 
                                                defines.WORKER_VALIDATE_DECLARATION_TIMEOUT, declare_client_queue_list) == True:
                    
                    # If we succeeded, all exchanges were created.
                    declaration_successfull_queue_name_set = set([client_queue.name for client_queue in declare_client_queue_list])
                    
                else:
                            
                    # If we failed, we have to track which exchanges were created and which timed out.
                    client_queue_validated_list, client_queue_invalidated_list = self.shared_memory_manager.which_client_queues_exist_and_are_attached(declare_client_queue_list)
                    declaration_successfull_queue_name_set = set([client_queue.name for client_queue in client_queue_validated_list])
                    declaration_timed_out_queue_name_set = set([client_queue.name for client_queue in client_queue_invalidated_list])
                    
            # Create the response dictionary.
            response_code_dict = dict()
            for queue_name in exchange_does_not_exist_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.EXCHANGE_DOES_NOT_EXIST
            for queue_name in direct_exchange_already_attached_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.DIRECT_EXCHANGE_ALREADY_HAS_A_QUEUE_ATTACHED
            for queue_name in exchange_type_does_not_accept_routing_keys_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.EXCHANGE_TYPE_DOES_NOT_ACCEPT_ROUTING_KEYS
            for queue_name in exchange_type_requires_routing_key_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.EXCHANGE_TYPE_REQUIRES_ROUTING_KEY
            for queue_name in exchange_has_queue_attached_under_different_routing_key_set: 
                response_code_dict[queue_name] = client_response_codes.EXCHANGE_HAS_QUEUE_ATTACHED_UNDER_DIFFERENT_ROUTING_KEY
            for queue_name in queue_already_exists_and_is_attached_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.QUEUE_ALREADY_EXISTS_AND_IS_ATTACHED
            for queue_name in declaration_successfull_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.QUEUE_CREATED_AND_ATTACHED
            for queue_name in declaration_timed_out_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.OPERATION_TIMED_OUT
            for queue_name in queue_name_not_provided_set: 
                response_code_dict[queue_name] = client_response_codes.MALFORMED_MESSAGE
                            
            # Reply. 
            status_message = ""
            reply_message = client_messages.ClientDeclareQueuesReplyMessage(reply_id_tag, response_code_dict, status_message)
            reply_message.send(self.request_socket)
            
            # Notify.
            response_string = ""
            for queue_name, response_code in list(response_code_dict.items()):
                response_string += "{0}:{1}; ".format(queue_name, client_response_codes.convert_response_code_to_string(response_code))
            notification = "Client queue declaration request received.  Queue count: {0}.  Response: {1}.".format(len(request_message.client_queue_list), response_string)
            self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, notification))
        
            # Denote the message has been handled.
            return True
             
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _handle_client_delete_queues_request(self, request_message, reply_id_tag):
        """
        Handles a client delete queues request.
        Goes through each queue name in the given request message and attempts to remove all traces of the queue from shared memory.
        Note this method does direct reads from shared memory for its processing rather than using local snapshots.
        """
        
        try:
            
            # Do not allow any locked queues to be processed.
            current_locked_queue_set = set(self.shared_memory_manager.get_locked_queue_names())
            locked_queue_name_set = set()
            for queue_name in current_locked_queue_set:
                if queue_name in request_message.queue_name_list:
                    request_message.queue_name_list.remove(queue_name)
                    locked_queue_name_set.add(queue_name)

            # Get all queue names in the given request which currently exist somewhere in shared memory.
            # Note we use shared memory in the queue deletion memory instead of local thread memory.
            current_queue_names_set, already_does_not_exist_queue_name_set = self.shared_memory_manager.which_queue_names_exist_or_are_attached(set(request_message.queue_name_list))
    
            # If we have elements, forward to the main thread.
            deleted_queue_name_set = set()
            deletion_timed_out_queue_name_set = set()
            if len(current_queue_names_set) > 0:
                
                # Forward to the main thread and validate against shared memory.
                request_message.client_queue_list = list(current_queue_names_set)
                self.action_out_queue.put(request_message)
                
                # Sync against memory until we pass or we timeout.
                if self._validate_shared_memory(self._validate_queues_deleted, True, defines.WORKER_VALIDATE_QUEUE_DELETION_TIMEOUT, 
                                                current_queue_names_set) == True:
                    
                    # If we pass, all items were handled.
                    deleted_queue_name_set = current_queue_names_set
                    
                else:
                      
                    # If we timed out, determine which items were handled.
                    deletion_timed_out_queue_name_set, deleted_queue_name_set = self.shared_memory_manager.which_queue_names_exist_or_are_attached(current_queue_names_set)
                    
            # Create the response dictionary.
            response_code_dict = dict()
            for queue_name in already_does_not_exist_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.QUEUE_ALREADY_DOES_NOT_EXIST_AND_IS_NOT_ATTACHED 
            for queue_name in deleted_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.QUEUE_NO_LONGER_EXISTS_AND_IS_NOT_ATTACHED
            for queue_name in locked_queue_name_set:
                response_code_dict[queue_name] = client_response_codes.LOCKED_QUEUE_CAN_NOT_BE_DELETED
            for queue_name in deletion_timed_out_queue_name_set:
                response_code_dict[queue_name] = client_response_codes.OPERATION_TIMED_OUT

            # Reply. 
            status_message = ""
            reply_message = client_messages.ClientDeleteQueuesReplyMessage(reply_id_tag, response_code_dict, status_message)
            reply_message.send(self.request_socket)
            
            # Notify.
            response_string = ""
            for queue_name, response_code in list(response_code_dict.items()):
                response_string += "{0}:{1}; ".format(queue_name, client_response_codes.convert_response_code_to_string(response_code))
            notification = "Client queue deletion request received.  Response: {0}.".format(response_string)
            self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, notification))
            
            # Denote the message has been handled.
            return True
             
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _handle_client_lock_queues_request(self, lock_request, reply_id_tag):
        """
        Handles a client lock queues request.
        Will go through each client queue lock object given in the request object and attempt to attain a lock.
        Will validate queue exists.
        Will validate a lock doesn't exist or we are forcing an unlock.
        Will forward client queue locks which are validated to the main thread for processing.
        Will create a reply with the response code for each queue lock processed.
        Note that synchronization (thread-lock) will occur for any locks forwarded.
        """
        
        # SMK: VALIDATE
        try:
            
            # Get the set of queue names from the given queue lock list.
            queue_name_set = set([queue_lock.name for queue_lock in lock_request.client_queue_lock_list])
            
            # Always synchronize queues during a lock.
            if len(queue_name_set) > 0:
                self._current_data_queue_wrapper_dict = self.shared_memory_manager.get_queue_wrapper_dict()
            
            # Filter out failures due to the queue not existing.
            does_not_exist_queue_name_set = queue_name_set - set(self._current_data_queue_wrapper_dict.keys())
            queue_name_set = queue_name_set - does_not_exist_queue_name_set
            
            # Filter out queues which are not ordered - do not allow non-ordered queues to be locked.
            ordered_queue_set = set()
            for data_queue_wrapper in list(self._current_data_queue_wrapper_dict.values()):
                if data_queue_wrapper.is_ordered() == True:
                    ordered_queue_set.add(data_queue_wrapper.name)
            not_ordered_queue_set = queue_name_set - ordered_queue_set
            queue_name_set = queue_name_set - not_ordered_queue_set 
                        
            # Get the current queue lock dictionary.  
            current_queue_lock_string_dict = self.shared_memory_manager.get_queue_lock_owner_dict()
                    
            # Go through each queue lock which has a valid queue name.
            lock_rejected_queue_name_set = set()
            locked_queue_name_set = set()
            forward_request_queue_lock_list = list()
            for queue_lock in lock_request.client_queue_lock_list:
                
                # If the queue name exists.
                if queue_lock.name in queue_name_set:
                    
                    # Get the current owner data from the local copy of queue locks, defaulting to no lock.
                    owner_id_string = current_queue_lock_string_dict.get(queue_lock.name, "")

                    # If the current lock owner on the queue is this QM, we succeed.
                    #if owner_id_string == self.qm_id_string and owner_process_id_string == queue_lock.process_id_string:
                    #    locked_queue_name_set.add(queue_lock.name)
                    
                    # If the current lock owner is set to another QM and we aren't forcing an unlock, we fail.
                    #elif owner_id_string != "" and queue_lock.force_unlock_flag == False:
                    if owner_id_string != "" and queue_lock.force_unlock_flag == False:
                        lock_rejected_queue_name_set.add(queue_lock.name)
                        
                    # If there is no current owner or if we are forcing an unlock, we must request from the main thread.
                    else:
                        forward_request_queue_lock_list.append(queue_lock)
                            
            # Go through each queue lock we found we need to request.
            response_timed_out_queue_name_set = set()
            if len(forward_request_queue_lock_list) > 0:
                
                # The main thread has to talk to the master QM to request locks.
                # Push this message to the main thread and sync against the desired result.
                # Update the list of queues in the original message and pass it along to the main thread.
                lock_request.client_queue_lock_list = forward_request_queue_lock_list
                self.action_out_queue.put(lock_request)
                
                # Form the list of queue lock data we will need to verify against in shared memory.
                validation_dictionary = dict()
                for queue_lock in forward_request_queue_lock_list:
                    validation_dictionary[queue_lock.name] = self.qm_id_string

                # Wait for the queue to appear in memory.
                if self._validate_shared_memory(self._validate_queue_locks_owned, True, defines.QM_LOCK_QUEUES_REQUEST_TIME_OUT, 
                                      validation_dictionary) == False:
                    
                    # Add all queue names to the locked queue name set if memory was synchronized. 
                    locked_queue_name_set = locked_queue_name_set.union(set(validation_dictionary.keys()))
                    
                else:
                    
                    # If memory wasn't synchronized, determine which lock status and place in the correct response set.
                    granted_set, rejected_set = self.shared_memory_manager.get_queue_locks_owned_sets(validation_dictionary)
                    for queue_name in granted_set:
                        locked_queue_name_set.add(queue_name)
                    for queue_name in rejected_set:
                        response_timed_out_queue_name_set.add(queue_name)
                        
            # Create the response dictionary.
            response_code_dict = dict()
            for queue_name in locked_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.QUEUE_LOCK_GRANTED
            for queue_name in lock_rejected_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.QUEUE_LOCK_REJECTED
            for queue_name in does_not_exist_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.QUEUE_DOES_NOT_EXIST
            for queue_name in not_ordered_queue_set: 
                response_code_dict[queue_name] = client_response_codes.QUEUE_MUST_BE_ORDERED_TO_LOCK
            for queue_name in response_timed_out_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.OPERATION_TIMED_OUT
            
            # Reply. 
            status_message = ""
            reply_message = client_messages.ClientLockQueuesReplyMessage(reply_id_tag, response_code_dict, status_message)
            reply_message.send(self.request_socket)
            
            # Notify.
            response_string = ""
            for queue_name, response_code in list(response_code_dict.items()):
                response_string += "{0}:{1}; ".format(queue_name, client_response_codes.convert_response_code_to_string(response_code))
            notification = "Client queue lock request received.  Response: {0}.".format(response_string)
            self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, notification))
        
            # Denote the message has been handled.
            return True
             
        except:
            
            raise ExceptionFormatter.get_full_exception()
        

    def _handle_client_requeue_data_request(self, requeue_request, reply_id_tag):
        """
        """
        
        # SMK: VALIDATE
        try:
            
            # Put data into the queue if we found it.
            self.shared_memory_manager.push_queue_data_list(requeue_request.queue_name, [requeue_request.data_tuple_dumped,])
            
            # Update our status tracker on data that has been put back into a queue by a client.
            if requeue_request.queue_name not in list(self.queue_data_requeued_status_report.keys()):
                self.queue_data_requeued_status_report[requeue_request.queue_name] = 0
            self.queue_data_requeued_status_report[requeue_request.queue_name] += 1
    
            # Set the response - our only responsibility is to tell the client if we pushed to the exchange or not.
            response_code = client_response_codes.DATA_REQUEUE_SUCCESS
            
            # Create the reply message.
            status_message = ""
            reply_message = client_messages.ClientRequeueDataReplyMessage(reply_id_tag, response_code, status_message)
            reply_message.send(self.request_socket)
            
            # Denote the message has been handled.
            return True
             
        except:
            
            raise ExceptionFormatter.get_full_exception()
        

    def _handle_client_unlock_queues_request(self, release_request, reply_id_tag):
        """
        Handles a client unlock queues request.
        Will attempt to release all queue names in the given request object.
        Will find all locks which are actually owned in the given request data and forward an unlock request to the main thread.
        Will create a reply with the response code for each queue name processed.
        Note that synchronization (thread-lock) will occur for any unlocks forwarded.
        """
        
        # SMK: VALIDATE
        try:
            
            # Filter out all locks which aren't actually owned by the client/process making this request.
            forward_request_queue_name_list = list()
            lock_not_owned_queue_name_set = set()
            current_queue_lock_owner_dict = self.shared_memory_manager.get_queue_lock_owner_dict()
            for queue_name in release_request.queue_name_list:
                if current_queue_lock_owner_dict.get(queue_name) == self.qm_id_string:
                    forward_request_queue_name_list.append(queue_name)
                else:
                    lock_not_owned_queue_name_set.add(queue_name)

            # Go through each queue lock we found we need to request.
            unlocked_queue_name_set = set()
            response_timed_out_queue_name_set = set()
            if len(forward_request_queue_name_list) > 0:
                
                # The main thread has to talk to the master QM to request releases.
                # Push this message to the main thread and sync against the desired result.
                # Update the list of queues in the original message and pass it along to the main thread.
                release_request.queue_name_list = forward_request_queue_name_list
                self.action_out_queue.put(release_request)      
                                
                # Wait for the unlocks to go through.
                if self._validate_shared_memory(self._validate_queues_not_locked, True, defines.QM_UNLOCK_QUEUES_REQUEST_TIME_OUT, 
                                      forward_request_queue_name_list) == False:
                    
                    # Add all queue names to the unlocked queue name set if memory was synchronized. 
                    unlocked_queue_name_set = set(forward_request_queue_name_list)
                    
                else:
                    
                    # If memory wasn't synchronized, determine which lock status and place in the correct response set.
                    queue_lock_owner_dict = self.shared_memory_manager.get_queue_lock_owner_dict()
                    for queue_name in forward_request_queue_name_list:
                        if queue_lock_owner_dict.get(queue_name, "") != "":
                            response_timed_out_queue_name_set.add(queue_name)
                        else:
                            unlocked_queue_name_set.add(queue_name)                        
                                        
            # Create the response dictionary.
            response_code_dict = dict()
            for queue_name in unlocked_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.QUEUE_UNLOCKED
            for queue_name in lock_not_owned_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.QUEUE_UNLOCK_REJECTED_NOT_OWNED
            for queue_name in response_timed_out_queue_name_set: 
                response_code_dict[queue_name] = client_response_codes.OPERATION_TIMED_OUT
            
            # Reply. 
            status_message = ""
            reply_message = client_messages.ClientUnlockQueuesReplyMessage(reply_id_tag, response_code_dict, status_message)
            reply_message.send(self.request_socket)
            
            # Notify.
            response_string = ""
            for queue_name, response_code in list(response_code_dict.items()):
                response_string += "{0}:{1}; ".format(queue_name, client_response_codes.convert_response_code_to_string(response_code))
            notification = "Client queue unlock request received.  Response: {0}.".format(response_string)
            self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, notification))
             
            # Denote the message has been handled.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
        
    def _handle_peer_online_handshake_request(self, raw_message, reply_id_tag):
        
        # SMK: VALIDATE
        try:

            # An online notification must be handled in the main thread because a PQM must exist and be altered based on the new state information.
            # We have to handle replying in this thread, however, because ZeroMQ sockets must be handled in the same thread they were connected in.
            # Both the main thread and the reply require us to have a QM ID Tag, create it now.
            qm_dealer_id_tag = ZmqUtilities.get_dealer_id_tag(self.qm_id_string, int(time.time()))
            
            # Translate the raw message; tag our thread name in the message.
            message = peer_messages.PeerOnlineHandshakeRequestMessage.create_from_received(raw_message, qm_dealer_id_tag)
            message.sending_thread_name = self._thread_name
            
            # Attempt to ping the host who is attempting to connect to us.            
            host_name = message.settings_dict["id"].split(":")[0]
            if self._ping_host(host_name) == False:
            
                # Notify.
                notification = "Could not ping host attempting to handshake.  Message: {0}".format(message)
                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, notification))
            
                # Create the reply.
                reply_message = peer_messages.PeerOnlineHandshakeReplyMessage(reply_id_tag, dict(),
                                                                              qm_dealer_id_tag, 
                                                                              False, 
                                                                              None, None,
                                                                              True,
                                                                              False)
                reply_message.send(self.request_socket)
                
                # Denote the message has been handled.
                return True
            
            # Queue to our main thread.
            self.action_out_queue.put(message)
            
            # The master QM has to send initialization data to a PQM on handshake.  
            qm_is_master_flag = self.shared_memory_manager.is_master(self.qm_id_string)
            master_synchronization_failure_flag = False
            if qm_is_master_flag == True:
                
                # We need to get the initialization data from our main thread, which will know to do this because we queued this handshake request in our action out queue.
                # The main thread will put the data in shared memory; we have to wait until it appears for our use.
                time_out_tracker = 0
                while True:
                    
                    # Check shared memory for the data.
                    data = self.shared_memory_manager.get_connection_reply_data(self._thread_name, message.settings_dict["id"])
                    if data != None:
                        
                        # Remove the data from shared memory immediately.
                        self.shared_memory_manager.remove_connection_reply_data(self._thread_name, message.settings_dict["id"])
                        
                        # Pull the data apart and break from our loop.
                        data_dict = bson.loads(data)
                        master_control_data_message = peer_messages.PeerMasterControlDataMessage.load(data_dict["cdm"])
                        master_setup_data_message = peer_messages.PeerMasterSetupDataMessage.load(data_dict["sdm"])
                        break
                                            
                    # If we have exceeded our maximum delay, exit out.
                    if time_out_tracker > self.config.manager_synchronize_master_data_on_handshake_timeout:
                        master_setup_data_message = None
                        master_control_data_message = None
                        self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Failed to sync with main thread on reply data for new peer: {0}".format(message.settings_dict["id"])))
                        master_synchronization_failure_flag = True
                        break
                    
                    # If we didn't find the data, delay and try again.
                    else:
                        time.sleep(0.25)
                        time_out_tracker += 0.25
                                
            else:
                
                master_setup_data_message = None
                master_control_data_message = None
            
            # Determine the redis connection string we'll send the peer; we have to replace "localhost" with our remote IP address.
            if "localhost" in self.redis_connection_string:
                redis_connection_string = self.redis_connection_string.replace("localhost", self.remote_ip_address)
            else:
                redis_connection_string = self.redis_connection_string
                
            # Build the settings dictionary via the Queue Manager static method.
            settings_dict = peer_messages.build_settings_dictionary(self.qm_id_string, self.qm_start_up_time, redis_connection_string, 
                                                                    self.config.manager_shared_memory_max_size, 
                                                                    self.config.manager_shared_memory_ordered_data_reject_threshold,
                                                                    self.config.manager_shared_memory_ordered_data_accept_threshold)
                
            # Create the reply.
            reply_message = peer_messages.PeerOnlineHandshakeReplyMessage(reply_id_tag, settings_dict,
                                                                          qm_dealer_id_tag, 
                                                                          qm_is_master_flag, 
                                                                          master_setup_data_message, master_control_data_message,
                                                                          master_synchronization_failure_flag,
                                                                          True)
            reply_message.send(self.request_socket)
            
            # Notify.
            self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, "Peer online notification received and responded to from {0}".format(message.settings_dict["id"])))
        
            # Denote the message has been handled.
            return True
             
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _ping_host(self, host_name):
        """
        Returns true if we are able to ping the given host name.
        """
                
        return os.system("ping -c 1 " + host_name) == 0
        

    def _push_data_to_remote_shared_memory(self, pqm_id_string, queue_name, data_list, push_locally_on_failure = True):
        """
        Will attempt to push the given data list to the remote shared memory of the given PQM ID String.
        Failure condition: if either the PQM ID's connection information is unknown or the connection fails.
        On failure condition, will push the data locally if the boolean is set.
        On failure condition, returns False.  On success, returns True. 
        """
        
        try:
            
            # Push.
            _ = self.shared_memory_manager.query_remote_shared_memory(pqm_id_string, self.shared_memory_manager.push_queue_data_list, 
                                                                           queue_name, data_list)
        
            # Return success.
            return True                              
            
        except RedisConnectionUnknownException: 
                            
            # Log.
            error_string = "PQM's shared memory connection information not found during remote push.  PQM ID: {0}.  Queue: {1}.".format(pqm_id_string, queue_name)
            self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, error_string)) 
            
            # Push and log if we are supposed to.
            if push_locally_on_failure == True:
                self.shared_memory_manager.push_queue_data_list(queue_name, data_list)    
                self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, "Pushed data locally as desired in response to remote push failure."))   
            
        except RedisConnectionFailureException:
            
            # Log.
            error_string = "PQM's shared memory connection could not be established during remote push.  PQM ID: {0}.  Queue: {1}.".format(pqm_id_string, queue_name)
            self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, error_string))
            
            # Push and log if we are supposed to.
            if push_locally_on_failure == True:
                self.shared_memory_manager.push_queue_data_list(queue_name, data_list)
                self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, "Pushed data locally as desired in response to remote push failure."))   
    
        # Default to failure.
        return False
    
    
    def _push_local_queue_data(self, message):
        """
        Responds to a system message sent from the main thread.
        The main thread can request we push local queue data to another QM's shared memory.
        We are given a map of queues to owner ID strings to push.
        """
        
        for queue_name, owner_id_string in list(message.queue_push_dict.items()):
            
            # Consume messages from our queue and push it remotely.
            while True:
                data_list, remaining_data_count = self.shared_memory_manager.pop_queue_data(queue_name, 100)
                data_list_count = len(data_list)
                if data_list_count > 0:
                    notification_string = "Popped {0} items from {1}; pushing remotely to {2}.".format(data_list_count, queue_name, owner_id_string)
                    self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, notification_string))  
                    result = self._push_data_to_remote_shared_memory(owner_id_string, queue_name, data_list)
                    if result == False:
                        notification_string = "Failed to push items remotely; breaking from further pushing."
                        self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, notification_string))  
                        break
                if remaining_data_count <= 0:
                    notification_string = "No items left to push from {0}; pushing remotely to {1}.".format(queue_name, owner_id_string)
                    self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, notification_string))  
                    break
    
    
    def _send_status_report(self):        
        
        # Capture each tracked set of data for the status report, resetting if data is going to be sent.
        queue_data_pushed_status_report = None
        if len(list(self.queue_data_pushed_status_report.keys())) > 0:
            queue_data_pushed_status_report = self.queue_data_pushed_status_report
            self.queue_data_pushed_status_report = dict()
            
        queue_data_popped_status_report = None
        if len(list(self.queue_data_popped_status_report.keys())) > 0:
            queue_data_popped_status_report = self.queue_data_popped_status_report
            self.queue_data_popped_status_report = dict()
            
        queue_data_requeued_status_report = None
        if len(list(self.queue_data_requeued_status_report.keys())) > 0:
            queue_data_requeued_status_report = self.queue_data_requeued_status_report
            self.queue_data_requeued_status_report = dict()
            
        synchronization_status_report = None
        if len(self.synchronization_status_report) > 0:
            synchronization_status_report = self.synchronization_status_report
            self.synchronization_status_report = list()
        
        updated_remote_shared_memory_connections_status_report = None
        if len(self.updated_remote_shared_memory_connections_status_report) > 0:
            updated_remote_shared_memory_connections_status_report = self.updated_remote_shared_memory_connections_status_report
            self.updated_remote_shared_memory_connections_status_report = dict()
        
        # Create and send a message if we have data to send.
        if queue_data_pushed_status_report != None or queue_data_popped_status_report != None or synchronization_status_report != None or updated_remote_shared_memory_connections_status_report != None:
            self.action_out_queue.put(system_messages.SystemDataWorkerStatusReportMessage(self._thread_name, 
                                                                                          queue_data_pushed_status_report,
                                                                                          queue_data_popped_status_report,
                                                                                          queue_data_requeued_status_report,
                                                                                          synchronization_status_report,
                                                                                          updated_remote_shared_memory_connections_status_report))
                
    
    def _set_pqm_queue_access_data(self, message):
        """
        Updates the PQM queue access dictionary.
        Returns true on success.
        """
        
        try:
            
            # Overwrite the old access dictionary.
            self._pqm_queue_access_dictionary = message.pqm_queue_access_dictionary
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        

    def _retrieve_data_in_remote_shared_memory(self, pqm_id_string, key):
        """
        Will attempt to retrieve the given data list in the remote shared memory of the given PQM ID String.
        Failure condition: if either the PQM ID's connection information is unknown or the connection fails.
        On failure condition, returns False.  On success, returns True. 
        """
                        
        try:
            
            # Push.
            data = self.shared_memory_manager.query_remote_shared_memory(pqm_id_string, self.shared_memory_manager.get_value, key)
        
            # Return success.
            return True, data                              
            
        except RedisConnectionUnknownException: 
                            
            # Log.
            error_string = "PQM's shared memory connection information not found during remote retrieval.  PQM ID: {0}.  Key: {1}.".format(pqm_id_string, key)
            self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, error_string))  
            
        except RedisConnectionFailureException:
            
            # Log.
            error_string = "PQM's shared memory connection could not be established during remote retrieval.  PQM ID: {0}.  Key: {1}.".format(pqm_id_string, key)
            self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, error_string)) 
    
        # Default to failure.
        return False, None
    

    def _delete_data_in_remote_shared_memory(self, pqm_id_string, key):
        """
        Will attempt to delete the given data list from the remote shared memory of the given PQM ID String.
        Failure condition: if either the PQM ID's connection information is unknown or the connection fails.
        On failure condition, returns False.  On success, returns True. 
        """
                        
        try:
            
            # Push.
            _ = self.shared_memory_manager.query_remote_shared_memory(pqm_id_string, self.shared_memory_manager.delete_value, key)
        
            # Return success.
            return True                              
            
        except RedisConnectionUnknownException: 
                            
            # Log.
            error_string = "PQM's shared memory connection information not found during remote delete.  PQM ID: {0}.  Key: {1}.".format(pqm_id_string, key)
            self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, error_string))   
            
        except RedisConnectionFailureException:
            
            # Log.
            error_string = "PQM's shared memory connection could not be established during remote delete.  PQM ID: {0}.  Key: {1}.".format(pqm_id_string, key)
            self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, error_string))
    
        # Default to failure.
        return False
    

    def _store_data_in_remote_shared_memory(self, pqm_id_string, key, data, expire_time, store_locally_on_failure = True):
        """
        Will attempt to store the given data list to the remote shared memory of the given PQM ID String.
        Failure condition: if either the PQM ID's connection information is unknown or the connection fails.
        On failure condition, will store the data locally if the boolean is set.
        On failure condition, returns False.  On success, returns True. 
        """
                        
        try:
            
            # Push.
            _ = self.shared_memory_manager.query_remote_shared_memory(pqm_id_string, self.shared_memory_manager.store_value, key, data, expire_time)
        
            # Return success.
            return True                              
            
        except RedisConnectionUnknownException: 
                            
            # Log.
            error_string = "PQM's shared memory connection information not found during remote store.  PQM ID: {0}.  Key: {1}.".format(pqm_id_string, key)
            self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, error_string)) 
            
            # Push and log if we are supposed to.
            if store_locally_on_failure == True:
                self.shared_memory_manager.store_value(key, data, expire_time)    
                self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, "Stored data locally as desired in response to remote store failure."))   
            
        except RedisConnectionFailureException:
            
            # Log.
            error_string = "PQM's shared memory connection could not be established during remote store.  PQM ID: {0}.  Key: {1}.".format(pqm_id_string, key)
            self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, error_string))
            
            # Push and log if we are supposed to.
            if store_locally_on_failure == True:
                self.shared_memory_manager.store_value(key, data, expire_time)    
                self.action_out_queue.put(system_messages.SystemNotificationMessage(self._thread_name, "Stored data locally as desired in response to remote store failure."))   
    
        # Default to failure.
        return False
    

    def _update_control_data(self, message):
        """
        Updates the current control data trackers in the data worker thread.
        Returns true on success.
        """
        
        try:
            
            # Synchronize.
            self._current_pecking_order_list = message.pecking_order_list
            self._current_queue_lock_owner_dict = message.queue_lock_owner_dict
            self._current_ordered_queue_owners_dict = message.ordered_queue_owners_dict
            self._current_push_rejection_queue_name_set = message.push_rejection_queue_name_set
            self._current_routing_key_rejection_list = message.routing_key_rejection_list
            self._current_accepting_data_owner_id_list = message.accepting_data_owner_id_list
            self._current_frozen_push_queue_set = set(message.frozen_push_queue_list)
            self._current_frozen_pull_queue_set = set(message.frozen_pull_queue_list)
            #print self._thread_name, "push", str(self._current_frozen_push_queue_list), "pull", str(self._current_frozen_pull_queue_list)
            #print self._thread_name, str(self._current_ordered_queue_owners_dict), "r", str(self._current_push_rejection_queue_name_set), "a", str(self._current_accepting_data_owner_id_list)
          
            # Add the time of this synchronization to our status report.
            self.synchronization_status_report.append(("control", int(time.time())))
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
    
    def _update_setup_data(self, message):
        """
        Updates the current setup data trackers in the data worker thread.
        Returns true on success.
        """
        
        try:
        
            # Synchronize.
            self._current_exchange_wrapper_dict = message.exchange_wrapper_dict
            self._current_data_queue_wrapper_dict = message.data_queue_wrapper_dict
            
            # Add the time of this synchronization to our status report.
            self.synchronization_status_report.append(("setup", int(time.time())))
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
    
    def _update_shared_memory_connections(self, message):
        """
        Updates shared memory connections in local memory.
        Returns true on success.
        """
        
        try:
            
            # Get the current list of shared memory connections.
            current_pqm_id_string_list = self.shared_memory_manager.get_connected_pqm_id_string_list()
            
            # Remove PQM connections which are no longer present.
            for pqm_id_string in current_pqm_id_string_list:
                if pqm_id_string not in list(message.shared_memory_connection_string_dict.keys()):
                    self.shared_memory_manager.remove_pqm_connection(pqm_id_string)
                    self.updated_remote_shared_memory_connections_status_report.setdefault("removed", list()).append(pqm_id_string)
                    
            # Find all new connections and create.
            for pqm_id_string, pqm_connection_string in list(message.shared_memory_connection_string_dict.items()):
                if pqm_id_string not in current_pqm_id_string_list:
                    self.shared_memory_manager.add_pqm_connection(pqm_id_string, pqm_connection_string)
                    self.updated_remote_shared_memory_connections_status_report.setdefault("added", list()).append(pqm_id_string)
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
                        
    def _validate_client_queue_list(self, client_queue_list):
        """
        Returns true if all the client queues given exist and are attached to existing exchanges with the correct routing keys.
        """
        
        try:
            
            # Get the current memory state.
            current_exchange_dict = self.shared_memory_manager.get_exchange_wrapper_dict()
            current_queue_name_list = self.shared_memory_manager.get_queue_name_list()
            
            # Test each client queue given.
            for client_queue in client_queue_list:
                exchange = current_exchange_dict.get(client_queue.exchange_name, None)
                
                # If the exchange doesn't exist for any client queue in the given list, we have invalidated the test.
                if exchange == None:
                    return False
                
                # If the exchange doesn't have the queue name attached to the routing key for any client queue in the given list, we have invalidated the test.
                elif exchange.is_queue_attached_to_routing_key(client_queue.routing_key, client_queue.name) == False:
                    return False
                
                # If the queue name doesn't exist for any client queue in the given list, we have invalidated the test.
                elif client_queue.name not in current_queue_name_list:
                    return False
            
            # We have validation if none of the given client queues have failed.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        

    def _validate_exchange_names_exist(self, exchange_name_set):
        """
        Returns true if all exchange names in the given set exist in shared memory.
        """
        
        try:
            return set(self.shared_memory_manager.get_exchange_wrapper_name_list()).issuperset(exchange_name_set)
        except:
            raise ExceptionFormatter.get_full_exception()
                
        
    def _validate_queue_locks_owned(self, validation_dictionary):
        """
        Expects the validation dictionary to be keyed off of queue names, valued with a QM ID string.
        Tests each entry in the given dictionary against the queue lock data in the given shared memory object.
        Returns True if all locks are owned; False if not.
        """
        
        try:
            queue_lock_owner_dict = self.shared_memory_manager.get_queue_lock_owner_dict()
            for queue_name, queue_owner in list(validation_dictionary.items()):
                if queue_lock_owner_dict.get(queue_name, "") != queue_owner:
                    return False
            return True
        except:
            raise ExceptionFormatter.get_full_exception()
        
        
    def _validate_queues_not_locked(self, queue_name_list):
        """
        Returns true if all queue names in the given list are not currently locked according to shared memory.
        """
        
        try:
            get_queue_lock_owner_dict = self.shared_memory_manager.get_queue_lock_owner_dict()
            for queue_name in queue_name_list:
                if get_queue_lock_owner_dict.get(queue_name, "") != "":
                    return False
            return True
        except:
            raise ExceptionFormatter.get_full_exception()
    
    
    def _validate_queues_deleted(self, queue_name_set):
        """
        """
        
        try:
            
            # Test against queue names first since they are simpler objects.
            current_queue_name_list = self.shared_memory_manager.get_queue_name_list()
            for queue_name in queue_name_set:
                if queue_name in current_queue_name_list:
                    return False
                
            # Test against exchanges.
            current_exchange_dict = self.shared_memory_manager.get_exchange_wrapper_dict()
            for queue_name in queue_name_set:
                for exchange in list(current_exchange_dict.values()):
                    if exchange.is_queue_attached(queue_name) == True:
                        return False
            
            # We have validation if none of the given client queues have failed.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception() 
                    

    def _validate_shared_memory(self, test_method, pass_condition, time_out, *test_method_arguments):
        """
        Designed to wait for shared memory to synchronize to the given condition.
        Supply the method to test on and the arguments to test with.
        This method will continue to test and sleep until the test returns the given pass condition.
        This method will allow for a timeout, as specified by the synchronize settings.
        If the method passes before time out, True is returned.
        If the method fails to pass before time out, False is returned.
        """
        
        # Set up our sleep data.
        sleep_count = 0
        sleep_interval = time_out / self.shared_memory_validation_check_count / 1000.
        
        # Enter the loop, checking the method with the given arguments against our pass condition.
        while test_method(*test_method_arguments) != pass_condition:
            
            # If we didn't, pass, sleep.
            time.sleep(sleep_interval)
            sleep_count += 1
            
            # If we exceeded our max sleep count, we have timed out.  
            # Return out.
            if sleep_count > self.shared_memory_validation_check_count:
                return False

        # Return synchronization found.
        return True
