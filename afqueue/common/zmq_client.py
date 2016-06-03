from afqueue.common.client_data_subscription import ClientDataSubscription #@UnresolvedImport
from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.socket_wrapper import SocketWrapper #@UnresolvedImport
from afqueue.common.zmq_utilities import ZmqUtilities #@UnresolvedImport
from afqueue.config_files import queue_defines as defines #@UnresolvedImport
from afqueue.messages import client_messages, client_response_codes #@UnresolvedImport

import simplejson as json
import bson
import os, inspect
import zmq #@UnresolvedImport

# If logging is needed to a debug file, uncomment and use the logger.
from afqueue.common.logger import Logger
import logging
log_file_path = "/var/log/appfirst/queue.zmq_client.log"
log_method = "rotate"#"local1"
logger = Logger("AF_QM", "queue", logging.INFO, log_method, log_file_path, 30) 


class ReplyFailureException(Exception):
    """
    Denotes an exception encountered while handling replies from the QM.
    """
    pass


class ReplyTimeoutException(Exception):
    """
    Denotes an exception encountered while waiting for a reply from the QM.
    """
    pass


class ZmqClient(object):
    
    def __init__(self, queue_server_list, current_queue_server_index = 0):
        
        # Set up.
        self._queue_server_list = queue_server_list
        self._current_queue_server_index = current_queue_server_index
        self._process_id_string = str(os.getpid())

        # Setup our connection string based on the current queue server index.
        self._connection_string = self._get_queue_server_connection_string(self._current_queue_server_index)
        
        # Track all the exchanges and queues we declare.
        # This allows us to redeclare them later if we change queue servers or our current queue server complains one doesn't exist.
        # The queue dictionary has queue names for keys; definition tuples for that queue (exchange used, queue types, etc) for values.
        self._client_exchange_list = list()
        self._client_queue_list = list()
        
        # Track the queue callback map.
        # The key is built from a tuple of queue name and routing key.
        self._callback_key_map = dict()
        
        # Track the routing keys subscribed to for each queue; this allows us to match an incoming routing key for a queue quickly and properly.
        self._queue_routing_keys_map = dict()
        
        # Track a dumped list of all queue names currently subscribed to for faster/more efficient data requests.
        self._queue_name_subscription_list_dumped_string = json.dumps(list())
        
        # Track the set of all queue names we have requested locks on.
        self._queue_name_lock_set = set()
            
        # We have to track request failures.
        # If we fail due to a time out, we don't want to send a new request, nor do we want to recreate our sockets.
        self.failed_request_count = 0
        self.failed_send_count = 0
        self.failed_store_count = 0
        self.failed_retrieve_count = 0
        self.failed_get_stores_list_count = 0
        self.failed_get_pecking_order_count = 0
        
        # We have to eventually give up and assume the failures due to time outs are a socket issue and recreate them.
        # Note that when a true socket error occurs during a request, the failed count is set to this value immediately to force a recreation.
        self.failed_request_count_to_recreate_socket = 5 
        self.failed_send_count_to_recreate_socket = 5 
        self.failed_store_count_to_recreate_socket = 5 
        self.failed_retrieve_count_to_recreate_socket = 5 
        self.failed_get_stores_list_count_to_recreate_socket = 5 
        self.failed_get_pecking_order_to_recreate_socket = 5 
        

    def connect(self):
        """
        Runs connection code to initiate a connection between this process and the Queue Manager (QM).
        The QM which will be used is designed by the loaded queue servers and the current queue server index.
        """
        
        # Open a context, create a poller, and use this information to create the request socket.
        self._zmq_context = zmq.Context()
        self._poller = zmq.Poller()
        self._create_request_socket()
        
        
    def delete_queues(self, queue_name_list, timeout = defines.CLIENT_DEFAULT_DELETE_QUEUES_REQUEST_TIME_OUT):
        """
        """
                
        try:
            
            # If this queue has been locked by us, we must first unlock it.  
            unlock_queue_name_list = list()
            for queue_name in queue_name_list:
                if queue_name in self._queue_name_lock_set:
                    unlock_queue_name_list.append(queue_name)
                    
            # Pass all queues which require an unlock onto our unlock list.
            if len(unlock_queue_name_list) > 0:
                self._unlock_queues(unlock_queue_name_list)
                
            # Create the exchange declaration request message and send it to the QM.
            request_message = client_messages.ClientDeleteQueuesRequestMessage(queue_name_list)
            request_message.send(self._request_socket)

            # Wait for a reply.
            if self._poller.poll(timeout):
                
                # Translate the reply.
                reply_message = client_messages.ClientDeleteQueuesReplyMessage.create_from_received(SocketWrapper.pull_message(self._request_socket))            
                                
                # Check each response codes.
                failure_message = ""
                for queue_name, response_code in list(reply_message.response_code_dict.items()):
                    if int(response_code) >= 500:
                        failure_message += "Queue {0} : {1};".format(queue_name, client_response_codes.convert_response_code_to_string(response_code))
                        
                # If we have content in our failure message, raise an exception.
                if failure_message != "":
                    raise ReplyFailureException("Failed to delete all queues.  Failed: " + failure_message)
                
            # If we didn't get a reply in time, raise an exception. 
            else:
                raise ReplyTimeoutException("Timeout while waiting for reply from QM")   
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def declare_exchanges(self, client_exchange_list, timeout = defines.CLIENT_DEFAULT_DECLARE_EXCHANGES_REQUEST_TIME_OUT):
        """
        Declares an exchange on the connected Queue Manager (QM).
        Will allow the operation to take as long as needed, capped by the specified timeout value (microseconds).
        Raises an exception if either the time out is reached or if the response from the QM was an error which should
            ultimately cause the client process to close.
        """
        
        try:
            
            # Do not allow an empty declaration.
            if len(client_exchange_list) == 0:
                logger.log_info("*** Zero exchanges declared; doing nothing.")
                logger.log_info(inspect.stack()[1:])
                return 
            
            # Register the exchanges in our tracker.
            self.register_exchanges(client_exchange_list)
                
            # Create the exchange declaration request message and send it to the QM.
            request_message = client_messages.ClientDeclareExchangesRequestMessage(client_exchange_list)
            request_message.send(self._request_socket)

            # Wait for a reply.
            if self._poller.poll(timeout):
                
                # Translate the reply.
                reply_message = client_messages.ClientDeclareExchangesReplyMessage.create_from_received(SocketWrapper.pull_message(self._request_socket))
                
                # Check each response codes.
                failure_message = ""
                for exchange_name, response_code in list(reply_message.response_code_dict.items()):
                    if int(response_code) >= 500:
                        failure_message += "Exchange {0} : {1};".format(exchange_name, client_response_codes.convert_response_code_to_string(response_code))
                        
                # If we have content in our failure message, raise an exception.
                if failure_message != "":
                    raise ReplyFailureException("Failed to declare all exchanges.  Failed: " + failure_message)
                
            # If we didn't get a reply in time, raise an exception. 
            else:
                raise ReplyTimeoutException("Timeout while waiting for reply from QM")   
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
        
    def declare_exchange(self, client_exchange, timeout = defines.CLIENT_DEFAULT_DECLARE_EXCHANGE_REQUEST_TIME_OUT):
        """
        Declares an exchange on the connected Queue Manager (QM).
        Will allow the operation to take as long as needed, capped by the specified timeout value (microseconds).
        Raises an exception if either the time out is reached or if the response from the QM was an error which should
            ultimately cause the client process to close.
        """
        
        try:
            
            # Forward the exchange name as a list.
            self.declare_exchanges([client_exchange,], timeout)
            
        except:
            
            raise ExceptionFormatter.get_full_exception()

        
    def declare_queues(self, client_queue_list, timeout = defines.CLIENT_DEFAULT_DECLARE_QUEUES_REQUEST_TIME_OUT):
        """
        Declares a queue and attaches it to the exchange on the connected Queue Manager (QM).
        Will allow the operation to take as long as needed, capped by the specified timeout value (microseconds).
        Raises an exception if either the time out is reached or if the response from the QM was an error which should
            ultimately cause the client process to close.
        """
                
        try:
            
            # Do not allow an empty declaration.
            if len(client_queue_list) == 0:
                logger.log_info("*** Zero queues declared; doing nothing.")
                logger.log_info(inspect.stack()[1:])
                return 
            
            # Register the queues in our tracker.
            self.register_queues(client_queue_list)
                
            # Create the queue declaration request message and send it to the QM.
            request_message = client_messages.ClientDeclareQueuesRequestMessage(client_queue_list)
            request_message.send(self._request_socket)

            # Wait for a reply.
            if self._poller.poll(timeout):
                
                # Translate the reply.
                reply_message = client_messages.ClientDeclareQueuesReplyMessage.create_from_received(SocketWrapper.pull_message(self._request_socket))
                
                # Check each response codes.
                failure_message = ""
                for queue_name, response_code in list(reply_message.response_code_dict.items()):
                    if int(response_code) >= 500:
                        failure_message += "Queue {0} : {1};".format(queue_name, client_response_codes.convert_response_code_to_string(response_code))
                        
                # If we have content in our failure message, raise an exception.
                if failure_message != "":
                    raise ReplyFailureException("Failed to declare all queues.  Failed: " + failure_message)
                 
            # If we didn't get a reply in time, raise an exception. 
            else:
                raise ReplyTimeoutException("Timeout while waiting for reply from QM")   
            
        except:
            
            raise ExceptionFormatter.get_full_exception()

        
    def declare_queue(self, zmq_client_queue, timeout = defines.CLIENT_DEFAULT_DECLARE_QUEUE_REQUEST_TIME_OUT):
        """
        Declares a queue and attaches it to the exchange on the connected Queue Manager (QM).
        Will allow the operation to take as long as needed, capped by the specified timeout value (microseconds).
        Raises an exception if either the time out is reached or if the response from the QM was an error which should
            ultimately cause the client process to close.
        """
                
        try:
            
            # Forward the queue as a list.
            self.declare_queues([zmq_client_queue,], timeout)
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
    
    def disconnect(self):
        """
        Runs code to disconnect the connection between this process and the Queue Manager (QM).
        """
        
        # On graceful shutdown, we must unlock any locked queues.
        if len(self._queue_name_lock_set) > 0:
            self._unlock_queues(list(self._queue_name_lock_set))                
                
        # Close down the request socket and destroy the context.
        self._destroy_request_socket()
        self._zmq_context.term()
            
            
    def get_data_stores_dict(self, timeout = 3000):
        """
        Sends a get data stores command to the connected QM.
        Returns two elements - handle as appropriate:
            An error notification in string format
            A dictionary of list of data store connection information.
            The dictionary contains an "accepting" key, with values being a list of data stores which are accepting new data.
            The dictionary contains an "rejecting" key, with values being a list of data stores which are not accepting new data.            
        """
        
        try:
              
            # Store the data.
            response, notification = self._get_data_stores_dict(timeout)
            
            # If the response is False, we failed due to a socket time out.
            if response == False:
                
                # If our overall failure count has been exceeded, recreate the socket.
                if self.failed_get_stores_list_count >= self.failed_get_stores_list_count_to_recreate_socket:
                    
                    # Recreate.
                    self._recreate_socket_connection()
                    return "Failure to get data stores list: {0}.  Socket has been recreated; next retrieve will be on a fresh connection.".format(notification), None
                    
                # If we haven't exceeded our failure count, just notify that we failed this time.
                else:
                    return "Failure to get data stores list: {0}.  Failure count: {1}".format(notification, self.failed_get_stores_list_count), None
            
            # If the response is a failure, ...
            if int(response.response_code) >= 500:
                
                # Get the response code as a string and return the message.
                response_code_as_string = client_response_codes.convert_response_code_to_string(response.response_code)
                return "Failure to get data stores list.  Failure notification from QM: {0}".format(response_code_as_string), None
            
            # Massage the reply message into our return dictionary.
            return_dict = dict()
            return_dict["accepting"] = response.accepting_data_store_list
            return_dict["rejecting"] = response.rejecting_data_store_list
             
            # Return the retrieved data dictionary with no error notification.
            return None, return_dict
                
        except:
            
            raise ExceptionFormatter.get_full_exception()
                
                
    def get_pecking_order(self, timeout = 3000):
        """
        Sends a get pecking order command to the connected QM.
        Returns two elements - handle as appropriate:
            An error notification in string format
            An ordered list of QMs in the mesh, the first of which is the current master node.            
        """        
        try:
            # Send the request.
            response, notification = self._get_pecking_order(timeout)
            
            # If the response is False, we failed due to a socket time out.
            if response == False:
                # If our overall failure count has been exceeded, recreate the socket.
                if self.failed_get_pecking_order_count >= self.failed_get_pecking_order_to_recreate_socket:
                    self._recreate_socket_connection()
                    return "Failure to get pecking order: {0}.  Socket has been recreated; next retrieve will be on a fresh connection.".format(notification), None
                    
                # If we haven't exceeded our failure count, just notify that we failed this time.
                else:
                    return "Failure to get pecking order: {0}.  Failure count: {1}".format(notification, self.failed_get_pecking_order_count), None
            
            # If the response is a failure, get the response code as a string and return the message.
            if int(response.response_code) >= 500:
                response_code_as_string = client_response_codes.convert_response_code_to_string(response.response_code)
                return "Failure to get pecking order.  Failure notification from QM: {0}".format(response_code_as_string), None
                         
            # Return the retrieved list with no error notification.
            return None, response.pecking_order_list
                
        except:            
            raise ExceptionFormatter.get_full_exception()

        
    def lock_queues(self, queue_lock_list):
        """
        """
                
        try:
            
            # Update each queue lock with the current PID.
            for queue_lock in queue_lock_list:
                queue_lock.process_id_string = self._process_id_string
            
            # Create the queue lock request message and send it to the QM.
            request_message = client_messages.ClientLockQueuesRequestMessage(queue_lock_list)
            request_message.send(self._request_socket)

            # Wait for a reply.
            if self._poller.poll(defines.CLIENT_LOCK_QUEUE_REQUEST_TIME_OUT):
                
                # Translate the reply.
                reply_message = client_messages.ClientLockQueuesReplyMessage.create_from_received(SocketWrapper.pull_message(self._request_socket))
                
                # Check each response codes.
                # Append failures to a larger failure message; append successes to the set of locked queue names.
                failure_message = ""
                for queue_name, response_code in list(reply_message.response_code_dict.items()):
                    if int(response_code) >= 500:
                        failure_message += "Queue {0} : {1};".format(queue_name, client_response_codes.convert_response_code_to_string(response_code))
                    else:
                        self._queue_name_lock_set.add(queue_name)
                        
                # If we have content in our failure message, raise an exception.
                if failure_message != "":
                    raise ReplyFailureException("Failed to lock all queues.  Failed: " + failure_message)
                 
            # If we didn't get a reply in time, raise an exception. 
            else:
                raise ReplyTimeoutException("Timeout while waiting for reply from QM")   
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def requeue_data(self, queue_name, routing_key, data, timeout=3000):
        """
        
        """
        try:
            
            request_message = client_messages.ClientRequeueDataRequestMessage(queue_name, routing_key, data)
            request_message.send(self._request_socket)

            # Regardless of the previous state, we'll need to poll for data.
            if self._poller.poll(timeout):
                
                # We got a response.
                # Reset our failed out, get the reply message, and return it.
                self.failed_send_count = 0
                reply_message = client_messages.ClientRequeueDataReplyMessage.create_from_received(SocketWrapper.pull_message(self._request_socket))
                return reply_message, ""
            
            else:
                
                # We got no response.
                # Increment our failed request count, return failure with a timed out notification.
                self.failed_send_count += 1
                return False, "Timed out"    

        except:
            
            raise ExceptionFormatter.get_full_exception()
        

    def register_exchanges(self, client_exchange_list):
        """
        Registers the exchanges.  
        Note that this is done automatically during a declare call and should not be done again.
        """
        
        try:
            
            # Get new exchanges only.
            validated_list = list()
            for client_exchange in client_exchange_list:
                validated = True
                for tracked_client_exchange in self._client_exchange_list:
                    if client_exchange.name == tracked_client_exchange.name and client_exchange.type == tracked_client_exchange.type:
                        validated = False
                        break
                if validated == True:
                    validated_list.append(client_exchange)

            # Register the exchanges in our tracker.
            self._client_exchange_list.extend(validated_list)
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def register_queues(self, client_queue_list):
        """
        Registers the queues.  
        Note that this is done automatically during a declare call and should not be done again.
        """
        
        try:
            
            # Register the queues in our tracker.
            self._client_queue_list.extend(client_queue_list)
            
        except:
            
            raise ExceptionFormatter.get_full_exception()


    def request_data(self, count = 50, timeout = 100):
        """
        Requests data from the connected Queue Manager (QM) for all queues which have been previously subscribed to by this process.
        Data received will be sent to the callback which was registered to each queue's data during the subscription call.
        Will get as much data as possible for each subscribed queue, capped by the specified count.
        Will allow the operation to take as long as needed, capped by the specified timeout value (microseconds).
        Returns None if processing proceeds without issue.
        Returns a string-based notification message if this method needs to notify the caller.
        Raises an exception if any critical errors occur.
        Note: data objects pushed and popped from the queue server must always be string representations of those objects.
        """
        
        try:
            
            # Push the data.
            response, notification = self._request_data(count, timeout)
            
            # If the response is False, we failed due to a socket time out.
            if response == False:
                
                # If our overall failure count has been exceeded, recreate the socket.
                if self.failed_request_count >= self.failed_request_count_to_recreate_socket:
                    
                    # Recreate.
                    self._recreate_socket_connection()
                    return "Failure to request data: {0}.  Socket has been recreated; next request will be on a fresh connection.".format(notification)
                    
                # If we haven't exceeded our failure count, just notify that we failed this time.
                else:
                    return "Failure to request data: {0}.  Failure count: {1}".format(notification, self.failed_request_count)
            
            # Handle sending data received to the callback definitions we have.
            # The queue data pulled dictionary is a dictionary indexed off of keys with the values being a 2 item tuple.
            # The first item in the tuple is a list of dumped data tuples from the queue; the second item is the number of items the server had remaining after the pull for the queue.
            # The dumped data tuples need to be loaded before processed; each tuple has two items: the routing key and the actual data.
            # Note we have to track queues which failed to route correctly.
            failed_to_route_count_dict = dict()
            total_data_remaining_count = 0
            requeue_flag = False
            for queue_name, pulled_data_tuple in list(response.queue_data_pulled_dict.items()):
                
                # Break the tuple.
                dumped_data_map_list = pulled_data_tuple[0]
                total_data_remaining_count += pulled_data_tuple[1]

                # Go through each dumped data tuple for the queue.
                for dumped_data_map in dumped_data_map_list:
                    
                    # If we have valid data.
                    if dumped_data_map != "":

                        # Load the routing key and data.
                        routing_key_to_data_dict = bson.loads(dumped_data_map)       
                        for routing_key, data in list(routing_key_to_data_dict.items()):
                            
                            try:

                                # Get the matching callback key data.
                                matched_routing_key = self._get_matching_callback_key(queue_name, routing_key)
                                
                                # If we have been told to put the data back on the queue, do so.
                                # For some strange reason we have to encode it back to ascii from unicode.  
                                if requeue_flag:                                    
                                    self.requeue_data(queue_name.encode('ascii'), routing_key.encode('ascii'), data)
                                    
                                # If we have no matching callback key, track in our error.
                                elif matched_routing_key == None:
                                    failed_callback_key = ClientDataSubscription.get_callback_key(queue_name, routing_key)
                                    failed_to_route_count_dict[failed_callback_key] = failed_to_route_count_dict.get(failed_callback_key, 0) + 1
                                
                                # If we have matching callback key, send the data to our handler.       
                                else:
                                    
                                    # 
                                    matched_callback_key = ClientDataSubscription.get_callback_key(queue_name, matched_routing_key)
                                    if hasattr(self,'routing_key_msg_types_map'):
                                        cls = self.routing_key_msg_types_map[routing_key]
                                        obj = cls() if inspect.isclass(cls) else cls.__class__()
                                        obj.deserialize(data)
                                    else:
                                        obj = data

                                    # Call the handler.
                                    self._callback_key_map[matched_callback_key].handler_method(obj)
                                    
                            except Exception as e:
                                
                                # DEBUG.
                                print("ZMQ Client Request Data Error", ExceptionFormatter.get_message())
                                
                                # If an unusual exception occurs we will want to requeue all of the extra data.
                                requeued_exc = e
                                requeue_flag = True
                            
            # Handle building a failure message for queues which got data for a routing key which we don't have a subscription for.
            failed_to_route_message = ""
            for callback_key, failed_count in list(failed_to_route_count_dict.items()):
                failed_to_route_message += "{0} - {1} messages; ".format(callback_key, failed_count)
            
            # Error out on the failures if we had any.
            if failed_to_route_message != "":
                raise Exception("Failed to handle routing keys from queues.  Failure summary: " + failed_to_route_message)
    
            # Handle the response codes.
            redeclare_flag = False
            for queue_name, response_code in list(response.response_code_dict.items()):
                
                # If the response tells us that the queue does not exist, set our redeclare flag so we can attempt to resolve the issue.
                if response_code == client_response_codes.QUEUE_DOES_NOT_EXIST:
                    redeclare_flag = True
                
                # DEBUG
                if int(response_code) >= 500:
                    response_code_as_string = client_response_codes.convert_response_code_to_string(response_code)
                    print("response received for queue {0}: {1}".format(queue_name, response_code_as_string))
                        
            # Redeclare if our flag tells us to.
            if redeclare_flag == True:
                self._redeclare_from_tracker()

            if requeue_flag == True:
                #raise Exception("Exception raised in callback functions, requeued data and raised an exception out") 
                raise requeued_exc
                
            # Return the total amount of data remaining in all queues queried.
            return total_data_remaining_count
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
    
    def send_data_to_pika_endpoint(self, exchange_name, routing_key, data, timeout = 1000):
        """
        Functions similar to normal send_data method, except does not check return codes / attempt to redeclare exchanges / queues on failures.
        """
        
        try:
            
            # Push the data.
            response, notification = self._send_data(exchange_name, routing_key, data, timeout)
            
            # If the response is False, we failed due to a socket time out.
            if response == False:
                
                # Recreate and retry.
                self._recreate_socket_connection()
                response, notification = self._send_data(exchange_name, routing_key, data, timeout)
                
                # If we failed again, return out.
                if response == False:
                    return "Failure to send: {0}.  Socket has been recreated; data will be sent again.".format(notification)
                
            # If the response is a failure, ...
            if int(response.response_code) >= 500:
                
                # Get the response code as a string and return the message.
                response_code_as_string = client_response_codes.convert_response_code_to_string(response.response_code)
                return "Failure to push to {0}/{1}.  Failure notification from QM: {2}".format(exchange_name. routing_key, response_code_as_string)
                
            # Return None to signify no error during processing.
            return None
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
    
    def send_data(self, exchange_name, routing_key, data, timeout = 3000, encode = True):      
        """
        Sends the given data to the given exchange on the connected Queue Manager (QM).        
        Will allow the operation to take as long as needed, capped by the specified timeout value (microseconds).
        Returns None if processing proceeds without issue.
        Returns a string-based notification message if this method needs to notify the caller.
        Raises an exception if any critical errors occur.
        Note: data objects pushed and popped from the queue server must always be string representations of those objects.
        """  
        
        try:
            
            # Encode the data if necessary.
            if encode == True:
                pass
                #data = data.encode('base64')
                                    
            # Push the data.
            response, notification = self._send_data(exchange_name, routing_key, data, timeout)
            
            # If the response is False, we failed due to a socket time out.
            if response == False:
                
                # If our overall failure count has been exceeded, recreate the socket.
                if self.failed_send_count >= self.failed_send_count_to_recreate_socket:
                    
                    # Recreate.
                    self._recreate_socket_connection()
                    return "Failure to send data: {0}.  Socket has been recreated; next send will be on a fresh connection.".format(notification)
                    
                # If we haven't exceeded our failure count, just notify that we failed this time.
                else:
                    return "Failure to send data: {0}.  Failure count: {1}".format(notification, self.failed_send_count)
            
            # If the response tells us that the exchange does not exist, declare the exchange and attempt the push again.
            if response.response_code == client_response_codes.EXCHANGE_DOES_NOT_EXIST:
                
                # Redeclare.
                self._redeclare_from_tracker()
                
                # Attempt again.
                response, notification = self._send_data(exchange_name, routing_key, data, timeout)
                
                # Check for a failure from the response immediately.
                if response == False:
                    return "Failure to send: {0}.  Socket has been recreated; data will be sent again.".format(notification)
                
            # If the response is a failure, ...
            if int(response.response_code) >= 500:
                
                # Get the response code as a string and return the message.
                response_code_as_string = client_response_codes.convert_response_code_to_string(response.response_code)
                return "Failure to push to {0}/{1}.  Failure notification from QM: {2}".format(exchange_name, routing_key, response_code_as_string)
                
            # Return None to signify no error during processing.
            return None
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
                
    
    def retrieve_data(self, full_key_get_list, full_key_delete_list, timeout = 3000):
        """
        Sends a retrieve data command to the connected QM.
        Note that when attempting to read or delete keys from QM storage, you must provide a fully qualified key.
        Use the ZMQ Utility method get_full_storage_key to build the full key from a store and location key.
        Returns two elements - handle as appropriate:
            An error notification in string format
            A dictionary with each full key as the key and the data retrieved as the value
            Note: Either value could be None, depending on the success of the storage command.
        Will delete any keys specified.
        """
        
        try:
              
            # Store the data.
            response, notification = self._retrieve_data(full_key_get_list, full_key_delete_list, timeout)
            
            # If the response is False, we failed due to a socket time out.
            if response == False:
                
                # If our overall failure count has been exceeded, recreate the socket.
                if self.failed_retrieve_count >= self.failed_retrieve_count_to_recreate_socket:
                    
                    # Recreate.
                    self._recreate_socket_connection()
                    return "Failure to retrieve data: {0}.  Socket has been recreated; next retrieve will be on a fresh connection.".format(notification), None
                    
                # If we haven't exceeded our failure count, just notify that we failed this time.
                else:
                    return "Failure to retrieve data: {0}.  Failure count: {1}".format(notification, self.failed_retrieve_count), None
            
            # If the response is a failure, ...
            if int(response.response_code) >= 500:
                
                # Get the response code as a string and return the message.
                response_code_as_string = client_response_codes.convert_response_code_to_string(response.response_code)
                return "Retrieved {0}.  Failure notification from QM: {1}".format(full_key_get_list, response_code_as_string), None
            
            # Return the retrieved data dictionary with no error notification.
            return None, response.full_key_to_data_dict
                
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def store_data(self, store_key_to_data_dict, expire_seconds, timeout = 3000):
        """
        Sends a store data command to the connected QM.
        Expects a dictionary keyed off the store key, values must be data represented as a string.
        Returns two elements - handle as appropriate:
            An error notification in string format
            The location key where all keys were stored
            Note: Either value could be None, depending on the success of the storage command.
        Note that when attempting to read or delete keys from QM storage, you must provide a fully qualified key.
        Use the ZMQ Utility method get_full_storage_key to build the full key from a store and location key.
        """
        
        try:
              
            # Store the data.
            response, notification = self._store_data(store_key_to_data_dict, expire_seconds, timeout)
            
            # If the response is False, we failed due to a socket time out.
            if response == False:
                
                # If our overall failure count has been exceeded, recreate the socket.
                if self.failed_store_count >= self.failed_store_count_to_recreate_socket:
                    
                    # Recreate.
                    self._recreate_socket_connection()
                    return "Failure to store data: {0}.  Socket has been recreated; next store will be on a fresh connection.".format(notification), None
                    
                # If we haven't exceeded our failure count, just notify that we failed this time.
                else:
                    return "Failure to store data: {0}.  Failure count: {1}".format(notification, self.failed_store_count), None
            
            # If the response is a failure, ...
            if int(response.response_code) >= 500:
                
                # Get the response code as a string and return the message.
                response_code_as_string = client_response_codes.convert_response_code_to_string(response.response_code)
                return "Storage of data failed on keys {0}.  Failure notification from QM: {1}".format(list(store_key_to_data_dict.keys()), response_code_as_string), None
            
            # Return the location key with no error notification.
            return None, response.location_key

        except:
            
            raise ExceptionFormatter.get_full_exception()
    

    def subscribe_to_queue_map(self, callback_map, queue_name, routing_key_msg_types_map):
        self.routing_key_msg_types_map = routing_key_msg_types_map

        for routing_key in list(routing_key_msg_types_map.keys()):
            cb_key = routing_key_msg_types_map[routing_key]
            client_data_subscription = ClientDataSubscription(queue_name, routing_key, callback_map[cb_key]) 
            self.subscribe_to_queue(client_data_subscription)


    def subscribe_to_queue(self, client_data_subscription):
        """
        Subscribes the given callback method to data from the given queue.
        Future requests on data which yield data from the given queue name will be fed to the given callback method.
        Note that each queue name can only correspond to one callback method.
        """
        
        # Create our key.
        client_data_request_key = ClientDataSubscription.get_callback_key(client_data_subscription.queue_name, client_data_subscription.routing_key)
        
        # Set the queue name's callback method in our tracker map.
        self._callback_key_map[client_data_request_key] = client_data_subscription

        # Register the routing key information in the routing key map.
        # Note that if we receive an empty routing key, we replace all prior routing keys in the map.
        if client_data_subscription.routing_key == "":
            self._queue_routing_keys_map[client_data_subscription.queue_name] = [client_data_subscription.routing_key,]
        else:
            self._queue_routing_keys_map.setdefault(client_data_subscription.queue_name, list()).append(client_data_subscription.routing_key)
            
        # Create a list of all queue names we are subscribed to and dump it; this will speed up our data requests by having the list always ready.
        queue_name_list = [cds.queue_name for cds in list(self._callback_key_map.values())]
        self._queue_name_subscription_list_dumped_string = json.dumps(list(set(queue_name_list)))
        
        
    def _create_request_socket(self):
        """
        Creates a new request socket and adds it to the poller.
        """
        
        self._request_socket = self._zmq_context.socket(zmq.REQ)
        self._request_socket.setsockopt(zmq.LINGER, 0)
        self._request_socket.connect(self._connection_string)
        self._poller.register(self._request_socket, zmq.POLLIN)
    
    
    def _destroy_request_socket(self):
        """
        Destroys the current request socket and removes it from the poller.
        """
        
        self._request_socket.close()
        self._poller.unregister(self._request_socket)
                    
            
    def _get_data_stores_dict(self, timeout):
        """
        Sends a get data stores command to the connected QM.
        Returns two elements - handle as appropriate:
            A valid reply message if we received a response or a failure flag (False) if we did not.
            An error notification in string format corresponding to what caused the failure.     
        """
        
        try:
            
            # If we haven't failed on a previous call, we should be sending a new send.
            if self.failed_get_stores_list_count == 0:
                request_message = client_messages.ClientGetDataStoresRequestMessage()
                request_message.send(self._request_socket)
        
            # Regardless of the previous state, we'll need to poll for data.
            if self._poller.poll(timeout):
                
                # We got a response.
                # Reset our failed out, get the reply message, and return it.
                self.failed_get_stores_list_count = 0
                reply_message = client_messages.ClientGetDataStoresReplyMessage.create_from_received(SocketWrapper.pull_message(self._request_socket))
                return reply_message, ""
            
            else:
                
                # We got no response.
                # Increment our failed request count, return failure with a timed out notification.
                self.failed_get_stores_list_count += 1
                return False, "Timed out"

        except zmq.ZMQError:
                
            # A socket error requires us to recreate the socket; set the failed request count to our max count to ensure this happens.
            self.failed_get_stores_list_count = self.failed_get_stores_list_count_to_recreate_socket
            return False, "Socket failure"
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
                    
            
    def _get_pecking_order(self, timeout):
        """
        Sends a get pecking order command to the connected QM.
        Returns two elements - handle as appropriate:
            A valid reply message if we received a response or a failure flag (False) if we did not.
            An error notification in string format corresponding to what caused the failure.     
        """        
        try:            
            # If we haven't failed on a previous call, we should be sending a new send.
            if self.failed_get_pecking_order_count == 0:
                request_message = client_messages.ClientGetPeckingOrderRequestMessage()
                request_message.send(self._request_socket)
        
            # Regardless of the previous state, we'll need to poll for data.
            if self._poller.poll(timeout):                
                # We got a response.
                # Reset our failed out, get the reply message, and return it.
                self.failed_get_pecking_order_count = 0
                reply_message = client_messages.ClientGetPeckingOrderReplyMessage.create_from_received(SocketWrapper.pull_message(self._request_socket))
                return reply_message, ""
            
            else:                
                # We got no response.
                # Increment our failed request count, return failure with a timed out notification.
                self.failed_get_pecking_order_count += 1
                return False, "Timed out"

        except zmq.ZMQError:                
            # A socket error requires us to recreate the socket; set the failed request count to our max count to ensure this happens.
            self.failed_get_pecking_order_count = self.failed_get_stores_list_count_to_recreate_socket
            return False, "Socket failure"
            
        except:            
            raise ExceptionFormatter.get_full_exception()
        

    def _get_matching_callback_key(self, queue_name, routing_key):
        """
        Return the routing key subscribed to in the given queue which matches the given routing key.
        Note that if a routing key of "" is defined for a queue subscription, any given routing key will match it.
        """        
        
        # Get the list of routing keys for this queue.
        callback_routing_key_list = self._queue_routing_keys_map.get(queue_name, list())
        
        # Check each callback key for a match.
        matched_routing_key = None
        for callback_routing_key in callback_routing_key_list:
            
            # If the key is "", match automatically.
            if callback_routing_key == "":
                matched_routing_key = ""
                
            elif callback_routing_key == routing_key:
                matched_routing_key = routing_key
                
        # Check our matched routing key.
        if matched_routing_key == None:
            return None
                
        # Return the match.
        return matched_routing_key
        
        
    def _get_queue_server_connection_string(self, queue_server_index):
        """
        Returns the connection string to the queue server at the given index in the configuration file for this process.
        """
        
        # Load the queue server information.
        queue_server = self._queue_server_list[queue_server_index]
        ip_address, port = queue_server.split(":")
        port = int(port)

        # Return the socket connection string.
        return ZmqUtilities.get_socket_connection_string(ip_address, port)
        
        
    def _recreate_socket_connection(self):
        """
        Recreates the socket connection to the current Queue Manager (QM).
        """
        
        # Destroy the socket first; create it second.
        # SMK: This is where we eventually would have smart logic to manage connecting to a live QM.
        #     We would track recreations within a time period and change to a different QM as necessary, or something along those lines.
        self._destroy_request_socket()
        self._create_request_socket()
        
        # Reset our failure counts.
        self.failed_request_count = 0
        self.failed_send_count = 0
        self.failed_store_count = 0
        self.failed_retrieve_count = 0
        self.failed_get_stores_list_count = 0
        self.failed_get_pecking_order_count = 0
        
        
    def _redeclare_from_tracker(self):
        """
        Sends another declaration request for each exchange and queue previously declared in this process.
        """
        
        # Declare all exchanges.
        self.declare_exchanges(self._client_exchange_list)
        
        # Declare all queues.
        self.declare_queues(self._client_queue_list)

        
    def _request_data(self, count, timeout):
        """
        Requests data from the connected Queue Manager (QM) for all queues which have been previously subscribed to by this process.
        Will get as much data as possible for each subscribed queue, capped by the specified count.
        Will allow the operation to take as long as needed, capped by the specified timeout value (microseconds).
        Returns two parameters.
        If the request yields data:
         Returns the data message and an empty error string.
        If the request fails:
         Returns False and an error string.
        Note that this method manages the internal failed request counter, which is responsible for signaling when to send new requests vs poll against old requests
         as well as when to recreate the entire socket connection.
        """  
        
        try:
            
            # We have two states: 
            #  The previous request yielded a response within the timeout period: we send a new request.
            #  The previous request did not yield a response within the timeout period: we poll again but send no new request.
            
            # If we haven't failed on a previous call, we should be sending a new request.
            if self.failed_request_count == 0:
                request_message = client_messages.ClientDataPullRequestMessage(self._queue_name_subscription_list_dumped_string, count)
                request_message.send(self._request_socket)
                
            # Regardless of the previous state, we'll need to poll for data.
            if self._poller.poll(timeout):
                
                # We got a response.
                # Reset our failed out, get the reply message, and return it.
                self.failed_request_count = 0
                reply_message = client_messages.ClientDataPullReplyMessage.create_from_received(SocketWrapper.pull_message(self._request_socket))
                return reply_message, ""
            
            else:
                
                # We got no response.
                # Increment our failed request count, return failure with a timed out notification.
                self.failed_request_count += 1
                return False, "Timed out"

        except zmq.ZMQError:
                
            # A socket error requires us to recreate the socket; set the failed request count to our max count to ensure this happens.
            self.failed_request_count = self.failed_request_count_to_recreate_socket
            return False, "Socket failure"
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
                    
            
    def _retrieve_data(self, full_key_get_list, full_key_delete_list, timeout):
        """
        Sends a retrieve data command to the connected QM.
        Note that when attempting to read or delete keys from QM storage, you must provide a fully qualified key.
        Use the ZMQ Utility method get_full_storage_key to build the full key from a store and location key.
        Will return a dictionary with each full key as the key and the data retrieved as the value.
        Will delete any keys specified.
        """
        
        try:
            
            # If we haven't failed on a previous call, we should be sending a new send.
            if self.failed_retrieve_count == 0:
                request_message = client_messages.ClientDataRetrieveRequestMessage(full_key_get_list, full_key_delete_list)
                request_message.send(self._request_socket)
        
            # Regardless of the previous state, we'll need to poll for data.
            if self._poller.poll(timeout):
                
                # We got a response.
                # Reset our failed out, get the reply message, and return it.
                self.failed_retrieve_count = 0
                reply_message = client_messages.ClientDataRetrieveReplyMessage.create_from_received(SocketWrapper.pull_message(self._request_socket))
                return reply_message, ""
            
            else:
                
                # We got no response.
                # Increment our failed request count, return failure with a timed out notification.
                self.failed_retrieve_count += 1
                return False, "Timed out"

        except zmq.ZMQError:
                
            # A socket error requires us to recreate the socket; set the failed request count to our max count to ensure this happens.
            self.failed_retrieve_count = self.failed_retrieve_count_to_recreate_socket
            return False, "Socket failure"
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _send_data(self, exchange_name, routing_key, data, timeout):
        """
        Sends the given data to the given exchange on the connected Queue Manager (QM).
        Will allow the operation to take as long as needed, capped by the specified timeout value (microseconds).
        Returns False if the operation times out; automatically attempts to recreate the socket when this happens.
        Returns the reply from the QM if the operation succeeds.
        Returns secondary: a notification message (empty on success; populated on failure).
        """  
        
        try:
            
            # We have two states: 
            #  The previous send yielded a response within the timeout period: we send a new send.
            #  The previous send did not yield a response within the timeout period: we poll again but send no new request.
            
            # If we haven't failed on a previous call, we should be sending a new send.
            if self.failed_send_count == 0:
                request_message = client_messages.ClientDataPushRequestMessage(exchange_name, routing_key, data)
                request_message.send(self._request_socket)
                
            # Regardless of the previous state, we'll need to poll for data.
            if self._poller.poll(timeout):
                
                # We got a response.
                # Reset our failed out, get the reply message, and return it.
                self.failed_send_count = 0
                reply_message = client_messages.ClientDataPushReplyMessage.create_from_received(SocketWrapper.pull_message(self._request_socket))
                return reply_message, ""
            
            else:
                
                # We got no response.
                # Increment our failed request count, return failure with a timed out notification.
                self.failed_send_count += 1
                return False, "Timed out"

        except zmq.ZMQError:
                
            # A socket error requires us to recreate the socket; set the failed request count to our max count to ensure this happens.
            self.failed_send_count = self.failed_send_count_to_recreate_socket
            return False, "Socket failure"
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _store_data(self, store_key_to_data_dict, expire_seconds, timeout):
        """
        Sends a store data command to the connected QM.
        Expects a dictionary keyed off the store key, values must be data represented as a string.
        Returns the location key where all keys were stored.
        Note that when attempting to read or delete keys from QM storage, you must provide a fully qualified key.
        Use the ZMQ Utility method get_full_storage_key to build the full key from a store and location key.
        """
        
        try:
            
            # If we haven't failed on a previous call, we should be sending a new send.
            if self.failed_store_count == 0:
                request_message = client_messages.ClientDataStoreRequestMessage(store_key_to_data_dict, expire_seconds)
                request_message.send(self._request_socket)
                
            # Regardless of the previous state, we'll need to poll for data.
            if self._poller.poll(timeout):
                
                # We got a response.
                # Reset our failed out, get the reply message, and return it.
                self.failed_store_count = 0
                reply_message = client_messages.ClientDataStoreReplyMessage.create_from_received(SocketWrapper.pull_message(self._request_socket))
                return reply_message, ""
            
            else:
                
                # We got no response.
                # Increment our failed request count, return failure with a timed out notification.
                self.failed_store_count += 1
                return False, "Timed out"

        except zmq.ZMQError:
                
            # A socket error requires us to recreate the socket; set the failed request count to our max count to ensure this happens.
            self.failed_store_count = self.failed_store_count_to_recreate_socket
            return False, "Socket failure"
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
                
    def _unlock_queues(self, queue_name_list):
        """
        """
                
        try:

            # Create the queue lock request message and send it to the QM.
            request_message = client_messages.ClientUnlockQueuesRequestMessage(queue_name_list, self._process_id_string)
            request_message.send(self._request_socket)
            
            # Wait for a reply.
            if self._poller.poll(defines.CLIENT_UNLOCK_QUEUE_REQUEST_TIME_OUT):
                
                # Translate the reply.
                reply_message = client_messages.ClientUnlockQueuesReplyMessage.create_from_received(SocketWrapper.pull_message(self._request_socket))
                
                # Check each response codes.
                # Append failures to a larger failure message; append successes to the set of locked queue names.
                failure_message = ""
                for queue_name, response_code in list(reply_message.response_code_dict.items()):
                    if int(response_code) >= 500:
                        failure_message += "Queue {0} : {1};".format(queue_name, client_response_codes.convert_response_code_to_string(response_code))
                    else:
                        self._queue_name_lock_set.discard(queue_name)

                # If we have content in our failure message, raise an exception.
                if failure_message != "":
                    raise ReplyFailureException("Failed to unlock all queues.  Failed: " + failure_message)
                 
            # If we didn't get a reply in time, raise an exception. 
            else:
                raise ReplyTimeoutException("Timeout while waiting for reply from QM")   
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
