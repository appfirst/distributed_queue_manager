from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.socket_wrapper import SocketWrapper #@UnresolvedImport
from afqueue.common.zmq_utilities import ZmqUtilities #@UnresolvedImport
from afqueue.messages import system_messages, command_messages #@UnresolvedImport
from afqueue.messages.base_message import BaseMessage #@UnresolvedImport
import afqueue.messages.message_types as message_types #@UnresolvedImport
from afqueue.source.shared_memory_manager import SharedMemoryManager #@UnresolvedImport
import zmq #@UnresolvedImport
from afqueue.messages import command_response_codes #@UnresolvedImport
from queue import Empty as EmptyQueueException #@UnresolvedImport
import os, time
from multiprocessing import Process #@UnresolvedImport


class MannagerCommandRequestThread:
        
    def __init__(self, thread_name, config, redis_connection_string, command_request_port, action_in_queue, action_out_queue):

        # Assign.
        self._thread_name = thread_name
        self.config = config
        self.redis_connection_string = redis_connection_string
        self.command_request_port = command_request_port
        self.action_in_queue = action_in_queue
        self.action_out_queue = action_out_queue
        
        # Capture configuration data we need.
        self.poll_interval = self.config.manager_command_request_poll_interval
        self.no_activity_sleep_interval = self.config.manager_command_request_no_activity_sleep
        
        # Denote running.
        self.is_running = True
        
        # Create space for our shared memory manager; we will create this when we enter our run loop.
        self.shared_memory_manager = None
        
        # Create the process object.
        self._process = Process(target=self._run_logic, name=thread_name)        
        
        # Create trackers.
        self._qm_queue_size_dictionary = dict()
        self._pqm_queue_size_dictionaries = dict()
        self._full_queue_size_dictionary = None

        
    def start(self):
        """
        Starts the thread's run logic.
        """
        
        # Denote running.
        self.is_running = True
        
        # Start.
        self._process.start() 
        
        
    def stop(self):
        """
        Stops the thread's run logic.
        """
        
        # Denote running.
        self.is_running = False
        
        # Start.
        self._process.join()    
            
            
    def _build_full_queue_size_dictionary(self):
        
        self._full_queue_size_dictionary = dict()
        for queue_name, queue_size in list(self._qm_queue_size_dictionary.items()):
            if queue_name not in list(self._full_queue_size_dictionary.keys()):
                self._full_queue_size_dictionary[queue_name] = 0
            self._full_queue_size_dictionary[queue_name] += queue_size 
            
        for _, pqm_queue_size_dict in list(self._pqm_queue_size_dictionaries.items()):
            for queue_name, queue_size in list(pqm_queue_size_dict.items()):
                if queue_name not in list(self._full_queue_size_dictionary.keys()):
                    self._full_queue_size_dictionary[queue_name] = 0
                self._full_queue_size_dictionary[queue_name] += queue_size 
                   
    
    def _run_logic(self):
                
        # Create our context.
        self.zmq_context = zmq.Context(1)
                
        # Initialize variables.
        request_socket = None
        poller = None
        
        try:
            
            # Send a system message to notify we have started our thread.
            self.action_out_queue.put(system_messages.SystemThreadStateMessage(self._thread_name, True, os.getpid()))    
            
            # Connect to shared memory.
            self.shared_memory_manager = SharedMemoryManager(self.redis_connection_string)
            
            # Create the bridge request socket.
            bind_string = ZmqUtilities.get_socket_bind_string(self.command_request_port)
            request_socket = self.zmq_context.socket(zmq.REP)
            request_socket.setsockopt(zmq.LINGER, 0)
            request_socket.bind(bind_string)    
            
            # Send a system message to notify we have opened our socket.
            self.action_out_queue.put(system_messages.SystemSocketStateMessage(self._thread_name, True, "Request bound to {0}".format(bind_string)))
            
            # Create the poller objects so we can utilize time outs.
            poller = zmq.Poller()
            poller.register(request_socket, zmq.POLLIN)
            
        except:
            
            # Do not allow to run.
            self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "*****************************")) 
            self.is_running = False
            
        # Enter a loop, waiting for requests.
        while self.is_running == True:
                        
            try:           

                # Poll on receiving with our time out.
                if poller.poll(self.poll_interval):
                       
                    # Always allow the action queue to respond first.
                    self._consume_action_queue()
                    
                    # Receive.
                    raw_message = SocketWrapper.pull_message(request_socket)
                    raw_message_type = BaseMessage.get_raw_message_type(raw_message) 
                    
                    # Surround the handling of the message in another try block.
                    # The handling of messages could cause the thread to crash due to user input error in the command script.
                    # We want to trap this out and return an error message when this happens.
                    try:

                        # Handle commands: forward known types to the main thread.       
                        if raw_message_type == message_types.COMMAND_SHUT_DOWN_REQUEST:
                            
                            # Decode.
                            decoded_message = command_messages.CommandShutDownMessage.create_from_received(raw_message)
                            
                            # If the decoding was successful, handle the message.   
                            if decoded_message != None:
                                self.action_out_queue.put(decoded_message)
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_RECEIVED)
                                
                            # If the decoding failed, notify the main thread and set the reply accordingly.
                            else:
                                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Malformed COMMAND_SHUT_DOWN_REQUEST message received.")) 
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE)         
                            
                        elif raw_message_type == message_types.COMMAND_ADD_WORKERS_REQUEST:
                            
                            # Decode.
                            decoded_message = command_messages.CommandAddWorkersRequestMessage.create_from_received(raw_message)  
                            
                            # If the decoding was successful, handle the message.      
                            if decoded_message != None:
                                self.action_out_queue.put(decoded_message)
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_RECEIVED)
                                
                            # If the decoding failed, notify the main thread and set the reply accordingly.
                            else:
                                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Malformed COMMAND_ADD_WORKERS_REQUEST message received.")) 
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE)  
                            
                        elif raw_message_type == message_types.COMMAND_REMOVE_WORKERS_REQUEST:
                            
                            # Decode.
                            decoded_message = command_messages.CommandRemoveWorkersRequestMessage.create_from_received(raw_message)   
                            
                            # If the decoding was successful, handle the message.   
                            if decoded_message != None:
                                self.action_out_queue.put(decoded_message)
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_RECEIVED)
                                
                            # If the decoding failed, notify the main thread and set the reply accordingly.
                            else:
                                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Malformed COMMAND_REMOVE_WORKERS_REQUEST message received.")) 
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE)  
                                  
                        elif raw_message_type == message_types.COMMAND_PURGE_QUEUES_REQUEST:                
                            
                            # Decode.
                            decoded_message = command_messages.CommandPurgeQueuesRequestMessage.create_from_received(raw_message)
                            
                            # If the decoding was successful, handle the message.      
                            if decoded_message != None:
                                self.action_out_queue.put(decoded_message)
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_RECEIVED)
                                
                            # If the decoding failed, notify the main thread and set the reply accordingly.
                            else:
                                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Malformed COMMAND_PURGE_QUEUE_REQUEST message received.")) 
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE) 
                                  
                        elif raw_message_type == message_types.COMMAND_FREEZE_QUEUE_REQUEST:                
                            
                            # Decode.
                            decoded_message = command_messages.CommandFreezeQueueRequestMessage.create_from_received(raw_message)
                            
                            # If the decoding was successful, handle the message.      
                            if decoded_message != None:
                                self.action_out_queue.put(decoded_message)
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_RECEIVED)
                                
                            # If the decoding failed, notify the main thread and set the reply accordingly.
                            else:
                                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Malformed COMMAND_FREEZE_QUEUE_REQUEST message received.")) 
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE)  
                                  
                        elif raw_message_type == message_types.COMMAND_DELETE_QUEUES_REQUEST:                
                            
                            # Decode.
                            decoded_message = command_messages.CommandDeleteQueuesRequestMessage.create_from_received(raw_message) 
                            
                            # If the decoding was successful, handle the message.     
                            if decoded_message != None:
                                self.action_out_queue.put(decoded_message)
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_RECEIVED)
                                
                            # If the decoding failed, notify the main thread and set the reply accordingly.
                            else:
                                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Malformed COMMAND_DELETE_QUEUES_REQUEST message received.")) 
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE)  
                                  
                        elif raw_message_type == message_types.COMMAND_UNLOCK_QUEUE_REQUEST:                
                            
                            # Decode.
                            decoded_message = command_messages.CommandUnlockQueueRequestMessage.create_from_received(raw_message) 
                            
                            # If the decoding was successful, handle the message.     
                            if decoded_message != None:
                                self.action_out_queue.put(decoded_message)
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_RECEIVED)
                                
                            # If the decoding failed, notify the main thread and set the reply accordingly.
                            else:
                                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Malformed COMMAND_UNLOCK_QUEUE_REQUEST message received.")) 
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE)  
                                          
                        elif raw_message_type == message_types.COMMAND_GET_QUEUE_SIZE_REQUEST:                   
                            
                            # Decode.          
                            decoded_message = command_messages.CommandGetQueueSizeRequestMessage.create_from_received(raw_message)
                            
                            # If the decoding was successful, handle the message.     
                            if decoded_message != None:
                                decoded_message.queue_size = self.shared_memory_manager.get_queue_size(decoded_message.queue_name) 
                                self.action_out_queue.put(decoded_message)
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_RECEIVED, str(decoded_message.queue_size))
                                
                            # If the decoding failed, notify the main thread and set the reply accordingly.
                            else:
                                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Malformed COMMAND_GET_QUEUE_SIZE_REQUEST message received.")) 
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE) 
                                          
                        elif raw_message_type == message_types.COMMAND_GET_STATISTICS_REQUEST:                   
                            
                            # Decode.          
                            decoded_message = command_messages.CommandGetStatisticsRequestMessage.create_from_received(raw_message)
                            
                            # If the decoding was successful, handle the message.     
                            if decoded_message != None:
                                thread_name_dict = self.shared_memory_manager.get_thread_name_dict()
                                net_stat_dict = self.shared_memory_manager.get_network_memory_statistics_dict()
                                reply_message = command_messages.CommandGetStatisticsReplyMessage(command_response_codes.COMMAND_RECEIVED, thread_name_dict, net_stat_dict)
                                self.action_out_queue.put(decoded_message) 
                                
                            # If the decoding failed, notify the main thread and set the reply accordingly.
                            else:
                                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Malformed COMMAND_GET_QUEUE_SIZE_REQUEST message received.")) 
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE) 
                                  
                        elif raw_message_type == message_types.COMMAND_GET_SETUP_DATA_REQUEST:                
                            
                            # Decode.
                            decoded_message = command_messages.CommandGetSetupDataRequestMessage.create_from_received(raw_message) 
                            
                            # If the decoding was successful, handle the message.     
                            if decoded_message != None:
                                
                                # Get the current setup data.
                                exchange_wrapper_list = list(self.shared_memory_manager.get_exchange_wrapper_dict().values())
                                queue_wrapper_list = list(self.shared_memory_manager.get_queue_wrapper_dict().values())
                                
                                # Create the reply; forward the request to the main thread.
                                reply_message = command_messages.CommandGetSetupDataReplyMessage(command_response_codes.COMMAND_RECEIVED, exchange_wrapper_list, queue_wrapper_list)
                                self.action_out_queue.put(decoded_message) 
                                
                            # If the decoding failed, notify the main thread and set the reply accordingly.
                            else:
                                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Malformed COMMAND_GET_SETUP_DATA_REQUEST message received.")) 
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE)                               
                                            
                        elif raw_message_type == message_types.COMMAND_LIST_QUEUES_REQUEST:      
                            
                            # Decode.          
                            decoded_message = command_messages.CommandListQueuesRequestMessage.create_from_received(raw_message)
                            
                            # If the decoding was successful, handle the message.     
                            if decoded_message != None:
                                
                                # Use the correct dictionary based off the all flag in the message.
                                if decoded_message.from_all_servers_flag == True:
                                    if self._full_queue_size_dictionary == None:
                                        self._build_full_queue_size_dictionary()
                                    queue_size_dict = self._full_queue_size_dictionary
                                else:
                                    queue_size_dict = self._qm_queue_size_dictionary
                                    
                                # Trim the return dictionary to only the queue names requested; if all queue names were requested, we have no work to do here.
                                if len(decoded_message.queue_name_list) == 1 and decoded_message.queue_name_list[0] == "":
                                    pass
                                else:
                                    for queue_name in list(queue_size_dict.keys()):
                                        if queue_name not in decoded_message.queue_name_list:
                                            del(queue_size_dict[queue_name])
                                
                                # Get the size information and create the reply.
                                reply_message = command_messages.CommandListQueuesReplyMessage(command_response_codes.COMMAND_RECEIVED, queue_size_dict)
                                
                                # Forward the message to the main thread.
                                self.action_out_queue.put(decoded_message) 
                                
                            # If the decoding failed, notify the main thread and set the reply accordingly.
                            else:
                                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Malformed COMMAND_LIST_QUEUES_REQUEST message received.")) 
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE)    
                              
                        elif raw_message_type == message_types.COMMAND_GET_PECKING_ORDER_REQUEST:    
                            
                            # Decode.          
                            decoded_message = command_messages.CommandGetPeckingOrderRequestMessage.create_from_received(raw_message)
                            
                            # If the decoding was successful, handle the message.     
                            if decoded_message != None:     
                                
                                # Get the pecking order from shared memory and create the reply.
                                pecking_order_list = self.shared_memory_manager.get_pecking_order_list()
                                reply_message = command_messages.CommandGetPeckingOrderReplyMessage(pecking_order_list, "")       
                                
                                # Forward the message to the main thread.
                                self.action_out_queue.put(decoded_message) 
                                                                                                
                            # If the decoding failed, notify the main thread and set the reply accordingly.
                            else:
                                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Malformed COMMAND_GET_PECKING_ORDER_REQUEST message received.")) 
                                reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE)   
                            
                        # Handle unknown.
                        else:
                            
                            # Notify the main thread and set the reply accordingly.
                            self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Unknown message type received: {0}".format(raw_message_type))) 
                            reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_UNKNOWN)
                            
                        # Send a response.
                        reply_message.send(request_socket)
                    
                    except:
                        
                        # On exception while handling, send a reply and notify the main thread.
                        # Continue processing.
                        self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "General exception raised while processing request: " + ExceptionFormatter.get_message()))
                        
                        try:
                            reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_FAILED)
                            reply_message.send(request_socket)
                        except:
                            pass
                        
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
        
        # Clean up the bridge socket.
        if request_socket != None:
            request_socket.setsockopt(zmq.LINGER, 0)
            request_socket.close()
        if poller != None:
            poller.unregister(request_socket)
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

                elif message.get_type() == message_types.SYSTEM_UPDATE_QM_QUEUE_SIZE_DICTIONARY:
                    self._qm_queue_size_dictionary = message.queue_size_dictionary
                    self._full_queue_size_dictionary = None
                    message_handled = True

                elif message.get_type() == message_types.SYSTEM_UPDATE_PQM_QUEUE_SIZE_DICTIONARIES:
                    self._pqm_queue_size_dictionaries = message.queue_size_dictionaries
                    self._full_queue_size_dictionary = None
                    message_handled = True

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
    