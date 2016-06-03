from queue import Empty as EmptyQueueException #@UnresolvedImport
from afqueue.source.bridge_worker_manager import BridgeWorkerManager #@UnresolvedImport
from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.zmq_utilities import ZmqUtilities #@UnresolvedImport
from afqueue.common.utility import Utility #@UnresolvedImport
from multiprocessing import Process, Queue #@UnresolvedImport
from afqueue.threads.bridge_load_balancer_thread import bridge_load_balancer_thread #@UnresolvedImport
from afqueue.threads.bridge_command_request_thread import bridge_command_request_thread  #@UnresolvedImport
from afqueue.messages import command_messages, system_messages, message_types #@UnresolvedImport
import time
import zmq #@UnresolvedImport


class NetworkBridge():    
                
    def __init__(self, statsd_library, client_router_port, config, logger):
        
        try:
            
            # Assign.
            self.config = config
            self.logger = logger
            self.statsd_library = statsd_library
            
            # Denote running and not shut down.
            self.is_running = True
            self.shut_down_ran = False
        
            # Create the thread process ID tracker.
            self._thread_process_id_dict = dict() 
               
            # Set up our socket information.
            self._data_client_request_socket_port = client_router_port
            self._command_request_socket_port = client_router_port + 1
            self._bridge_worker_router_socket_port = client_router_port + 2
            
            # Create the thread notification queue.
            # Most threads in the system will use this queue to put all data which the main thread needs to access.
            self._child_thread_notification_queue = Queue()
            self._load_balancer_thread_action_queue = Queue()
            self._command_request_action_queue = Queue()
        
            # When a bridge worker times out, the bridge will automatically try to recreate a new socket connection.
            # The tracker and time stamp dictionaries are used to track which connections to recreate and when we are allowed to create them.
            # The interval at which we check if we should recreate, how many we can create in a period, and the period itself are all configuration values.
            self._worker_recreation_tracker_dict = dict()
            self._worker_recreation_time_stamp_dict = dict()
            self._last_worker_recreation_check_time = 0
            self.config.bridge_worker_recreation_max_per_period = 10
            self.config.bridge_worker_recreation_period = 90
            
            # Create the ZMQ context.
            self._zmq_context = zmq.Context(1)
            
            # Create the command thread; utilized to receive commands from external sources.
            self._bridge_command_thread_process = Process(target=bridge_command_request_thread, 
                                                args=(self.config.bridge_command_request_thread_name, 
                                                      self._zmq_context, None, # We have no shared memory object in the bridge at this point.
                                                      self._command_request_socket_port, self.config.zmq_poll_timeout,
                                                      self._command_request_action_queue, self._child_thread_notification_queue,),
                                                name=self.config.bridge_command_request_thread_name)
            
            # Create the data broker thread (read thread method's description for processing information). 
            self._bridge_broker_process = Process(target=bridge_load_balancer_thread, 
                                                args=(self.config.bridge_load_balancer_thread_name, 
                                                      self._zmq_context, self.config.bridge_poll_timeout,
                                                      self._data_client_request_socket_port, self._bridge_worker_router_socket_port,
                                                      self._load_balancer_thread_action_queue, self._child_thread_notification_queue,),
                                                name=self.config.bridge_load_balancer_thread_name)
                
            # Start our process threads.
            self._bridge_command_thread_process.start()
            self._bridge_broker_process.start()
                        
            # Create the bridge worker manager.
            self._bridge_worker_manager = BridgeWorkerManager(self.config, self._zmq_context, self._bridge_worker_router_socket_port, self._child_thread_notification_queue, self.logger)
                            
            # We should give our threads some time to start up and then handle all notifications from them before proceeding.
            time.sleep(0.5)
            if self._handle_notifications() == False:
                raise Exception("Initial notification handling failed")
            
            # Make initial connections found in the configuration file.
            for connection_type, remote_connection_information, connection_count in self.config.bridge_data_worker_connections:
                
                # Form a connection message, like we would receive from the commander script, from the parameters.
                if connection_type == "zmq":
                    remote_ip, remote_port = remote_connection_information.split(":")
                    message = command_messages.CommandRemoteConnectRequestMessage(remote_ip, remote_port, connection_count)
                elif connection_type == "pika":
                    message = command_messages.CommandRemoteConnectPikaRequestMessage(remote_connection_information, self.config.bridge_data_worker_pika_connection_type,
                                                                                      connection_count)
                else:
                    raise Exception("Unknown connection type specified in initial bridge data worker connections in config file.")
                
                # Put in our notification queue.
                self._child_thread_notification_queue.put(message, False)                
                
        except:
            
            # Ensure we call shut down to destroy any sockets we may have created.
            try:
                self.shut_down()
            except:
                pass
            
            # Raise out.
            raise Exception("Initialization failed: " + ExceptionFormatter.get_message())
        
        
    def run_loop(self):
        
        try:

            # Initialize the successful operation flag.
            success_flag = True
                        
            # Handle notification messages: messages put into the notification queue by the threads within our system.
            if self._handle_notifications() == False:
                success_flag = False
            
            # Handle time based functionality.
            if self._handle_timers(self._get_current_time()) == False:
                success_flag = False
                
            # Return the success flag.
            return success_flag
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
    
    def shut_down(self):
        
        # Notify.
        self.logger.log_info("Shut down method called.")
        
        # Denote no longer running.
        self.is_running = False
        
        # Notify the load balancer thread and each bridge worker thread we are shutting down.
        self._command_request_action_queue.put(system_messages.SystemStopThreadMessage(), False)
        self._load_balancer_thread_action_queue.put(system_messages.SystemStopThreadMessage(), False)
        self._bridge_worker_manager.signal_all_stop()
        
        # Wait for the threads to finish.
        self._bridge_command_thread_process.join()
        self._bridge_broker_process.join()
        self._bridge_worker_manager.verify_all_stopped()
                    
        # Handle remaining messages
        self._handle_notifications()    
    
        # Shut down our main context.
        self._zmq_context.term()
                
        # Notify completion.
        self.logger.log_info("Shut down complete.")
        
        # Set shut down.
        self.shut_down_ran = True
            
        
    def _get_current_time(self):
        """
        Returns the current local time as an integer.
        """
        
        return int(time.time())         
            
        
    def _handle_notifications(self):
        
        # Surround in a try/except block which will raise an unexpected exception out with full information.
        try:
            
            # Initialize the successful operation flag.
            success_flag = True
            
            # Enter a loop which will only break when we receive no more messages from the queue we are handling.
            while True:
                
                try:
                    
                    # Get a new message; initialize it to not handled.
                    message = self._child_thread_notification_queue.get(False)
                    message_handled = False

                    if message.get_primary_type() == message_types.SYSTEM:

                        if message.get_type() == message_types.SYSTEM_THREAD_STATE:
                            message_handled = self._handle_system_thread_state(message)
                            
                        elif message.get_type() == message_types.SYSTEM_SOCKET_STATE:
                            message_handled = self._handle_system_socket_state(message)
                            
                        elif message.get_type() == message_types.SYSTEM_NOTIFICATION_MESSAGE:
                            message_handled = self._handle_system_notification_message(message)
                            
                        elif message.get_type() == message_types.SYSTEM_ERROR_MESSAGE:
                            message_handled = self._handle_system_error_message(message)
                            
                        elif message.get_type() == message_types.SYSTEM_STATS_MESSAGE:
                            message_handled = self._handle_system_stats(message)
                            
                        elif message.get_type() == message_types.SYSTEM_BRIDGE_WORKER_TIMED_OUT:
                            message_handled = self._handle_system_bridge_worker_timed_out(message)
                                                    
                    elif message.get_primary_type() == message_types.COMMAND:
                         
                        if message.get_type() == message_types.COMMAND_SHUT_DOWN_REQUEST:
                            message_handled = self._handle_command_shut_down_request(message)

                        elif message.get_type() == message_types.COMMAND_REMOTE_CONNECT_REQUEST:
                            message_handled = self._handle_command_remote_connect_request(message)
                            
                        elif message.get_type() == message_types.COMMAND_REMOTE_DISCONNECT_REQUEST:
                            message_handled = self._handle_command_remote_disconnect_request(message)

                        elif message.get_type() == message_types.COMMAND_REMOTE_CONNECT_PIKA_REQUEST:
                            message_handled = self._handle_command_remote_connect_pika_request(message)
                            
                        elif message.get_type() == message_types.COMMAND_REMOTE_DISCONNECT_PIKA_REQUEST:
                            message_handled = self._handle_command_remote_disconnect_pika_request(message)
                                                        
                    # If the message hasn't been handled, notify.
                    if message_handled == False:
                        self.logger.log_error("Thread notification handler found a message it could not process: {0}".format(message))
                
                except EmptyQueueException:
                    
                    # Break from the loop; there are no more messages to handle.
                    break
                    
                except IOError:
                    
                    self._log_exception(note = "IO Error while handling message: {0}".format(str(message)))
                    success_flag = False
                
                except:
                    
                    raise ExceptionFormatter.get_full_exception()
            
            # Return success flag.
            return success_flag
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
            
        
    def _handle_command_shut_down_request(self, message):
        """
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            # Log.
            self.logger.log_info("Command received: Shut down.")

            # Respond to the shut down call.  
            self.shut_down()
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()              
                
        
    def _handle_command_remote_connect_request(self, message):
        """
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            # Notify.
            notification_string = "Command received: Remote connect.  IP: {0}; Port: {1}; Count: {2}".format(message.remote_ip_address, message.remote_port, message.count)
            self.logger.log_info(notification_string)
        
            # Get the connection string and create and register a new bridge worker.
            connection_string = ZmqUtilities.get_socket_connection_string(message.remote_ip_address, message.remote_port)
            
            # Create <count> new bridge workers with the connection string.
            for _ in range(message.count):
                
                # Create and add a new worker with the connection info. 
                bridge_worker = self._bridge_worker_manager.create_and_add_worker(connection_string)
                bridge_worker.process.start()
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()                   
                
        
    def _handle_command_remote_connect_pika_request(self, message):
        """
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            # Notify.
            notification_string = "Command received: PIKA Remote connect.  Connection string: {0}; Mode: {1}; Count: {2}".format(message.connection_string, message.queue_mode, message.count)
            self.logger.log_info(notification_string)
            
            # Create <count> new bridge workers with the connection string.
            for _ in range(message.count):
            
                # Create, register, and start the worker.
                bridge_worker = self._bridge_worker_manager.create_and_add_pika_worker(message.connection_string, message.queue_mode)
                bridge_worker.process.start()

            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()          
                
        
    def _handle_command_remote_disconnect_request(self, message):
        """
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            # Notify.
            notification_string = "Command received: Remote disconnect.  IP: {0}; Port: {1}; Count: {2}".format(message.remote_ip_address, message.remote_port, message.count)
            if message.notification != "":
                notification_string += "; Note: " + message.notification
            self.logger.log_info(notification_string)
            
            # Get the connection string from our remote data and stop/remove <count> workers.
            connection_string = ZmqUtilities.get_socket_connection_string(message.remote_ip_address, message.remote_port)
            
            # Clear out connections from our tracker first - these are connections we are actively trying to recreate.
            if connection_string in list(self._worker_recreation_tracker_dict.keys()):
                
                # Get the number of recreations desired according to the tracker.
                desired_recreation_count = self._worker_recreation_tracker_dict[connection_string]
                
                # Get the number of recreations we will actually want to do, based on the given message.
                to_recreate_count = desired_recreation_count - message.count
                
                # If the message yielded more recreations than we desired, update the message count with the number remaining.
                if to_recreate_count < 0:
                    message.count = 0 - to_recreate_count
                    to_recreate_count = desired_recreation_count
                
                # If the message couldn't satisfy all desired recreations, set the the remaining to zero.
                else:
                    message.count = 0

                # If we have a new count, log and update. 
                if to_recreate_count > 0:
                    self.logger.log_info("Removed {0} connections from the desired recreation tracker in response to disconnect request.".format(to_recreate_count))
                    self._worker_recreation_tracker_dict[connection_string] = desired_recreation_count - to_recreate_count 
                                
            # Clear out the remaining count from our actual live connections.
            self._bridge_worker_manager.stop_and_remove_bridge_workers_by_connection_string(connection_string, message.count)

            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()            
                
        
    def _handle_command_remote_disconnect_pika_request(self, message):
        """
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            # Notify.
            notification_string = "Command received: PIKA Remote disconnect.  Connection string: {0}; Count: {1}".format(message.connection_string, message.count)
            if message.notification != "":
                notification_string += "; Note: " + message.notification
            self.logger.log_info(notification_string)
            
            # Clear out connections from our tracker first - these are connections we are actively trying to recreate.
            if message.connection_string in list(self._worker_recreation_tracker_dict.keys()):
                
                # Get the number of recreations desired according to the tracker.
                desired_recreation_count = self._worker_recreation_tracker_dict[message.connection_string]
                
                # Get the number of recreations we will actually want to do, based on the given message.
                to_recreate_count = desired_recreation_count - message.count
                
                # If the message yielded more recreations than we desired, update the message count with the number remaining.
                if to_recreate_count < 0:
                    message.count = 0 - to_recreate_count
                    to_recreate_count = desired_recreation_count
                
                # If the message couldn't satisfy all desired recreations, set the the remaining to zero.
                else:
                    message.count = 0

                # Update our tracker; this effectively removes the number of connections in the to recreate count.
                self._worker_recreation_tracker_dict[message.connection_string] = desired_recreation_count - to_recreate_count  
            
            # Stop/remove <count> workers.
            self._bridge_worker_manager.stop_and_remove_bridge_workers_by_connection_string(message.connection_string, message.count)
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()      
    
        
    def _handle_system_bridge_worker_timed_out(self, message):
        """
        """
        
        try:
            
            # Log.
            self.logger.log_error("Received bridge worker timed out: {0}.  Stopping and removing bridge worker thread from tracking.".format(message.thread_name))
            
            # Kill.
            bridge_worker = self._bridge_worker_manager.stop_and_remove_bridge_worker_by_thread_name(message.thread_name)
            
            # If no bridge worker was returned, denote.
            if bridge_worker == None:
                self.logger.log_error("No bridge worker found for given thread name: {0}.".format(message.thread_name))
            
            # If a thread worker was found, handle the removed bridge worker due to the time out.
            else:
                
                # Register the bridge worker's connection string in our recreation tracker.
                if bridge_worker.connection_string not in list(self._worker_recreation_tracker_dict.keys()):
                    self._worker_recreation_tracker_dict[bridge_worker.connection_string] = 0
                    self._worker_recreation_time_stamp_dict[bridge_worker.connection_string] = list()
                self._worker_recreation_tracker_dict[bridge_worker.connection_string] = self._worker_recreation_tracker_dict[bridge_worker.connection_string] + 1 
                
            # Return that we successfully handled the message.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()     
                         
                         
    def _handle_system_error_message(self, message):
        """
        Handles a SystemErrorMessage.
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            # Log.
            process_id = self._thread_process_id_dict[message.thread_name]
            self.logger.log_error("{0} - {1}: Thread ERROR: {2}".format(process_id, message.thread_name, message.message))
            
            # Return that we successfully handled the message.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()                     
        
        
    def _handle_system_notification_message(self, message):
        """
        Handles a SystemNotificationMessage.
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            # Log.
            process_id = self._thread_process_id_dict[message.thread_name]
            self.logger.log_info("{0} - {1}: Thread notification: {2}".format(process_id, message.thread_name, message.message))      

            # Return that we successfully handled the message.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()                   
                     
        
    def _handle_system_socket_state(self, message):
        """
        Handles a SystemSocketStateMessage.
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            # Log.
            if message.opened_flag == True:
                
                self.logger.log_info("{0}: Socket opened: {1}".format(message.thread_name, message.notification_string))
                
            else:
                
                self.logger.log_info("{0}: Socket closed: {1}".format(message.thread_name, message.notification_string))
         
            # Return that we successfully handled the message.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()     
        
        
    def _handle_system_stats(self, message):
        """
        Handles a SystemStatsMessage.
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
         
            # Update StatsD.
            if message.stat_type == "call_count":
                Utility.send_stats_count(self.statsd_library, self.config.STATSD_NETWORK_BRIDGE_WORKER_CALL_COUNT, message.stat_value)
            elif message.stat_type == "art":
                Utility.send_stats_gauge(self.statsd_library, self.config.STATSD_NETWORK_BRIDGE_WORKER_AVERAGE_RESPONSE_TIME, message.stat_value)
               
            # Log.
            self.logger.log_info("{0}: Statistics update.  Type: {1}; Value: {2}.".format(message.thread_name, message.stat_type, message.stat_value))
                    
            # Return that we successfully handled the message.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()        
        
        
    def _handle_system_thread_state(self, message):
        """
        Handles a SystemThreadState message.
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            # Log; check running state if the message is telling us the thread is no longer running.
            if message.started_flag == True:
                
                self.logger.log_info("{0} - {1}: Thread started.".format(message.process_id, message.thread_name))
                self._thread_process_id_dict[message.thread_name] = message.process_id
                
            else:
                
                self.logger.log_info("{0}: Thread stopped".format(message.thread_name))
                
                # If this thread is still running, we might have to shut down.
                if self.is_running == True:
                    
                    # If this is a worker thread.
                    if self._bridge_worker_manager.check_master_thread_list(message.thread_name):
                                                    
                        # If the name of the thread which has stopped resolves to a valid bridge worker, 
                        #  it means the bridge worker thread has stopped before the tracker was removed.
                        # Check to see if the bridge worker is in a shutdown state; shut down the entire server it this was unexpected.
                        bridge_worker = self._bridge_worker_manager.get_bridge_worker_from_thread_name(message.thread_name)
                        if bridge_worker != None:
                            if bridge_worker.should_shutdown == False:
                                self.logger.log_error("{0}: Thread resolved to a bridge worker which has been shut down unexpectedly; shutting down server.".format(message.thread_name))
                                self.shut_down()
                                
                        # Regardless, we must notify the broker that the worker is stopped.
                        self._load_balancer_thread_action_queue.put(message)
                            
                    # Any other thread type should result in a shut down.
                    #else:
                    #    self.shut_down()
        
            # Return that we successfully handled the message.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
    
    def _handle_timers(self, current_time_stamp):
        
        # Surround in a try/except block which will raise an unexpected exception out with full information.
        try:
            
            # Initialize the successful operation flag.
            success_flag = True
            
            # If it is time to recreate bridge workers, run the code.
            if current_time_stamp > self._last_worker_recreation_check_time + self.config.bridge_check_worker_recreation_interval:
                self._last_worker_recreation_check_time = current_time_stamp
                success_flag = self._recreate_bridge_workers(current_time_stamp)
            
            # Return.
            return success_flag
            
        except:
            
            raise ExceptionFormatter.get_full_exception()   


    def _log_exception(self, as_incident = True, note = "", increment_statsd = True):
        
        # Log the exception.
        self.logger.log_error("::: *** EXCEPTION: {0}".format(ExceptionFormatter.get_message()), as_incident)
        if note != "":
            self.logger.log_error("::: *** EXCEPTION - Note: {0}".format(note))
        
        # Send to the exception bucket.
        if increment_statsd == True:
            Utility.send_stats_count(self.statsd_library, self.config.STATSD_NETWORK_BRIDGE_EXCEPTION_COUNT, 1)       
        
        
    def _recreate_bridge_workers(self, current_time_stamp):
        """
        Runs the connection recreation code.
        Attempts to recreate bridge workers for previously disconnected bridge workers due to timeouts.
        Will maintain recreation settings within configuration parameters.
        """
        
        try:

            # Go through the worker recreation tracker to see if any workers need recreation.
            for connection_string, desired_recreation_count in list(self._worker_recreation_tracker_dict.items()):
                
                # If there are workers needing recreation, 
                if desired_recreation_count > 0:
                    
                    # Get the previously recreated time stamp list.
                    previous_recreated_time_stamp_list = self._worker_recreation_time_stamp_dict[connection_string]
                    
                    # We can only recreate so many bridge workers in a certain period.  
                    # Clean up our recreated timer list, up to our period, so we know how many we can create.
                    for time_stamp in previous_recreated_time_stamp_list:
                        if current_time_stamp - time_stamp > self.config.bridge_worker_recreation_period:
                            previous_recreated_time_stamp_list.remove(time_stamp)
                        else:
                            break
                        
                    # Determine how many workers we can recreate on this connection.
                    allowed_recreation_count = self.config.bridge_worker_recreation_max_per_period - len(previous_recreated_time_stamp_list)
                    
                    # Attempt to create our desired count; cap by the allowed amount if necessary.
                    creation_count = desired_recreation_count
                    if allowed_recreation_count < creation_count:
                        creation_count = allowed_recreation_count
                        
                    # Create new workers, registering the time stamp.
                    for _ in range(creation_count):
                        bridge_worker = self._bridge_worker_manager.create_and_add_worker(connection_string)
                        bridge_worker.process.start()
                        previous_recreated_time_stamp_list.append(current_time_stamp)
                        desired_recreation_count -= 1
                        
                    # Update the recreation count.
                    self._worker_recreation_tracker_dict[connection_string] = desired_recreation_count
            
        except:
            
            raise ExceptionFormatter.get_full_exception()    
