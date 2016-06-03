from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.socket_wrapper import SocketWrapper #@UnresolvedImport
from afqueue.common.zmq_utilities import ZmqUtilities #@UnresolvedImport
from afqueue.messages import system_messages #@UnresolvedImport
import time, os
import zmq #@UnresolvedImport
from queue import Empty as EmptyQueueException #@UnresolvedImport
import afqueue.messages.message_types as message_types #@UnresolvedImport
from multiprocessing import Process #@UnresolvedImport


class DataBrokerThread:
    """
    The sole purpose of this thread is to allow multiple workers to work on incoming requests in parallel.
    Workers create a REQUEST socket connection to the worker ROUTER socket.
    Clients create a REQUEST socket connection to the client ROUTER socket.
    Workers send a message to denote they are ready for work; the broker responds with requests it receives from clients.
    ID Tags are tracked throughout the process to ensure data flows properly.
    """
        
    def __init__(self, thread_name, config, poll_timeout, client_router_port, worker_router_port, action_queue, notification_queue):

        # Assign.
        self.thread_name = thread_name
        self.config = config
        self.poll_timeout = poll_timeout
        self.client_router_port = client_router_port
        self.worker_router_port = worker_router_port
        self.action_in_queue = action_queue
        self.action_out_queue = notification_queue
        
        # Denote running.
        self.is_running = True
        
        # Create space for our shared memory manager; we will create this when we enter our run loop.
        self.shared_memory_manager = None
        
        # Create the process object.
        self._process = Process(target=self._run_logic, name=thread_name)   

        
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
                   
    
    def _run_logic(self):
                
        # Create our context.
        self.zmq_context = zmq.Context(1)
                
        self.is_running = True
        client_router_socket = None
        worker_router_socket = None
        poller = None
        
        try:
        
            # Send a system message to notify we have started our thread.
            self.action_out_queue.put(system_messages.SystemThreadStateMessage(self.thread_name, True, os.getpid()))        
            
            # Create the client ROUTER socket.
            connection_string = ZmqUtilities.get_socket_bind_string(self.client_router_port)
            client_router_socket = self.zmq_context.socket(zmq.ROUTER)
            client_router_socket.bind(connection_string)
            
            # Send a system message to notify we have opened our socket.
            self.action_out_queue.put(system_messages.SystemSocketStateMessage(self.thread_name, True, "Client router bound on {0}".format(connection_string)))
            
            # Create the worker ROUTER socket.
            connection_string = ZmqUtilities.get_socket_bind_string(self.worker_router_port)
            worker_router_socket = self.zmq_context.socket(zmq.ROUTER)
            worker_router_socket.bind(connection_string)
            
            # Send a system message to notify we have opened our socket.
            self.action_out_queue.put(system_messages.SystemSocketStateMessage(self.thread_name, True, "Worker router bound on {0}".format(connection_string)))
        
            # Use a poller so we can receive with timeouts (receive is blocking; we can poll with a timeout).
            # This will allow us to see if we should shut down the socket in between polls.
            poller = zmq.Poller()
            poller.register(client_router_socket, zmq.POLLIN)
            poller.register(worker_router_socket, zmq.POLLIN)
        
            # List of available workers.
            available_worker_id_tag_list = []
            idle_worker_id_tag_time_dict = dict()
            
        except:
            
            # Do not allow to run.
            self.action_out_queue.put(system_messages.SystemErrorMessage(self.thread_name, "*****************************")) 
            self.is_running = False
                    
        #
        handled_count = 0
        idle_time = 0
        loop_time = time.time()
        while self.is_running == True:
            
            try:
                        
                # Check if we should log metrics.
                current_time = time.time()
                passed_time = current_time - loop_time
                if passed_time > self.config.manager_data_broker_thread_metric_interval:
                    
                    # Gather and log our metrics.
                    worker_threads = len(idle_worker_id_tag_time_dict)
                    if handled_count > 0 and worker_threads > 0:
                        average_idle_time = idle_time / handled_count * 1000
                        average_idle_time = (idle_time / passed_time) / len(idle_worker_id_tag_time_dict)
                    else:
                        idle_time = passed_time
                        average_idle_time = 1
                    metric_notification = "Handled {0} requests in {1:.3f} seconds across {2} threads.  Idle time: {3:.3f}.  Average idle time: {4:.3f}.".format(handled_count, passed_time, worker_threads, 
                                                                                                                                                         idle_time, average_idle_time)
                    self.action_out_queue.put(system_messages.SystemNotificationMessage(self.thread_name, metric_notification))
                    
                    # Clear the old dictionary.
                    idle_worker_id_tag_time_dict.clear()
                    
                    # Reset our metric trackers.
                    loop_time = current_time
                    handled_count = 0
                    idle_time = 0
                
                # Poll.
                socket_events_dictionary = dict(poller.poll(self.poll_timeout))
                
                # Track if we should sleep due to no traffic.
                sleep_flag = True
    
                # Handle a message on the worker ROUTER socket.
                if socket_events_dictionary.get(worker_router_socket) == zmq.POLLIN:
                
                    # Set no sleep.
                    sleep_flag = False
        
                    # Receive.
                    full_raw_message = SocketWrapper.pull_message(worker_router_socket)

                    # Strip off the worker ID tag.
                    worker_id_tag = full_raw_message[0]
        
                    # Add the address of the worker to our available worker list.
                    idle_worker_id_tag_time_dict[worker_id_tag] = time.time()
                    
                    # Strip off the directive.
                    directive = full_raw_message[2]
                    
                    # If the directive is READY, we have nothing more to do: the worker was added to our available worker list already.
                    # If the directive is the client reply address, forward to the client.
                    if directive != "READY":
        
                        # Get the raw message from the full raw message.    
                        raw_message = full_raw_message[4:]
                        
                        # Forward, with the directive (which is the client reply id tag).


                        client_router_socket.send(directive, zmq.SNDMORE)
                        client_router_socket.send(b"", zmq.SNDMORE)
                        for data in raw_message[:-1]:
                            ZmqUtilities.send_data(client_router_socket, data, zmq.SNDMORE)
                        ZmqUtilities.send_data(client_router_socket, raw_message[-1])
                        
                    # XXX
                    available_worker_id_tag_list.append(worker_id_tag)
                        
                # Only poll for incoming requests if there are workers available to take the them.
                if len(available_worker_id_tag_list) > 0:
                    
                    if socket_events_dictionary.get(client_router_socket) == zmq.POLLIN:
                    
                        # Set no sleep.
                        sleep_flag = False
                        
                        # Get the full raw message and break it into separate components.
                        full_raw_message = SocketWrapper.pull_message(client_router_socket)
                        client_id_tag = full_raw_message[0]
                        raw_message = full_raw_message[2:]
                        
                        # If there's raw data.
                        if len(raw_message) > 0:
                            
                            # Pop off the oldest worker marked as available.
                            worker_id_tag = available_worker_id_tag_list.pop(0)
                            
                            # Capture the oldest worker was added, compare it to our current time, and track.
                            current_time = time.time()
                            added_time = idle_worker_id_tag_time_dict.get(worker_id_tag, current_time)
                            handled_count += 1
                            idle_time += current_time - added_time
            
                            # Forward the request to the worker.


                            worker_router_socket.send(worker_id_tag.encode(), zmq.SNDMORE)
                            worker_router_socket.send(b"", zmq.SNDMORE)
                            worker_router_socket.send(client_id_tag, zmq.SNDMORE)
                            worker_router_socket.send(b"", zmq.SNDMORE)
                            for data in raw_message[:-1]:
                                ZmqUtilities.send_data(worker_router_socket, data, zmq.SNDMORE)
                            ZmqUtilities.send_data(worker_router_socket, raw_message[-1])
                         
                        # If there's no raw data.
                        else:

                            # Notify.
                            self.action_out_queue.put(system_messages.SystemErrorMessage(self.thread_name, "Incoming message has no fields.  Sending empty message to sender."))
    
                            # Send a reply back to the sender with no data. 
                            client_router_socket.send(client_id_tag, zmq.SNDMORE)
                            client_router_socket.send(b"", zmq.SNDMORE)
                            client_router_socket.send(b"")
                            
                # Check the sleep flag for this iteration.
                if sleep_flag == True:
                    
                    # We sleep when no traffic came in or out of the broker.
                    time.sleep(0.1)
            
            except KeyboardInterrupt:
                
                # Ignore keyboard interrupts; the main thread will handle as desired. 
                pass   
                    
            except zmq.ZMQError:
                
                # When we get an exception, log it and break from our loop.
                self.action_out_queue.put(system_messages.SystemErrorMessage(self.thread_name, "ZMQ error raised while processing request: " + ExceptionFormatter.get_message())) 
                break
    
            except:
                
                # When we get an exception, log it and break from our loop.
                self.action_out_queue.put(system_messages.SystemErrorMessage(self.thread_name, "General exception raised while processing request: " + ExceptionFormatter.get_message())) 
                break
                       
            # Check our action queue.
            self._consume_action_queue(available_worker_id_tag_list)
        
        # Clean up the sockets.                
        if client_router_socket != None:
            client_router_socket.setsockopt(zmq.LINGER, 0)
            client_router_socket.close()
        if worker_router_socket != None:
            worker_router_socket.setsockopt(zmq.LINGER, 0)
            worker_router_socket.close()
        if poller != None:
            poller.unregister(client_router_socket)
            poller.unregister(worker_router_socket)
        
        # Send a system message to notify we have closed the socket.
        self.action_out_queue.put(system_messages.SystemSocketStateMessage(self.thread_name, False, "Router"))
        self.action_out_queue.put(system_messages.SystemSocketStateMessage(self.thread_name, False, "Dealer"))
        
        # Send a system message to notify we have shut down.
        self.action_out_queue.put(system_messages.SystemThreadStateMessage(self.thread_name, False))
                
                
    def _consume_action_queue(self, available_worker_id_tag_list):
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
                    self.action_out_queue.put(system_messages.SystemNotificationMessage(self.thread_name, "Shutting down per main thread request"))
                    message_handled = True
                    self.is_running = False
    
                elif message.get_type() == message_types.SYSTEM_THREAD_STATE:
                    if message.started_flag == False:
                        if message.thread_name in available_worker_id_tag_list:
                            available_worker_id_tag_list.remove(message.thread_name)
                            self.action_out_queue.put(system_messages.SystemNotificationMessage(self.thread_name, "Worker thread stopped notification from main thread resulted in worker removal from active list: {0}".format(message.thread_name)))
                        else:
                            self.action_out_queue.put(system_messages.SystemNotificationMessage(self.thread_name, "Worker thread stopped notification from main thread resulted in worker removal: {0}".format(message.thread_name)))
                        message_handled = True
                    
                # If the message hasn't been handled, notify.
                if message_handled == False:
                    self.action_out_queue.put(system_messages.SystemErrorMessage(self.thread_name, "Action queue message was not handled: {0}".format(message), self.thread_name))
        
            except EmptyQueueException:
                
                # We are looping as we read from our queue object without waiting; as soon as we hit an empty message, we should break from the loop.
                break
            
            except KeyboardInterrupt:
                
                # Ignore keyboard interrupts; the main thread will handle as desired. 
                pass   
            
            except:
                
                # When we get an exception, log it and break from our loop.
                self.action_out_queue.put(system_messages.SystemErrorMessage(self._thread_name, "Action queue message processing raised exception: " + ExceptionFormatter.get_message()))
                self.is_running = False
                break   
        
        # Return the number of messages we saw.
        return message_count
    
