from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.socket_wrapper import SocketWrapper #@UnresolvedImport
from afqueue.common.zmq_utilities import ZmqUtilities #@UnresolvedImport
from afqueue.messages import system_messages #@UnresolvedImport
import time, os
import zmq #@UnresolvedImport
#from afqueue.common_thread_methods.consume_action_queue import consume_action_queue #@UnresolvedImport
from queue import Empty as EmptyQueueException
import afqueue.messages.message_types as message_types #@UnresolvedImport


def bridge_load_balancer_thread(thread_name, zmq_context, poll_timeout, client_router_port, worker_router_port, action_queue, notification_out_queue):
    """
    The sole purpose of this thread is to allow multiple workers to work on incoming requests in parallel.
    Workers create a REQUEST socket connection to the worker ROUTER socket.
    Clients create a REQUEST socket connection to the client ROUTER socket.
    Workers send a message to denote they are ready for work; the broker responds with requests it receives from clients.
    ID Tags are tracked throughout the process to ensure data flows properly.
    """
    
    # Send a system message to notify we have started our thread.
    notification_out_queue.put(system_messages.SystemThreadStateMessage(thread_name, True, os.getpid()))        

    # Create the client ROUTER socket.
    connection_string = ZmqUtilities.get_socket_bind_string(client_router_port)
    client_router_socket = zmq_context.socket(zmq.ROUTER)
    client_router_socket.bind(connection_string)
    
    # Send a system message to notify we have opened our socket.
    notification_out_queue.put(system_messages.SystemSocketStateMessage(thread_name, True, "Client router bound on {0}".format(connection_string)))
    
    # Create the worker ROUTER socket.
    connection_string = ZmqUtilities.get_socket_bind_string(worker_router_port)
    worker_router_socket = zmq_context.socket(zmq.ROUTER)
    worker_router_socket.bind(connection_string)
    
    # Send a system message to notify we have opened our socket.
    notification_out_queue.put(system_messages.SystemSocketStateMessage(thread_name, True, "Worker router bound on {0}".format(connection_string)))

    # Use a poller so we can receive with timeouts (receive is blocking; we can poll with a timeout).
    # This will allow us to see if we should shut down the socket in between polls.
    poller = zmq.Poller()
    poller.register(client_router_socket, zmq.POLLIN)
    poller.register(worker_router_socket, zmq.POLLIN)

    # List of available workers.
    available_worker_id_tag_list = []

    is_running = True
    while is_running:
        
        try: 
                        
            # Poll.
            socket_events_dictionary = dict(poller.poll(poll_timeout))
            
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
                
                # Strip off the directive.
                directive = full_raw_message[2]
                
                # When worker threads start up, they send a ready command.
                # If this is not ready, we have a real message.

                if directive != "READY":
                    
                    # Get the raw message from the full raw message.    
                    raw_message = full_raw_message[4:]
                    
                    # If the raw message is just a TIMEOUT notification:
                    if raw_message[0] == "TIMEOUT":                        
                        
                        # Notify the main thread of this issue.
                        # Note that by not putting this worker back into the available worker list, it will not receive any more messages.
                        notification_out_queue.put(system_messages.SystemErrorMessage(thread_name, "Time out received from worker thread: " + worker_id_tag)) 
                    
                    # If the raw message is not a TIMEOUT notification, we have to deal with the message.
                    else: 
                        
                        # If the directive is not timeout, add the worker to the available list.
                        available_worker_id_tag_list.append(worker_id_tag)
                        
                        # Forward, with the directive (which is the client reply id tag).
                        ZmqUtilities.send_data(client_router_socket, directive, zmq.SNDMORE)
                        client_router_socket.send(b"", zmq.SNDMORE)
                        for data in raw_message[:-1]:
                            ZmqUtilities.send_data(client_router_socket, data, zmq.SNDMORE)
                        ZmqUtilities.send_data(client_router_socket, raw_message[-1])
                        
                else:
                    
                    # If the directive is ready, add the worker to the available list.
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
    
                    # Pop off the oldest worker marked as available.
                    worker_id_tag = available_worker_id_tag_list.pop(0)
                    
                    # Forward the request to the worker.
                    ZmqUtilities.send_data(worker_router_socket, worker_id_tag, zmq.SNDMORE)
                    worker_router_socket.send(b"", zmq.SNDMORE)
                    ZmqUtilities.send_data(worker_router_socket, client_id_tag, zmq.SNDMORE)
                    worker_router_socket.send(b"", zmq.SNDMORE)
                    for data in raw_message[:-1]:
                        ZmqUtilities.send_data(worker_router_socket, data, zmq.SNDMORE)
                    ZmqUtilities.send_data(worker_router_socket, raw_message[-1])
                     
            # Check the sleep flag for this iteration.
            if sleep_flag == True:
                
                # We sleep when no traffic came in or out of the broker.
                time.sleep(0.1)
                            
        except KeyboardInterrupt:            
            
            # Ignore keyboard interrupts; the main thread will handle as desired. 
            pass
                
        except zmq.ZMQError:
            
            # When we get an exception, log it and break from our loop.
            notification_out_queue.put(system_messages.SystemErrorMessage(thread_name, "ZMQ error raised while processing request: " + ExceptionFormatter.get_message())) 
            break

        except:
            
            # When we get an exception, log it and break from our loop.
            notification_out_queue.put(system_messages.SystemErrorMessage(thread_name, "General exception raised while processing request: " + ExceptionFormatter.get_message())) 
            break
                
        # Respond to requests put this thread's action queue.
        action_queue_exception = False
        while True:
               
            try:
                             
                # Get a new message; initialize to not being handled. 
                # Note that we will be routed to our empty message exception if no messages are waiting.
                message = action_queue.get(False)
                message_handled = False
    
                # Based on the message type, handle.
                if message.get_type() == message_types.SYSTEM_STOP_THREAD:
                    notification_out_queue.put(system_messages.SystemNotificationMessage(thread_name, "Shutting down per main thread request"))
                    message_handled = True
                    is_running = False
    
                elif message.get_type() == message_types.SYSTEM_THREAD_STATE:
                    if message.started_flag == False:
                        if message.thread_name in available_worker_id_tag_list:
                            available_worker_id_tag_list.remove(message.thread_name)
                            notification_out_queue.put(system_messages.SystemNotificationMessage(thread_name, "Worker thread stopped notification from main thread resulted in worker removal from active list: {0}".format(message.thread_name)))
                        else:
                            notification_out_queue.put(system_messages.SystemNotificationMessage(thread_name, "Worker thread stopped notification from main thread resulted in worker removal: {0}".format(message.thread_name)))
                        message_handled = True
                    
                # If the message hasn't been handled, notify.
                if message_handled == False:
                    notification_out_queue.put(system_messages.SystemErrorMessage(thread_name, "Action queue message was not handled: {0}".format(message), thread_name))
        
            except EmptyQueueException:
                
                # We are looping as we read from our queue object without waiting; as soon as we hit an empty message, we should break from the loop.
                break
            
            except:
                
                # When we get an exception, log it and break from our loop.
                notification_out_queue.put(system_messages.SystemErrorMessage(thread_name, "Action queue message processing raised exception: " + ExceptionFormatter.get_message()))
                action_queue_exception = True
                break   
        
        # If we had an action queue exception, we must exit out of our thread loop.
        if action_queue_exception == True:
            break
        
    # Clean up the socket.
    client_router_socket.setsockopt(zmq.LINGER, 0)
    client_router_socket.close()
    worker_router_socket.setsockopt(zmq.LINGER, 0)
    worker_router_socket.close()
    poller.unregister(client_router_socket)
    poller.unregister(worker_router_socket)
    
    # Send a system message to notify we have closed the socket.
    notification_out_queue.put(system_messages.SystemSocketStateMessage(thread_name, False, "ClientRouter"))
    notification_out_queue.put(system_messages.SystemSocketStateMessage(thread_name, False, "WorkerRouter"))
    
    # Send a system message to notify we have shut down.
    notification_out_queue.put(system_messages.SystemThreadStateMessage(thread_name, False))