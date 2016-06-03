from queue import Empty as EmptyQueueException #@UnresolvedImport
from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.zmq_utilities import ZmqUtilities #@UnresolvedImport
from afqueue.messages import system_messages #@UnresolvedImport
import afqueue.messages.message_types as message_types #@UnresolvedImport
import zmq #@UnresolvedImport
import os


def pqm_outgoing_message_thread(thread_name, router_socket_port, 
                                action_queue, notification_queue):
    """
    This thread is responsible for sending messages to PQMs.
    An outside thread commands this thread via the action queue (send message, shut down, etc).
    Results and notifications are put in the notification queue (when necessary).
    Note that this ROUTER/DEALER connection relationship is modeled after PUSH/PULL for asynchronous data transmission.
    """
    
    # Initialize variables.
    is_running = True
    router_socket = None
    
    try:
        
        # Send a system message to notify we have started our thread.thread.
        notification_queue.put(system_messages.SystemThreadStateMessage(thread_name, True, os.getpid()))   
        
        # Create our ZMQ context.
        zmq_context = zmq.Context(1)  
            
        # Create the router socket.
        connection_string = ZmqUtilities.get_socket_bind_string(router_socket_port)
        router_socket = zmq_context.socket(zmq.ROUTER)
        router_socket.bind(connection_string)
        
        # Send a system message to notify we have opened our socket.
        notification_queue.put(system_messages.SystemSocketStateMessage(thread_name, True, "Router bound on {0}".format(connection_string)))
        
    except:
        
        # Do not allow to run.
        notification_queue.put(system_messages.SystemErrorMessage(thread_name, "*****************************")) 
        is_running = False
    
    # Enter a loop in which we wait for a router request message.
    while is_running == True:
        
        try:
            
            # Get a new message; initialize to not being handled. 
            # Note that we will be routed to our empty message exception if no messages are waiting.
            message = action_queue.get(True)
            message_handled = False
            
            # We should only receive a send data request.
            if message.get_primary_type() == message_types.PEER:
                
                # Heart beat related messages: Send the message as is.
                if message.get_type() == message_types.PEER_HEART_BEAT or message.get_type() == message_types.PEER_HEART_BEAT_FAILURE:
                    
                    # Forward and denote as handled.
                    message.send(router_socket)
                    message_handled = True
                
                if message.get_type() == message_types.PEER_ORDERED_QUEUES_OWNERS_EXHAUSTED or message.get_type() == message_types.PEER_FORWARDED_COMMAND_MESSAGE:
                    
                    # Forward and denote as handled.
                    message.send(router_socket)
                    message_handled = True
                    
                elif message.get_type() == message_types.PEER_MASTER_SETUP_DATA or message.get_type() == message_types.PEER_MASTER_CONTROL_DATA:
                
                    # Forward and denote as handled.
                    message.send(router_socket) 
                    message_handled = True
                    
                elif message.get_type() == message_types.PEER_CLIENT_DECLARE_EXCHANGES_REQUEST or message.get_type() == message_types.PEER_CLIENT_DECLARE_QUEUES_REQUEST:
                
                    # Forward and denote as handled.
                    message.send(router_socket) 
                    message_handled = True
                    
                elif message.get_type() == message_types.PEER_CLIENT_DELETE_QUEUES_REQUEST or message.get_type() == message_types.PEER_REQUEST_MASTER_DATA:
                
                    # Forward and denote as handled.
                    message.send(router_socket) 
                    message_handled = True                  
                    
                elif message.get_type() == message_types.PEER_CLIENT_LOCK_QUEUES_REQUEST or message.get_type() == message_types.PEER_CLIENT_UNLOCK_QUEUES_REQUEST:
                
                    # Forward and denote as handled.
                    message.send(router_socket) 
                    message_handled = True     
                    
                elif message.get_type() == message_types.PEER_OFFLINE:
                
                    # Forward and denote as handled.
                    message.send(router_socket) 
                    message_handled = True       
              
            # Check for a system shut down.      
            elif message.get_primary_type() == message_types.SYSTEM:
                                        
                if message.get_type() == message_types.SYSTEM_STOP_THREAD:
                    notification_queue.put(system_messages.SystemNotificationMessage(thread_name, "Shutting down per main thread request"))
                    message_handled = True
                    is_running = False

            # If the message hasn't been handled, notify.
            if message_handled == False:
                notification_queue.put(system_messages.SystemErrorMessage(thread_name, "Action queue message was not handled: {0}".format(message)))
    
        except EmptyQueueException:
            
            pass
        
        except KeyboardInterrupt:
            
            # Ignore keyboard interrupts; the main thread will handle as desired. 
            pass   
        
        except:
            
            # When we get an exception, log it and break from our loop.
            notification_queue.put(system_messages.SystemErrorMessage(thread_name, "Action queue message processing raised exception: " + ExceptionFormatter.get_message())) 
            break   
                                
    # Clean up the socket.
    if router_socket != None:
        router_socket.setsockopt(zmq.LINGER, 0)
        router_socket.close()
    
    # Send a system message to notify we have closed the socket.
    notification_queue.put(system_messages.SystemSocketStateMessage(thread_name, False, "Router"))
    
    # Send a system message to notify we have shut down.
    notification_queue.put(system_messages.SystemThreadStateMessage(thread_name, False))
    