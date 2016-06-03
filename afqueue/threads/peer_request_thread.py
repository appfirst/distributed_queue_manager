from queue import Empty as EmptyQueueException
from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.messages import system_messages #@UnresolvedImport
import afqueue.messages.message_types as message_types #@UnresolvedImport
import zmq #@UnresolvedImport
import os
    

def peer_request_thread(thread_name, shared_dict, action_queue, notification_queue):
    """
    This thread is responsible for ensuring the blocking nature of requests made to peers via the REQ/REP socket pattern does not block our main thread.
    This thread is instructed to create/destroy separate REQ sockets for each known peer via its action queue.
    This thread is instructed to send data to a peer via its action queue.
    Results and notifications are put in the notification queue (when necessary).  
    """
    
    # Create our ZMQ context.
    zmq_context = zmq.Context(1)
    
    # Use a poller so we can receive with timeouts (receive is blocking; we can poll with a timeout).
    # This will allow us to see if we should shut down the socket in between polls.
    # This will also allow us to check our request queue to add more connections to the poller.
    poller = zmq.Poller()

    # We will need to track our current dealer sockets and which peer queue manager they map to.
    peer_id_string_to_request_socket_map = dict()
    
    # Define a socket connection method.
    def _connect_socket(peer_queue_manager_id_string, connection_string):
        
        # If there is a socket for the peer given already in the map, disconnect.
        if peer_queue_manager_id_string in list(peer_id_string_to_request_socket_map.keys()):
            _disconnect_socket(peer_queue_manager_id_string) 
            
        # Connect.
        request_socket = zmq_context.socket(zmq.REQ)
        request_socket.connect(connection_string)
        peer_id_string_to_request_socket_map[peer_queue_manager_id_string] = request_socket
        poller.register(request_socket, zmq.POLLIN)
        
        # Send a system message to notify we have closed the socket.
        notification_queue.put(system_messages.SystemSocketStateMessage(thread_name, True, "Request: {0}".format(peer_queue_manager_id_string)))
            
    # Define a socket disconnection method.
    def _disconnect_socket(peer_queue_manager_id_string):
        
        # If there is a socket for the peer given in the map, disconnect.
        if peer_queue_manager_id_string in list(peer_id_string_to_request_socket_map.keys()):
            
            # Disconnect
            request_socket = peer_id_string_to_request_socket_map[peer_queue_manager_id_string]
            request_socket.setsockopt(zmq.LINGER, 0)
            request_socket.close()
            poller.unregister(request_socket)
            
            # Remove from our tracker.
            del(peer_id_string_to_request_socket_map[peer_queue_manager_id_string])
            
            # Send a system message to notify we have closed the socket.
            notification_queue.put(system_messages.SystemSocketStateMessage(thread_name, False, "Request: {0}".format(peer_queue_manager_id_string)))
                    
    # Send a system message to notify we have started our thread.thread.
    notification_queue.put(system_messages.SystemThreadStateMessage(thread_name, True, os.getpid()))
    
    # Enter a loop in which we wait for a router request message.
    is_running = True
    while is_running == True:
        
        try:
            
            # Get a new message; initialize to not being handled. 
            # Note that we will be routed to our empty message exception if no messages are waiting.
            message = action_queue.get(True, 1)
            message_handled = False
            
            # Branch off message type.
            if message.get_type() == message_types.SYSTEM_CONNECT_PEER_REQUEST_SOCKET:
                
                # Connect.
                _connect_socket(message.peer_queue_manager_id_string, message.connection_string)
                
                # Denote message as handled.
                message_handled = True
            
            elif message.get_type() == message_types.SYSTEM_DISCONNECT_PEER_REQUEST_SOCKET:
                
                # Disconnect.
                _disconnect_socket(message.peer_queue_manager_id_string)

                # Denote message as handled.
                message_handled = True
                                        
            elif message.get_type() == message_types.SYSTEM_STOP_THREAD:
                notification_queue.put(system_messages.SystemNotificationMessage(thread_name, "Shutting down per main thread request"))
                message_handled = True
                is_running = False
                
            # If the message hasn't been handled, notify.
            if message_handled == False:
                
                notification_queue.put(system_messages.SystemErrorMessage(thread_name, "Action queue message was not handled: {0}".format(message)))
    
        except EmptyQueueException:
            
            pass
        
        except:
            
            # When we get an exception, log it and break from our loop.
            notification_queue.put(system_messages.SystemErrorMessage(thread_name, "Action queue message processing raised exception: " + ExceptionFormatter.get_message())) 
            break   

    # Close all sockets.
    peer_id_string_list = list(peer_id_string_to_request_socket_map.keys())
    for peer_id_string in peer_id_string_list:
        _disconnect_socket(peer_id_string)
    
    # Send a system message to notify we have shut down.
    notification_queue.put(system_messages.SystemThreadStateMessage(thread_name, False))
    