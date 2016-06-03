from queue import Empty as EmptyQueueException #@UnresolvedImport
from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.socket_wrapper import SocketWrapper #@UnresolvedImport
from afqueue.messages import system_messages #@UnresolvedImport
import afqueue.messages.message_types as message_types #@UnresolvedImport
import zmq #@UnresolvedImport
import os, time

            
def pqm_incoming_message_thread(thread_name, dealer_id_string, poll_timeout, no_activity_sleep, 
                                action_queue, notification_queue, received_message_queue):
    """
    This thread is responsible for handling incoming messages from PQMs.
    An outside thread can command this thread to take action via the action queue.
    Results and notifications are put in the notification queue (when necessary).
    A PQM connection action will create a dealer socket to the desired PQM.
    This thread will then watch for incoming messages from that PQM.
    Messages received from PQMs will be put on the PQM received message queue.
    Note that this ROUTER/DEALER connection relationship is modeled after PUSH/PULL for asynchronous data transmission.
    """
    
    # Send a system message to notify we have started our thread.thread.
    notification_queue.put(system_messages.SystemThreadStateMessage(thread_name, True, os.getpid()))     

    # We will need to track our current dealer sockets and which peer queue manager they map to.
    dealer_socket_to_id_string_map = dict()
    
    # Create our ZMQ context.
    zmq_context = zmq.Context(1)
    
    # Use a poller so we can receive with timeouts (receive is blocking; we can poll with a timeout).
    # This will allow us to see if we should shut down the socket in between polls.
    # This will also allow us to check our request queue to add more connections to the poller.
    poller = zmq.Poller()
            
    # Enter our loop.
    # We have two steps:
    #  1) Accept any data sent to our dealer sockets by PQMs (poll to check for existence).
    #  2) Respond to requests put this thread's action queue.
    is_running = True
    while is_running == True:              
        
        # Track if we have handled any data in each iteration so we know if we should sleep or not to give the CPU some rest.
        handled_data = False

        # 1) Accept any data sent to our dealer sockets by PQMs.
        try:
            
            # Poll for socket events.
            socket_events_dictionary = dict(poller.poll(poll_timeout))
            
            # Check each dealer socket in our map.
            for dealer_socket in list(dealer_socket_to_id_string_map.keys()):
                
                # If the dealer socket has data waiting, process it.
                if dealer_socket in socket_events_dictionary and socket_events_dictionary[dealer_socket] == zmq.POLLIN:
                    
                    # Receive.
                    received_message = SocketWrapper.pull_message(dealer_socket)
                    
                    # Place the PQM's id string and received message in our received queue.
                    # Received messages are handled outside of this thread; we must allow it to continue receiving uninterrupted.  
                    received_message_queue.put((dealer_socket_to_id_string_map[dealer_socket], received_message))     
                    
                    # Denote data was handled.
                    handled_data = True  
        
        except KeyboardInterrupt:
            
            # Ignore keyboard interrupts; the main thread will handle as desired. 
            pass   
                    
        except:
            
            # When we get an exception, log it and break from our loop.
            notification_queue.put(system_messages.SystemErrorMessage(thread_name, "Failed to receive an incoming PQM message: " + ExceptionFormatter.get_message())) 
            break    
        
        # 2) Respond to requests put this thread's action queue.
        action_queue_exception = False
        while True:
               
            try:
                             
                # Get a new message; initialize to not being handled. 
                # Note that we will be routed to our empty message exception if no messages are waiting.
                message = action_queue.get(False)
                message_handled = False
                
                # Denote data was handled.
                handled_data = True  
                
                # Based on the message type, handle.
                if message.get_type() == message_types.SYSTEM_PEER_CONNECTION_UDPATE:
                    
                    # If we should connect.
                    if message.connect_flag == True:
    
                        # This message is requesting we make a DEALER socket connection to the given peer.
                        
                        # Create the dealer socket, setting the identity to this QM.
                        # This will tell tell the corresponding ROUTER running on the PQM to only send data intended for this QM.
                        dealer_socket = zmq_context.socket(zmq.DEALER)
                        dealer_socket.setsockopt(zmq.IDENTITY, message.dealer_id_string.encode())
                        dealer_socket.connect(message.peer_socket_connection_string)
                        
                        # Send a system message to notify we have opened our socket.
                        notification_queue.put(system_messages.SystemSocketStateMessage(thread_name, True, "Dealer connected to {0} in response to action request.".format(message.peer_socket_connection_string)))
                        
                        # Register in our map.
                        dealer_socket_to_id_string_map[dealer_socket] = message.peer_id_string
                        
                        # Register in our poller.
                        poller.register(dealer_socket, zmq.POLLIN)

                    # If we should disconnect.
                    else:
                        
                        # This message is requesting we remove a DEALER socket connection from the given peer.
                        
                        # Get the dealer socket from the ID string.
                        dealer_socket = None
                        for test_socket, test_id_string in list(dealer_socket_to_id_string_map.items()):
                            if message.peer_id_string == test_id_string:
                                dealer_socket = test_socket
                                break
                            
                        # Close and remove from our poller.
                        dealer_socket.setsockopt(zmq.LINGER, 0)
                        dealer_socket.close()
                        poller.unregister(dealer_socket)
                        
                        # Remove the dealer socket.
                        del dealer_socket_to_id_string_map[dealer_socket]
                        
                        # Send a system message to notify we have closed the socket.
                        notification_queue.put(system_messages.SystemSocketStateMessage(thread_name, False, "Dealer disconnected from {0} in response to action request.".format(message.peer_socket_connection_string)))
                                            
                    # Denote the message has been handled.
                    message_handled = True
                        
                elif message.get_type() == message_types.SYSTEM_STOP_THREAD:
                    notification_queue.put(system_messages.SystemNotificationMessage(thread_name, "Shutting down per main thread request"))
                    message_handled = True
                    is_running = False
                    
                # If the message hasn't been handled, notify.
                if message_handled == False:
                    notification_queue.put(system_messages.SystemErrorMessage(thread_name, "Action queue message was not handled: {0}".format(message), thread_name))
        
            except EmptyQueueException:
                
                # We are looping as we read from our queue object without waiting; as soon as we hit an empty message, we should break from the loop.
                break
        
            except KeyboardInterrupt:
                
                # Ignore keyboard interrupts; the main thread will handle as desired. 
                pass   
            
            except:
                
                # When we get an exception, log it and break from our loop.
                notification_queue.put(system_messages.SystemErrorMessage(thread_name, "Action queue message processing raised exception: " + ExceptionFormatter.get_message()))
                try:
                    notification_queue.put(system_messages.SystemErrorMessage(thread_name, str(message)))
                except:
                    pass                    
                action_queue_exception = True
                break   
        
        # If we had an action queue exception, we must exit out of our thread loop.
        if action_queue_exception == True:
            break
        
        # If we handled no messages, sleep.
        if handled_data == False:
            time.sleep(no_activity_sleep)

    # Close all sockets.
    for dealer_socket in list(dealer_socket_to_id_string_map.keys()):
        
        # Close and remove from our poller.
        dealer_socket.setsockopt(zmq.LINGER, 0)
        dealer_socket.close()
        poller.unregister(dealer_socket)
        
        # Send a system message to notify we have closed the socket.
        notification_queue.put(system_messages.SystemSocketStateMessage(thread_name, False, "Dealer: {0}".format(dealer_socket_to_id_string_map[dealer_socket])))
    
    # Send a system message to notify we have shut down.
    notification_queue.put(system_messages.SystemThreadStateMessage(thread_name, False))
    