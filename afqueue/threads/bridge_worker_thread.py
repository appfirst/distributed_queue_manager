from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.socket_wrapper import SocketWrapper #@UnresolvedImport
from afqueue.common.zmq_utilities import ZmqUtilities #@UnresolvedImport
from afqueue.messages import system_messages #@UnresolvedImport
import zmq #@UnresolvedImport
from afqueue.common_thread_methods.consume_action_queue import consume_action_queue #@UnresolvedImport
import time, os

DEFINE_ME = 5000.


def bridge_worker_thread(thread_name, zmq_context, worker_router_port, connection_string, remote_poll_timeout, action_queue, notification_out_queue):
    """
    """

    try:            
        
        # Set the poll timeout on waiting for messages from the bridge load balancer.
        # This time out is only used to give us a break in our infinite wait for work so we can check our in-bound notification queue for commands.
        load_balancer_time_out = 100
        
        # Send a system message to notify we have started our thread.
        notification_out_queue.put(system_messages.SystemThreadStateMessage(thread_name, True, os.getpid()))    
        
        # Create the bridge request socket.
        bridge_connection_string = ZmqUtilities.get_socket_connection_string("localhost", worker_router_port)
        bridge_request_socket = zmq_context.socket(zmq.REQ)
        bridge_request_socket.setsockopt(zmq.IDENTITY, thread_name.encode())
        bridge_request_socket.connect(bridge_connection_string)    
        
        # Send a system message to notify we have opened our socket.
        notification_out_queue.put(system_messages.SystemSocketStateMessage(thread_name, True, "Bridge request connected to {0}".format(bridge_connection_string)))
        
        # Create remote request socket.
        remote_request_socket = zmq_context.socket(zmq.REQ)
        remote_request_socket.connect(connection_string)    
        
        # Send a system message to notify we have opened our socket.
        notification_out_queue.put(system_messages.SystemSocketStateMessage(thread_name, True, "Remote request connected to {0}".format(connection_string)))

        # Create the poller objects so we can utilize time outs.
        # We will use a bridge poller to wait for incoming work.
        # We will use a remote poller to wait for a response to come back when we forward a message to the remote connection.
        bridge_poller = zmq.Poller()
        bridge_poller.register(bridge_request_socket, zmq.POLLIN)
        remote_poller = zmq.Poller()
        remote_poller.register(remote_request_socket, zmq.POLLIN)
    
        # Tell the broker we are ready for work
        bridge_request_socket.send(b"READY")
        
        # Track usage.
        call_count = 0
        last_time_sent = time.time()
        notification_interval = 30
        
    except:
        
        raise ExceptionFormatter.get_full_exception()

    polling = True;
    while polling == True:
        
        try:            
            
            # Poll on receiving with our time out.
            if bridge_poller.poll(load_balancer_time_out):

                # Get the message; initialize to not handled.
                full_raw_message = SocketWrapper.pull_message(bridge_request_socket)
                
                # Pull the reply tag and raw message data out of the full raw message.
                reply_id_tag = full_raw_message[0]
                raw_message = full_raw_message[2:]   
                
                # Forward the message to the remote socket.
                for data in raw_message[:-1]:
                    ZmqUtilities.send_data(remote_request_socket, data, zmq.SNDMORE)
                ZmqUtilities.send_data(remote_request_socket, raw_message[-1])
                            
                # Wait for a response from the remote socket.
                if remote_poller.poll(remote_poll_timeout):
                    
                    # Capture the response.
                    response_message = SocketWrapper.pull_message(remote_request_socket)
                    
                    # Put the reply tag on the message and send the received response back to the bridge.

                    ZmqUtilities.send_data(bridge_request_socket, reply_id_tag, zmq.SNDMORE)
                    bridge_request_socket.send(b"", zmq.SNDMORE)
                    for data in response_message[:-1]:
                        ZmqUtilities.send_data(bridge_request_socket, data, zmq.SNDMORE)
                    ZmqUtilities.send_data(bridge_request_socket, response_message[-1])
                    
                    # Increment statistics.
                    current_time = time.time()
                    call_count += 1
                    if current_time - last_time_sent > notification_interval:
                        last_time_sent = current_time
                        notification_out_queue.put(system_messages.SystemNotificationMessage(thread_name, "Handled {0} calls in the past {1} seconds.".format(call_count, notification_interval)))
                        call_count = 0
                    
                # If we timed out waiting for a response from the remote socket.
                else:
                    
                    # Send a TIMEOUT string to the bridge request socket.

                    ZmqUtilities.send_data(bridge_request_socket, reply_id_tag, zmq.SNDMORE)
                    bridge_request_socket.send(b"", zmq.SNDMORE)
                    bridge_request_socket.send(b"TIMEOUT")
                    
                    # Report a time out.
                    # This will ultimately lead the main thread to stop tracking this thread.
                    notification_out_queue.put(system_messages.SystemBridgeWorkerTimedOut(thread_name, "Timed out waiting for response from remote socket.")) 
                    
                    # Stop polling.
                    polling = False
                            
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
        
        # Consume our action queue; the result is if we should continue running.
        if consume_action_queue(thread_name, action_queue, notification_out_queue) == False:
            break
                         
    # Clean up the bridge socket.
    bridge_request_socket.setsockopt(zmq.LINGER, 0)
    bridge_request_socket.close()
    bridge_poller.unregister(bridge_request_socket)
    notification_out_queue.put(system_messages.SystemSocketStateMessage(thread_name, False, "Bridge request"))
    
    # Clean up the remote socket.
    remote_request_socket.setsockopt(zmq.LINGER, 0)
    remote_request_socket.close()
    remote_poller.unregister(remote_request_socket)
    notification_out_queue.put(system_messages.SystemSocketStateMessage(thread_name, False, "Remote request"))
    
    # Send a system message to notify we have shut down.
    notification_out_queue.put(system_messages.SystemThreadStateMessage(thread_name, False))
