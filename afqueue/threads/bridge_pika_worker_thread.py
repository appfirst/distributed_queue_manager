from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.socket_wrapper import SocketWrapper #@UnresolvedImport
from afqueue.common.pika_publisher import PikaPublisher #@UnresolvedImport
from afqueue.common.zmq_utilities import ZmqUtilities #@UnresolvedImport
from afqueue.common_thread_methods.consume_action_queue import consume_action_queue #@UnresolvedImport
from afqueue.messages import client_messages, client_response_codes #@UnresolvedImport
from afqueue.messages import system_messages #@UnresolvedImport
import bson #@UnresolvedImport
import time, datetime, os
import zmq #@UnresolvedImport

DEFINE_ME = 5000.


def bridge_pika_worker_thread(thread_name, zmq_context, worker_router_port, queue_server, queue_mode, remote_poll_timeout, action_queue, notification_out_queue, logger):
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
        bridge_request_socket.setsockopt(zmq.IDENTITY, thread_name)
        bridge_request_socket.connect(bridge_connection_string)    
        
        # Send a system message to notify we have opened our socket.
        notification_out_queue.put(system_messages.SystemSocketStateMessage(thread_name, True, "Bridge request connected to {0}".format(bridge_connection_string)))
        
        # Create the pika publisher.
        pika_publisher = PikaPublisher([queue_server,], queue_mode, logger)
        
        # Send a system message to notify we have opened our socket.
        notification_out_queue.put(system_messages.SystemSocketStateMessage(thread_name, True, "Remote pika request connected to {0}".format(queue_server)))

        # Create the poller objects so we can utilize time outs.
        # We will use a bridge poller to wait for incoming work.
        bridge_poller = zmq.Poller()
        bridge_poller.register(bridge_request_socket, zmq.POLLIN)
    
        # Tell the broker we are ready for work
        bridge_request_socket.send(b"READY")
        
        # Track usage.
        call_count = 0
        total_call_send_time = 0
        last_minute_sent = -1
        
    except:
        
        raise ExceptionFormatter.get_full_exception()

    while True:
        
        try:            
            
            # Poll on receiving with our time out.
            if bridge_poller.poll(load_balancer_time_out):

                # Get the message; initialize to not handled.
                full_raw_message = SocketWrapper.pull_message(bridge_request_socket)
                
                # Pull the reply tag and raw message data out of the full raw message.
                reply_id_tag = full_raw_message[0]
                raw_message = full_raw_message[2:]   
                
                # The Pika worker has to rebuild the raw message into the data push message so it can format the data for Pika rather than ZMQ.
                request_message = client_messages.ClientDataPushRequestMessage.create_from_received(raw_message)

                # Correct the routing key; we use ".." as an escape sequence for None. 
                routing_key = None
                if request_message.routing_key != "..":
                    routing_key = request_message.routing_key

                # Get the data.
                data = bson.loads(request_message.data_tuple_dumped)[request_message.routing_key]
                
                """    
                # SMK: This is failover code; use this instead of the line above if there's a chance of either bson or json encoding.
                try:
                    
                    data = bson.loads(request_message.data_tuple_dumped)[request_message.routing_key]
                    
                except:
                    
                    # If we fail with bson, try again using json + decode.  
                    import json
                    data = json.loads(request_message.data_tuple_dumped)[1].decode("base64")
                """
                 
                # Forward the message to the remote socket, tracking traversal time.
                response = client_response_codes.PIKA_DATA_PUSH_SUCCESS
                start_time = time.time()
                try:
                    
                    # Publish.
                    pika_publisher.publish(request_message.exchange_name, routing_key, data, logger)
                    
                    # Check time.
                    current_time = time.time()
                    time_to_send = current_time - start_time
                    if time_to_send > 1:
                        notification_out_queue.put(system_messages.SystemErrorMessage(thread_name, "Time taken to send message is too long: {0}".format(time_to_send)))
                        response = client_response_codes.PIKA_DATA_PUSH_SUCCESS_LONG_SEND
                        
                except:
                    
                    try:
                        
                        # Publish.
                        pika_publisher.publish(request_message.exchange_name, routing_key, data, logger)
                        
                        # Update response code.
                        response = client_response_codes.PIKA_DATA_PUSH_SUCCESS_AFTER_RETRY
                        
                        # Check time.
                        current_time = time.time()
                        time_to_send = current_time - start_time
                        
                    except:
                        
                        # Notify.
                        notification_out_queue.put(system_messages.SystemErrorMessage(thread_name, "Failed in both primary and secondary send attempts; exception: " + ExceptionFormatter.get_message()))
                        
                        # Update response code.
                        response = client_response_codes.PIKA_DATA_PUSH_FAILED_TWICE
                        
                        # Check time.
                        current_time = time.time()
                        time_to_send = current_time - start_time
                                      
                # Create the reply and send it.
                reply_message = client_messages.ClientDataPushReplyMessage(reply_id_tag, response, "")
                reply_message.send(bridge_request_socket)
                
                # Increment statistics.
                call_count += 1
                total_call_send_time += time_to_send
                dt = datetime.datetime.fromtimestamp(current_time)
                if dt.minute != last_minute_sent:
                    last_minute_sent = dt.minute
                    notification_out_queue.put(system_messages.SystemStatsMessage(thread_name, "call_count", call_count))
                    notification_out_queue.put(system_messages.SystemStatsMessage(thread_name, "art", (total_call_send_time / call_count) * 1000))
                    call_count = 0        
                    total_call_send_time = 0
        
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
    pika_publisher.close()
    notification_out_queue.put(system_messages.SystemSocketStateMessage(thread_name, False, "Remote request"))
    
    # Send a system message to notify we have shut down.
    notification_out_queue.put(system_messages.SystemThreadStateMessage(thread_name, False))