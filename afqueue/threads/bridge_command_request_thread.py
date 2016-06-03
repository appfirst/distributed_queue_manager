from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.socket_wrapper import SocketWrapper #@UnresolvedImport
from afqueue.common.zmq_utilities import ZmqUtilities #@UnresolvedImport
from afqueue.common_thread_methods.consume_action_queue import consume_action_queue #@UnresolvedImport
from afqueue.messages import system_messages, command_messages #@UnresolvedImport
from afqueue.messages.base_message import BaseMessage #@UnresolvedImport
import afqueue.messages.message_types as message_types #@UnresolvedImport
import zmq #@UnresolvedImport
from afqueue.messages import command_response_codes #@UnresolvedImport
import os


def bridge_command_request_thread(thread_name, zmq_context, shared_dict, command_request_port, command_poll_time_out, action_queue, action_out_queue):
    """
    """    
        
    # Send a system message to notify we have started our thread.
    action_out_queue.put(system_messages.SystemThreadStateMessage(thread_name, True, os.getpid()))    
    
    # Create the bridge request socket.
    bind_string = ZmqUtilities.get_socket_bind_string(command_request_port)
    request_socket = zmq_context.socket(zmq.REP)
    request_socket.setsockopt(zmq.LINGER, 0)
    request_socket.bind(bind_string)    
    
    # Send a system message to notify we have opened our socket.
    action_out_queue.put(system_messages.SystemSocketStateMessage(thread_name, True, "Request bound to {0}".format(bind_string)))
    
    # Create the poller objects so we can utilize time outs.
    poller = zmq.Poller()
    poller.register(request_socket, zmq.POLLIN)
        
    # Enter a loop, waiting for requests.
    while True:
        
        # Accept any data sent to our socket.
        try:
            
            # Poll on receiving with our time out.
            if poller.poll(command_poll_time_out):
    
                # Receive.
                raw_message = SocketWrapper.pull_message(request_socket)
                raw_message_type = BaseMessage.get_raw_message_type(raw_message) 
                
                # Handle based on message type: forward known types to the main thread.       
                if raw_message_type == message_types.COMMAND_SHUT_DOWN_REQUEST:
                            
                    # Decode.
                    decoded_message = command_messages.CommandShutDownMessage.create_from_received(raw_message)
                    
                    # If the decoding was successful, handle the message.   
                    if decoded_message != None:
                        action_out_queue.put(decoded_message)
                        reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_RECEIVED)
                        
                    # If the decoding failed, notify the main thread and set the reply accordingly.
                    else:
                        action_out_queue.put(system_messages.SystemErrorMessage(thread_name, "Malformed message received.")) 
                        reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE)    
                        
                elif raw_message_type == message_types.COMMAND_REMOTE_CONNECT_REQUEST:
                            
                    # Decode.
                    decoded_message = command_messages.CommandRemoteConnectRequestMessage.create_from_received(raw_message)
                    
                    # If the decoding was successful, handle the message.   
                    if decoded_message != None:
                        action_out_queue.put(decoded_message)
                        reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_RECEIVED)
                        
                    # If the decoding failed, notify the main thread and set the reply accordingly.
                    else:
                        action_out_queue.put(system_messages.SystemErrorMessage(thread_name, "Malformed message received.")) 
                        reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE) 
                        
                elif raw_message_type == message_types.COMMAND_REMOTE_DISCONNECT_REQUEST:   
                            
                    # Decode.
                    decoded_message = command_messages.CommandRemoteDisconnectRequestMessage.create_from_received(raw_message)
                    
                    # If the decoding was successful, handle the message.   
                    if decoded_message != None:
                        action_out_queue.put(decoded_message)
                        reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_RECEIVED)
                        
                    # If the decoding failed, notify the main thread and set the reply accordingly.
                    else:
                        action_out_queue.put(system_messages.SystemErrorMessage(thread_name, "Malformed message received.")) 
                        reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE)  
         
                elif raw_message_type == message_types.COMMAND_REMOTE_CONNECT_PIKA_REQUEST:
                            
                    # Decode.
                    decoded_message = command_messages.CommandRemoteConnectPikaRequestMessage.create_from_received(raw_message)
                    
                    # If the decoding was successful, handle the message.   
                    if decoded_message != None:
                        action_out_queue.put(decoded_message)
                        reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_RECEIVED)
                        
                    # If the decoding failed, notify the main thread and set the reply accordingly.
                    else:
                        action_out_queue.put(system_messages.SystemErrorMessage(thread_name, "Malformed message received.")) 
                        reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE)   
                          
                elif raw_message_type == message_types.COMMAND_REMOTE_DISCONNECT_PIKA_REQUEST: 
                            
                    # Decode.
                    decoded_message = command_messages.CommandRemoteDisconnectPikaRequestMessage.create_from_received(raw_message)
                    
                    # If the decoding was successful, handle the message.   
                    if decoded_message != None:
                        action_out_queue.put(decoded_message)
                        reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_RECEIVED)
                        
                    # If the decoding failed, notify the main thread and set the reply accordingly.
                    else:
                        action_out_queue.put(system_messages.SystemErrorMessage(thread_name, "Malformed message received.")) 
                        reply_message = command_messages.CommandReplyMessage(command_response_codes.MALFORMED_MESSAGE)           
                    
                # Handle unknown.
                else:
                    
                    reply_message = command_messages.CommandReplyMessage(command_response_codes.COMMAND_UNKNOWN)
                    
                # Send a response.
                reply_message.send(request_socket)

        except KeyboardInterrupt:           
            
            # Ignore keyboard interrupts; the main thread will handle as desired. 
            pass
                    
        except:
            
            # When we get an exception, log it and break from our loop.
            action_out_queue.put(system_messages.SystemErrorMessage(thread_name, "Failed to receive an incoming message: " + ExceptionFormatter.get_message())) 
            break    
        
        # Consume our action queue; the result is if we should continue running.
        if consume_action_queue(thread_name, action_queue, action_out_queue) == False:
            break        
        
    # Clean up the bridge socket.
    request_socket.setsockopt(zmq.LINGER, 0)
    request_socket.close()
    poller.unregister(request_socket)
    action_out_queue.put(system_messages.SystemSocketStateMessage(thread_name, False, "Request"))
    
    # Send a system message to notify we have shut down.
    action_out_queue.put(system_messages.SystemThreadStateMessage(thread_name, False))
    