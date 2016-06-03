# Pull in our config.
try:
    from afqueue import queue_config as config
except:
    from afqueue.config_files import queue_config_base as config

# Parse our arguments.
import argparse, sys
parser = argparse.ArgumentParser()
parser.add_argument("--command", choices=["shutdown", "connect", "disconnect"], required=True)
parser.add_argument("--address", required=True, help="IP:Port of socket to receive command.  Multiple must be separated by commas.")
parser.add_argument("--queue", required=False, help="Name of the queue to work with for queue commands.")
parser.add_argument("--queues", required=False, help="List of queue names for queue related commands; comma delimited.")
parser.add_argument("--remote_ip", default="localhost", required=False, help="IP of remote socket which the command will be ran against.")
parser.add_argument("--remote_port", type=int, required=False, help="Port of remote socket which the command will be ran against.  Integer.")
parser.add_argument("--remote_type", choices=["qm", "zmq", "pika"], default="zmq", required=False, help="The type of the remote socket connection the command will be ran against.")
parser.add_argument("--count", type=int, default=1, help="The iteration count of the command.  Integer.")
parser.add_argument("--queue_server", required=False, help="RabbitMQ server connection string which the command will be ran against; ip:port.")
parser.add_argument("--queue_mode", choices=["rabbitmq_cluster", "rabbitmq_singular"], default="rabbitmq_cluster", required=False, help="RabbitMQ Server mode which the command will be ran against.  Integer.")
parser.add_argument("--option", choices=["all"], required=False)
parser.add_argument("--note", default="", required=False, help="Optional notification to appear in the destination logs when the command is received.")
parsed_arguments = parser.parse_args(sys.argv[1:])

# Import the rest of our dependencies.
from afqueue.common.exception_formatter import ExceptionFormatter
from afqueue.common.socket_wrapper import SocketWrapper #@UnresolvedImport
from afqueue.common.zmq_utilities import ZmqUtilities           
from afqueue.messages import command_messages
from afqueue.source.shared_memory_manager import SharedMemoryManager
import zmq #@UnresolvedImport
import operator



def run_command(message, reply_message_type):
    
    try:
        
        # Create the ZMQ context.
        zmq_context = zmq.Context(1)
        
        # Form return dictionary.
        results_dict = dict()
        
        # Issue the command to each IP/Port combination we were given.
        for address in parsed_arguments.address.split(","):
            
            # Split the IP and port.
            split_data = address.split(":")
            bridge_ip = split_data[0]
            bridge_port = split_data[1]
        
            # Get the connection string and create the socket connection. 
            connection_string = ZmqUtilities.get_socket_connection_string(bridge_ip, bridge_port)
            remote_request_socket = zmq_context.socket(zmq.REQ)
            remote_request_socket.setsockopt(zmq.LINGER, 0)
            remote_request_socket.connect(connection_string)
            
            try:    
                
                # Send and wait for a reply.
                print("Sending message: {0}".format(message))
                message.send(remote_request_socket)
                
                # Get the message; initialize to not handled.
                print("Waiting for reply...")
                raw_message = SocketWrapper.pull_message(remote_request_socket)
                                
                # Path off command.
                reply_message = reply_message_type.create_from_received(raw_message)
                if int(reply_message.response_code) >= 500:
                    raise Exception("Error code returned.  Reply: {0}".format(reply_message))
                
                #
                results_dict[address] = reply_message
                        
            except:
                
                raise ExceptionFormatter.get_full_exception()
            
            finally:
                    
                # Destroy our socket.
                remote_request_socket.setsockopt(zmq.LINGER, 0)
                remote_request_socket.close()
                
        # Return results.
        return results_dict

    except:
        
        raise ExceptionFormatter.get_full_exception()

# Create the command message before bothering to create our sockets, in case the command setup fails.
try:    
    
    # Set the reply message type we'll receive.
    reply_message_type = command_messages.CommandReplyMessage
    
    # Shutdown command.
    if parsed_arguments.command == "shutdown":
        message = command_messages.CommandShutDownMessage(parsed_arguments.note)
        
        # Run the command.
        results_dict = reply_message = run_command(message, reply_message_type)
        print("Command successful.")
        for destination_server_name, reply_message in list(results_dict.items()):
            if reply_message.notification != "":
                print(destination_server_name, " - Notification received: ", reply_message.notification)
                                
    # Remote connect command.
    elif parsed_arguments.command == "connect":
        
        # Create message.
        if parsed_arguments.remote_type == "zmq":
            if parsed_arguments.remote_ip == None:
                raise Exception("No remote IP specified for remote connection call")
            elif parsed_arguments.remote_port == None:
                raise Exception("No remote port specified for remote connection call")
            message = command_messages.CommandRemoteConnectRequestMessage(parsed_arguments.remote_ip, parsed_arguments.remote_port, 
                                                                          parsed_arguments.count)
            
        elif parsed_arguments.remote_type == "pika":
            if parsed_arguments.queue_server == None:
                raise Exception("No queue server string specified for remote connection call")
            elif parsed_arguments.queue_mode == None:
                raise Exception("No queue mode specified for remote connection call")
            message = command_messages.CommandRemoteConnectPikaRequestMessage(parsed_arguments.queue_server, parsed_arguments.queue_mode, 
                                                                              parsed_arguments.count)
            
        # Run the command.
        results_dict = run_command(message, reply_message_type)
        print("Command successful.")
        for destination_server_name, reply_message in list(results_dict.items()):
            if reply_message.notification != "":
                print(destination_server_name, " - Notification received: ", reply_message.notification)
        
    # Remote disconnect command.
    elif parsed_arguments.command == "disconnect":
        
        # Validate remote details.
        if parsed_arguments.remote_type == "zmq":
            if parsed_arguments.remote_ip == None:
                raise Exception("No remote IP specified for remote connection call")
            elif parsed_arguments.remote_port == None:
                raise Exception("No remote port specified for remote connection call")
            message = command_messages.CommandRemoteDisconnectRequestMessage(parsed_arguments.remote_ip, parsed_arguments.remote_port, 
                                                                             parsed_arguments.count, parsed_arguments.note)
            
        elif parsed_arguments.remote_type == "pika":
            if parsed_arguments.queue_server == None:
                raise Exception("No queue server string specified for remote connection call")
            elif parsed_arguments.queue_mode == None:
                raise Exception("No queue mode specified for remote connection call")
            message = command_messages.CommandRemoteDisconnectPikaRequestMessage(parsed_arguments.queue_server, 
                                                                                 parsed_arguments.count, parsed_arguments.note)
            
        # Run the command.
        results_dict = run_command(message, reply_message_type)
        print("Command successful.")
        for destination_server_name, reply_message in list(results_dict.items()):
            if reply_message.notification != "":
                print(destination_server_name, " - Notification received: ", reply_message.notification)
            
                        
    # Unknown command.
    else:
        
        raise Exception("Invalid command type specified")
            
except:
    
    raise ExceptionFormatter.get_full_exception()

    