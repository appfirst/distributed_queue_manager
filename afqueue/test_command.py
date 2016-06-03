from .common.exception_formatter import ExceptionFormatter
from .common.zmq_client import ZmqClient
from .common.client_queue import ClientQueue 
from . import messages.message_types as message_types
from .messages import command_messages
import os
import time
import zmq #@UnresolvedImport
from .common import client_exchange_types
from .common.client_exchange import ClientExchange #@UnresolvedImport
from .common.zmq_utilities import ZmqUtilities


class TestCommand():
    
    def __init__(self, base_port, command = "shutdown", remote_port = -1):
        """
        """
    
        # Create the ZMQ context.
        zmq_context = zmq.Context(1)
        
        # Get the connection string and create the socket connection. 
        connection_string = ZmqUtilities.get_socket_connection_string("localhost", base_port)
        remote_request_socket = zmq_context.socket(zmq.REQ)
        remote_request_socket.setsockopt(zmq.LINGER, 0)
        remote_request_socket.connect(connection_string)
         
        # Setup polling.
        remote_poller = zmq.Poller()
        remote_poller.register(remote_request_socket, zmq.POLLIN)
        
        # Get the command we will send.
        message = None
        if command == "shutdown":
            message = command_messages.CommandShutDownMessage("Testing")
        elif command == "connect":
            remote_ip_address = "localhost"
            message = command_messages.CommandRemoteConnectRequestMessage(remote_ip_address, remote_port, "Testing")
        elif command == "disconnect":
            remote_ip_address = "localhost"
            message = command_messages.CommandRemoteDisconnectRequestMessage(remote_ip_address, remote_port, "Testing")

        if message == None:
            print("No message type found.")    
            
        else:        
        
            # Send and wait for a reply.
            print(message)
            message.send(remote_request_socket)
    
            # Get the message; initialize to not handled.
            raw_message = SocketWrapper.pull_message(remote_request_socket)
            reply_message = command_messages.CommandReplyMessage.create_from_received(raw_message)
            
            print(raw_message)
            
        # Destroy our socket.
        remote_request_socket.setsockopt(zmq.LINGER, 0)
        remote_request_socket.close()
        remote_poller.unregister(remote_request_socket)
        