from .common.exception_formatter import ExceptionFormatter
from .source.network_bridge import NetworkBridge
import os
import time
import zmq #@UnresolvedImport


class TestBridge():
        
    
    def __init__(self, base_port, qm_connection_list):
        
        # Create the network bridge.
        network_bridge = NetworkBridge(base_port, qm_connection_list)