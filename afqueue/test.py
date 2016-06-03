#python test.py s localhost:29990,localhost:30000

# ps -ef | grep -v 'grep' | grep python | grep queue_manager | awk '{ print $2; }' | xargs kill

from .test_consumer import TestConsumer
from .test_producer import TestProducer
from .test_streamer import TestStreamer
from .test_command import TestCommand
from .test_bridge import TestBridge
import sys
from .common import client_exchange_types

try:
    from afqueue import queue_config as config
except:
    from afqueue.config_files import queue_config_base as config

#
object_type = sys.argv[1]

# Setup base process name from parsed arguments.
process_base_name = "{1}{0}{2}{3}".format(config.PROCESS_NAME_SEPARATOR, 
                                          config.PROCESS_NAME_PREFIX, config.PROCESS_NAME_TEST, object_type)

# Create the logger before we import any other object which might touch it.
from .common.logger import Logger
log_file_name = "test_{0}".format(object_type)
log_file_path = config.LOG_FILENAME.format(log_file_name)
logger = Logger(process_base_name, "queue", config.LOG_LEVEL, config.LOG_MODE, log_file_path, config.LOG_EXIT_HANDLER_INTERVAL) 

if __name__ == "__main__":
    object_type = sys.argv[1]
    
    if object_type == "b":
        base_port = int(sys.argv[2])
        peer_connection_tuple_list = list()
        for peer_port in sys.argv[3:]:
            peer_connection_tuple_list.append(("localhost", int(peer_port)))
        bridge = TestBridge(base_port, peer_connection_tuple_list)
    
    elif object_type == "cd":
        base_port = int(sys.argv[2])
        force_unlock = sys.argv[3] if len(sys.argv) > 3 else "False"
        force_unlock = force_unlock == "True"
        consumer = TestConsumer(base_port, client_exchange_types.DIRECT, False, force_unlock)
    
    elif object_type == "cf":
        base_port = int(sys.argv[2])
        force_unlock = sys.argv[3] if len(sys.argv) > 3 else "False"
        if force_unlock != None:
            force_unlock = force_unlock == "True"
        consumer = TestConsumer(base_port, client_exchange_types.FANOUT, False, force_unlock)
    
    elif object_type == "ct":
        base_port = int(sys.argv[2])
        force_unlock = sys.argv[3] if len(sys.argv) > 3 else "False"
        if force_unlock != None:
            force_unlock = force_unlock == "True"
        consumer = TestConsumer(base_port, client_exchange_types.TOPIC, False, force_unlock)
    
    elif object_type == "cu":
        base_port = int(sys.argv[2])
        force_unlock = sys.argv[3] if len(sys.argv) > 3 else "False"
        if force_unlock != None:
            force_unlock = force_unlock == "True"
        consumer = TestConsumer(base_port, client_exchange_types.TOPIC, True, force_unlock)
        
    elif object_type == "pd":
        base_port = int(sys.argv[2])
        file_name = sys.argv[3] if len(sys.argv) > 3 else None
        producer = TestProducer(base_port, client_exchange_types.DIRECT, file_name)
        
    elif object_type == "pf":
        base_port = int(sys.argv[2])
        file_name = sys.argv[3] if len(sys.argv) > 3 else None
        producer = TestProducer(base_port, client_exchange_types.FANOUT, file_name)
        
    elif object_type == "pt":
        base_port = int(sys.argv[2])
        file_name = sys.argv[3] if len(sys.argv) > 3 else None
        producer = TestProducer(base_port, client_exchange_types.TOPIC, file_name)
        
    elif object_type == "s":
        connection_information = sys.argv[2]
        file_name = sys.argv[3] if len(sys.argv) > 3 else None
        streamer = TestStreamer(connection_information, client_exchange_types.DIRECT, file_name)
        
    elif object_type == "cmd":
        base_port = int(sys.argv[2])
        cmd = sys.argv[3] if len(sys.argv) > 3 else None
        remote_port = sys.argv[4] if len(sys.argv) > 4 else None
        #remote_ip_address = sys.argv[4] if len(sys.argv) > 4 else None
        #remote_port = sys.argv[5] if len(sys.argv) > 5 else None
        command = TestCommand(base_port, cmd, remote_port)
        
        
# python test.py pd 24000
# python test.py cd 25000
# ps -ef | grep -v 'grep' | grep python | grep afqm | awk '{ print $2; }' | xargs kill
        
