from afqueue.common.encoding_utilities import cast_list_of_strings, cast_bytes
from afqueue.messages.base_message import BaseMessage #@UnresolvedImport
from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.messages import message_types #@UnresolvedImport
from afqueue.data_objects.exchange_wrapper import ExchangeWrapper #@UnresolvedImport
from afqueue.data_objects.data_queue_wrapper import DataQueueWrapper #@UnresolvedImport
import json, bson #@UnresolvedImport


class CommandSetMasterRequestMessage(BaseMessage): 
    
    def __init__(self, qm_id_string, notification = ""):
        
        # Build base.
        super(CommandSetMasterRequestMessage, self).__init__(message_types.COMMAND_SET_MASTER_REQUEST)
        
        # Transmitted data.
        self.qm_id_string = qm_id_string
        self.notification = notification 
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.qm_id_string, self.notification)        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandSetMasterRequestMessage(raw_message[1], raw_message[2])
        except:
            return None


class CommandFreezeQueueRequestMessage(BaseMessage): 
    
    def __init__(self, queue_name, freeze_push = False, freeze_pull = False, notification = ""):
        
        # Build base.
        super(CommandFreezeQueueRequestMessage, self).__init__(message_types.COMMAND_FREEZE_QUEUE_REQUEST)
        
        # Transmitted data.
        self.queue_name = queue_name
        self.freeze_push = freeze_push
        self.freeze_pull = freeze_pull
        self.notification = notification 
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.queue_name, str(self.freeze_push), str(self.freeze_pull), self.notification)        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandFreezeQueueRequestMessage(raw_message[1], BaseMessage.bool_from_string(raw_message[2]), BaseMessage.bool_from_string(raw_message[3]), raw_message[4])
        except:
            return None

    
class CommandDeleteQueuesRequestMessage(BaseMessage): 
    
    def __init__(self, queue_name_list, notification = ""):
        
        # Build base.
        super(CommandDeleteQueuesRequestMessage, self).__init__(message_types.COMMAND_DELETE_QUEUES_REQUEST)
        
        # Transmitted data.
        self.queue_name_list = queue_name_list
        self.notification = notification
        

    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, json.dumps(self.queue_name_list), self.notification)        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandDeleteQueuesRequestMessage(json.loads(raw_message[1]), raw_message[2]) 
        except:
            return None


class CommandAddWorkersRequestMessage(BaseMessage): 
    
    def __init__(self, count, notification = ""):
        
        # Build base.
        super(CommandAddWorkersRequestMessage, self).__init__(message_types.COMMAND_ADD_WORKERS_REQUEST)
        
        # Transmitted data.
        self.count = count
        self.notification = notification
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, str(self.count), self.notification)       
        except:            
            raise ExceptionFormatter.get_full_exception()           
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandAddWorkersRequestMessage(int(raw_message[1]), raw_message[2])
        except:
            return None
        

class CommandGetPeckingOrderRequestMessage(BaseMessage): 
    
    def __init__(self, notification = ""):
        
        # Build base.
        super(CommandGetPeckingOrderRequestMessage, self).__init__(message_types.COMMAND_GET_PECKING_ORDER_REQUEST)
        
        # Transmitted data.
        self.notification = notification
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.notification)        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandGetPeckingOrderRequestMessage(raw_message[1])
        except:
            return None


class CommandGetPeckingOrderReplyMessage(BaseMessage): 
    
    def __init__(self, pecking_order_list, notification = ""):
        
        # Build base.
        super(CommandGetPeckingOrderReplyMessage, self).__init__(message_types.COMMAND_GET_PECKING_ORDER_REPLY)
        
        # Transmitted data.
        self.pecking_order_list = pecking_order_list
        self.notification = notification
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, json.dumps(self.pecking_order_list), self.notification)        
        except:            
            raise ExceptionFormatter.get_message()
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return CommandGetPeckingOrderReplyMessage(json.loads(raw_message[1]), raw_message[2])
        except:
            raise ExceptionFormatter.get_message()


class CommandGetStatisticsRequestMessage(BaseMessage): 
    
    def __init__(self, stat_type, notification = ""):
        
        # Build base.
        super(CommandGetStatisticsRequestMessage, self).__init__(message_types.COMMAND_GET_STATISTICS_REQUEST)
        
        # Transmitted data.
        self.type = stat_type
        self.notification = notification
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.type, self.notification)       
        except:            
            raise ExceptionFormatter.get_full_exception()           
    

    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandGetStatisticsRequestMessage(raw_message[1], raw_message[2])
        except:
            return None


class CommandGetStatisticsReplyMessage(BaseMessage): 
    
    def __init__(self, response_code, thread_dict, net_stat_dict, notification = ""):
        
        # Build base.
        super(CommandGetStatisticsReplyMessage, self).__init__(message_types.COMMAND_GET_STATISTICS_REPLY)
        
        # Transmitted data.
        self.response_code = response_code
        self.thread_dict = thread_dict
        self.net_stat_dict = net_stat_dict
        self.notification = notification
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.response_code, bson.dumps(self.thread_dict), bson.dumps(self.net_stat_dict), self.notification)        
        except:            
            raise ExceptionFormatter.get_message()
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return CommandGetStatisticsReplyMessage(raw_message[1], bson.loads(raw_message[2]), bson.loads(raw_message[3]), raw_message[4])
        except:
            raise ExceptionFormatter.get_message()
            

class CommandGetQueueSizeRequestMessage(BaseMessage): 
    
    def __init__(self, queue_name, notification = ""):
        
        # Build base.
        super(CommandGetQueueSizeRequestMessage, self).__init__(message_types.COMMAND_GET_QUEUE_SIZE_REQUEST)
        
        # Transmitted data.
        self.queue_name = queue_name
        self.notification = notification
        
        # Internal data.
        self.queue_size = None
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.queue_name, self.notification)        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandGetQueueSizeRequestMessage(raw_message[1], raw_message[2])
        except:
            return None


class CommandGetQueueSizeReplyMessage(BaseMessage): 
    
    def __init__(self, queue_name, queue_size, notification = ""):
        
        # Build base.
        super(CommandGetQueueSizeReplyMessage, self).__init__(message_types.COMMAND_GET_QUEUE_SIZE_REPLY_MESSAGE)
        
        # Transmitted data.
        self.queue_name = queue_name
        self.queue_size = queue_size
        self.notification = notification
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.queue_name, str(self.queue_size), self.notification)        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return CommandGetQueueSizeReplyMessage(raw_message[1], int(raw_message[2]), raw_message[3])
        except:
            raise ExceptionFormatter.get_message()
            

class CommandGetSetupDataRequestMessage(BaseMessage): 
    
    def __init__(self, notification = ""):
        
        # Build base.
        super(CommandGetSetupDataRequestMessage, self).__init__(message_types.COMMAND_GET_SETUP_DATA_REQUEST)
        
        # Transmitted data.
        self.notification = notification
        
        # Internal data.
        self.queue_size = None
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.notification)        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandGetSetupDataRequestMessage(raw_message[1])
        except:
            return None


class CommandGetSetupDataReplyMessage(BaseMessage): 
    
    def __init__(self, response_code, exchange_wrapper_list, queue_wrapper_list, notification = ""):
        
        # Build base.
        super(CommandGetSetupDataReplyMessage, self).__init__(message_types.COMMAND_GET_SETUP_DATA_REPLY_MESSAGE)
        
        # Transmitted data.
        self.response_code = response_code
        self.exchange_wrapper_list = exchange_wrapper_list
        self.queue_wrapper_list = queue_wrapper_list
        self.notification = notification
    
            
    def dump(self):
        """
        Dumps the message into a format in which it can be recreated via the "load" method.
        """
        
        try:
            
            dump_dict = dict()
            dump_dict["ewl"] = [ew.dump() for ew in self.exchange_wrapper_list]
            dump_dict["qwl"] = [qw.dump() for qw in self.queue_wrapper_list]
            dump_dict["n"] = self.notification
            return bson.dumps(dump_dict)     
        
        except:            
            
            raise ExceptionFormatter.get_full_exception()
            
            
    @staticmethod
    def load(dumped_string):
        """
        Returns an instance object of this class built from data which was created in the "dump" method.
        """
        
        dump_dict = bson.loads(dumped_string)
        exchange_wrapper_list = [ExchangeWrapper.load(dew) for dew in dump_dict["ewl"]]
        queue_wrapper_list = [DataQueueWrapper.load(dqw) for dqw in dump_dict["qwl"]]
        notification = dump_dict["n"]
        return CommandGetSetupDataReplyMessage("", exchange_wrapper_list, queue_wrapper_list, notification)
    
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.response_code, self.dump())        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            message = CommandGetSetupDataReplyMessage.load(raw_message[2])
            message.response_code = raw_message[1]
            return message
        except:
            raise ExceptionFormatter.get_full_exception()

            
class CommandListQueuesRequestMessage(BaseMessage): 
    
    def __init__(self, queue_name_list, from_all_servers_flag, notification = ""):
        
        # Build base.
        super(CommandListQueuesRequestMessage, self).__init__(message_types.COMMAND_LIST_QUEUES_REQUEST)
        
        # Transmitted data.
        self.queue_name_list = queue_name_list
        self.from_all_servers_flag = from_all_servers_flag
        self.notification = notification
        
        # Internal data.
        self.queue_size = None
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, ";".join(self.queue_name_list), str(self.from_all_servers_flag), self.notification)        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandListQueuesRequestMessage(raw_message[1].split(";"), BaseMessage.bool_from_string(raw_message[2]), raw_message[3])
        except:
            return None


class CommandListQueuesReplyMessage(BaseMessage): 
    
    def __init__(self, response_code, queue_size_dict, notification = ""):
        
        # Build base.
        super(CommandListQueuesReplyMessage, self).__init__(message_types.COMMAND_LIST_QUEUES_REPLY)
        
        # Transmitted data.
        self.response_code = response_code
        self.queue_size_dict = queue_size_dict
        self.notification = notification
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.response_code, bson.dumps(self.queue_size_dict), self.notification)        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            raw_message[2] = cast_bytes(raw_message[2])
            return CommandListQueuesReplyMessage(raw_message[1], bson.loads(raw_message[2]), raw_message[3])
        except:
            raise ExceptionFormatter.get_message()


class CommandPurgeQueuesRequestMessage(BaseMessage): 
    
    def __init__(self, queue_name_list, from_all_servers_flag, notification = ""):
        
        # Build base.
        super(CommandPurgeQueuesRequestMessage, self).__init__(message_types.COMMAND_PURGE_QUEUES_REQUEST)
        
        # Transmitted data.
        self.queue_name_list = queue_name_list
        self.from_all_servers_flag = from_all_servers_flag
        self.notification = notification 
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, json.dumps(self.queue_name_list), str(self.from_all_servers_flag), self.notification)        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandPurgeQueuesRequestMessage(json.loads(raw_message[1]), BaseMessage.bool_from_string(raw_message[2]), raw_message[3])
        except:
            return None


class CommandRemoteConnectRequestMessage(BaseMessage): 
    
    def __init__(self, remote_ip_address, remote_port, count, notification = ""):
        
        # Build base.
        super(CommandRemoteConnectRequestMessage, self).__init__(message_types.COMMAND_REMOTE_CONNECT_REQUEST)
        
        # Transmitted data.
        self.remote_ip_address = remote_ip_address
        self.remote_port = remote_port
        self.count = count
        self.notification = notification
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.remote_ip_address, str(self.remote_port), str(self.count), self.notification)     
        except:            
            raise ExceptionFormatter.get_full_exception()  
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandRemoteConnectRequestMessage(raw_message[1], int(raw_message[2]), int(raw_message[3]), raw_message[4])
        except:
            return None
        

class CommandRemoteConnectPikaRequestMessage(BaseMessage): 
    
    def __init__(self, connection_string, queue_mode, count, notification = ""):
        
        # Build base.
        super(CommandRemoteConnectPikaRequestMessage, self).__init__(message_types.COMMAND_REMOTE_CONNECT_PIKA_REQUEST) 
        
        # Transmitted data.
        self.connection_string = connection_string
        self.queue_mode = queue_mode
        self.count = count
        self.notification = notification
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.connection_string, self.queue_mode, str(self.count), self.notification)    
        except:            
            raise ExceptionFormatter.get_full_exception()  
            
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandRemoteConnectPikaRequestMessage(raw_message[1], raw_message[2], int(raw_message[3]), raw_message[4])
        except:
            return None
    

class CommandRemoteDisconnectRequestMessage(BaseMessage): 
    
    def __init__(self, remote_ip_address, remote_port, count, notification = ""):
        
        # Build base.
        super(CommandRemoteDisconnectRequestMessage, self).__init__(message_types.COMMAND_REMOTE_DISCONNECT_REQUEST)
        
        # Transmitted data.
        self.remote_ip_address = remote_ip_address
        self.remote_port = remote_port
        self.count = count
        self.notification = notification
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.remote_ip_address, str(self.remote_port), str(self.count), self.notification)  
        except:            
            raise ExceptionFormatter.get_full_exception()          
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandRemoteDisconnectRequestMessage(raw_message[1], int(raw_message[2]), int(raw_message[3]), raw_message[4])
        except:
            return None
    

class CommandRemoteDisconnectPikaRequestMessage(BaseMessage): 
    
    def __init__(self, connection_string, count, notification = ""):
        
        # Build base.
        super(CommandRemoteDisconnectPikaRequestMessage, self).__init__(message_types.COMMAND_REMOTE_DISCONNECT_PIKA_REQUEST)
        
        # Transmitted data.
        self.connection_string = connection_string
        self.count = count
        self.notification = notification
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.connection_string, str(self.count), self.notification)   
        except:            
            raise ExceptionFormatter.get_full_exception()          
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandRemoteDisconnectPikaRequestMessage(raw_message[1], int(raw_message[2]), raw_message[3])
        except:
            return None
    
    
class CommandRemoveWorkersRequestMessage(BaseMessage): 
    
    def __init__(self, count, notification = ""):
        
        # Build base.
        super(CommandRemoveWorkersRequestMessage, self).__init__(message_types.COMMAND_REMOVE_WORKERS_REQUEST)
        
        # Transmitted data.
        self.count = count
        self.notification = notification
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, str(self.count), self.notification)  
        except:            
            raise ExceptionFormatter.get_full_exception()    
        

    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandRemoveWorkersRequestMessage(int(raw_message[1]), raw_message[2])
        except:
            return None
            

class CommandReplyMessage(BaseMessage): 
        
    def __init__(self, response_code, notification = ""):
        
        # Build base.
        super(CommandReplyMessage, self).__init__(message_types.COMMAND_REPLY)
        
        # Store data.
        self.response_code = response_code
        self.notification = notification
            
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.response_code, self.notification)
        except:            
            raise ExceptionFormatter.get_full_exception()    

                        
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return CommandReplyMessage(raw_message[1], raw_message[2])
        except:
            raise ExceptionFormatter.get_message()


class CommandShutDownMessage(BaseMessage): 
    
    def __init__(self, notification = ""):
        
        # Build base.
        super(CommandShutDownMessage, self).__init__(message_types.COMMAND_SHUT_DOWN_REQUEST)
        
        # Transmitted data.
        self.notification = notification
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.notification)        
        except:            
            raise ExceptionFormatter.get_full_exception()  
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandShutDownMessage(raw_message[1])
        except:
            return None


class CommandUnlockQueueRequestMessage(BaseMessage): 
    
    def __init__(self, queue_name, notification = ""):
        
        # Build base.
        super(CommandUnlockQueueRequestMessage, self).__init__(message_types.COMMAND_UNLOCK_QUEUE_REQUEST)
        
        # Transmitted data.
        self.queue_name = queue_name
        self.notification = notification
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.queue_name, self.notification)        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return CommandUnlockQueueRequestMessage(raw_message[1], raw_message[2])
        except:
            return None
        
