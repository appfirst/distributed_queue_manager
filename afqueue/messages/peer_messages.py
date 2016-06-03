from afqueue.common.encoding_utilities import cast_bytes
from afqueue.messages.base_message import BaseMessage #@UnresolvedImport
from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.client_queue_lock import ClientQueueLock #@UnresolvedImport
from afqueue.messages import message_types #@UnresolvedImport
from afqueue.common.client_exchange import ClientExchange #@UnresolvedImport
from afqueue.common.client_queue import ClientQueue #@UnresolvedImport
from afqueue.data_objects.exchange_wrapper import ExchangeWrapper #@UnresolvedImport
from afqueue.data_objects.data_queue_wrapper import DataQueueWrapper #@UnresolvedImport
import simplejson as json #@UnresolvedImport
import bson #@UnresolvedImport
        

def build_settings_dictionary(id_string, start_time, redis_connection_string, shared_memory_max_size, 
                              ordered_ownership_stop_threshold, ordered_ownership_start_threshold):
    """
    Builds the settings dictionary which peers use to pass settings information back and forth.
    """
    
    # Build.
    settings_dict = dict()
    settings_dict["id"] = id_string
    settings_dict["start_time"] = start_time
    settings_dict["sm_connection"] = redis_connection_string
    settings_dict["sm_max"] = shared_memory_max_size
    settings_dict["oq_stop"] = ordered_ownership_stop_threshold
    settings_dict["oq_start"] = ordered_ownership_start_threshold
    
    # Return.
    return settings_dict


class PeerForwardedCommandMessage(BaseMessage): 
    
    def __init__(self, destination_dealer_id_tag, command_message_as_dict, sender_id_string = None):
        
        # Build base.
        super(PeerForwardedCommandMessage, self).__init__(message_types.PEER_FORWARDED_COMMAND_MESSAGE)
        
        # Store data.
        self.destination_dealer_id_tag = destination_dealer_id_tag
        self.command_message_as_dict = command_message_as_dict
        
        # Internal data.
        self.sender_id_string = sender_id_string
                
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination(self, socket, self.destination_dealer_id_tag, bson.dumps(self.command_message_as_dict))
        except:          
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message, sender_id_string):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return PeerForwardedCommandMessage(None, bson.loads(raw_message[1]), sender_id_string)
        except:
            raise ExceptionFormatter.get_full_exception()


class PeerOrderedQueuesExhaustedOwnersMessage(BaseMessage):
    
    def __init__(self, destination_dealer_id_tag, ordered_queues_owners_exhausted_dictionary, sender_id_string = None):
        
        # Build base.
        super(PeerOrderedQueuesExhaustedOwnersMessage, self).__init__(message_types.PEER_ORDERED_QUEUES_OWNERS_EXHAUSTED)
        
        # Store data.
        self.destination_dealer_id_tag = destination_dealer_id_tag
        self.ordered_queues_owners_exhausted_dictionary = ordered_queues_owners_exhausted_dictionary
        
        # Internal data.
        self.sender_id_string = sender_id_string
                
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination(self, socket, self.destination_dealer_id_tag, bson.dumps(self.ordered_queues_owners_exhausted_dictionary))
        except:          
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message, sender_id_string):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            raw_message[1] = cast_bytes(raw_message[1])
            return PeerOrderedQueuesExhaustedOwnersMessage(None, bson.loads(raw_message[1]), sender_id_string)
        except:
            raise ExceptionFormatter.get_full_exception()

        
class PeerClientDeclareExchangesRequestMessage(BaseMessage):
    
    def __init__(self, destination_dealer_id_tag, client_exchange_list, sender_id_string = None):
        
        # Build base.
        super(PeerClientDeclareExchangesRequestMessage, self).__init__(message_types.PEER_CLIENT_DECLARE_EXCHANGES_REQUEST)
        
        # Store data.
        self.destination_dealer_id_tag = destination_dealer_id_tag
        self.client_exchange_list = client_exchange_list
        
        # Internal data.
        self.sender_id_string = sender_id_string
                
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination(self, socket, self.destination_dealer_id_tag, json.dumps(ClientExchange.create_network_tuple_list(self.client_exchange_list)))
        except:          
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message, sender_id_string):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return PeerClientDeclareExchangesRequestMessage(None, ClientExchange.create_client_exchange_list(json.loads(raw_message[1])), sender_id_string)
        except:
            raise ExceptionFormatter.get_full_exception()
        
        
class PeerClientDeclareQueuesRequestMessage(BaseMessage):
    
    def __init__(self, destination_dealer_id_tag, client_queue_list, sender_id_string = None):
        
        # Build base.
        super(PeerClientDeclareQueuesRequestMessage, self).__init__(message_types.PEER_CLIENT_DECLARE_QUEUES_REQUEST)
        
        # Store data.
        self.destination_dealer_id_tag = destination_dealer_id_tag
        self.client_queue_list = client_queue_list
        
        # Internal data.
        self.sender_id_string = sender_id_string
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination(self, socket, self.destination_dealer_id_tag, json.dumps(ClientQueue.create_network_tuple_list(self.client_queue_list)))
        except:       
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message, sender_id_string):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return PeerClientDeclareQueuesRequestMessage(None, ClientQueue.create_client_queue_list(json.loads(raw_message[1])), sender_id_string)
        except:            
            raise ExceptionFormatter.get_full_exception()
        
        
class PeerClientDeleteQueuesRequestMessage(BaseMessage):
    
    def __init__(self, destination_dealer_id_tag, queue_name_list, sender_id_string = None):
        
        # Build base.
        super(PeerClientDeleteQueuesRequestMessage, self).__init__(message_types.PEER_CLIENT_DELETE_QUEUES_REQUEST)
        
        # Store data.
        self.destination_dealer_id_tag = destination_dealer_id_tag
        self.queue_name_list = queue_name_list
        
        # Internal data.
        self.sender_id_string = sender_id_string
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination(self, socket, self.destination_dealer_id_tag, json.dumps(self.queue_name_list))
        except:         
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message, sender_id_string):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return PeerClientDeleteQueuesRequestMessage(None, json.loads(raw_message[1]), sender_id_string)
        except:            
            raise ExceptionFormatter.get_full_exception()
            

class PeerClientLockQueuesRequestMessage(BaseMessage):
    
    def __init__(self, destination_dealer_id_tag, client_queue_lock_list, owner_id_string, sender_id_string = None):
        
        # Build base.
        super(PeerClientLockQueuesRequestMessage, self).__init__(message_types.PEER_CLIENT_LOCK_QUEUES_REQUEST)
        
        # Store data.
        self.destination_dealer_id_tag = destination_dealer_id_tag
        self.client_queue_lock_list = client_queue_lock_list
        self.owner_id_string = owner_id_string
        
        # Internal data.
        self.sender_id_string = sender_id_string
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination(self, socket, self.destination_dealer_id_tag, json.dumps(ClientQueueLock.create_network_tuple_list(self.client_queue_lock_list)), self.owner_id_string)
        except:         
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message, sender_id_string):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return PeerClientLockQueuesRequestMessage(None, ClientQueueLock.create_client_queue_lock_list(json.loads(raw_message[1])), raw_message[2], sender_id_string)
        except:            
            raise ExceptionFormatter.get_full_exception()
        
        
class PeerClientUnlockQueuesRequestMessage(BaseMessage):
    
    def __init__(self, destination_dealer_id_tag, queue_name_list, sender_id_string = None):
        
        # Build base.
        super(PeerClientUnlockQueuesRequestMessage, self).__init__(message_types.PEER_CLIENT_UNLOCK_QUEUES_REQUEST)
        
        # Store data.
        self.destination_dealer_id_tag = destination_dealer_id_tag
        self.queue_name_list = queue_name_list
        
        # Internal data.
        self.sender_id_string = sender_id_string
        self.owner_id_string = None
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination(self, socket, self.destination_dealer_id_tag, json.dumps(self.queue_name_list))
        except:         
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message, sender_id_string):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return PeerClientUnlockQueuesRequestMessage(None, json.loads(raw_message[1]), sender_id_string)
        except:            
            raise ExceptionFormatter.get_full_exception()
            
            
class PeerHeartBeatFailureMessage(BaseMessage): 
    
    def __init__(self, destination_dealer_id_tag, disconnecting_flag, sender_id_string = None):
        
        # Build base.
        super(PeerHeartBeatFailureMessage, self).__init__(message_types.PEER_HEART_BEAT_FAILURE)
        
        # Transmitted data.
        self.destination_dealer_id_tag = destination_dealer_id_tag
        self.disconnecting_flag = disconnecting_flag
        
        # Internal data.
        self.sender_id_string = sender_id_string
            
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination(self, socket, self.destination_dealer_id_tag, str(self.disconnecting_flag))        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message, sender_id_string):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            diconnecting_flag = True if raw_message[1] == "True" else False
            return PeerHeartBeatFailureMessage(None, diconnecting_flag, sender_id_string)
        except:
            raise ExceptionFormatter.get_full_exception()
            

class PeerHeartBeatMessage(BaseMessage): 
    
    def __init__(self, destination_dealer_id_tag, sender_time_stamp, sender_queue_size_snapshot_dict, sender_id_string = None):
        
        # Build base.
        super(PeerHeartBeatMessage, self).__init__(message_types.PEER_HEART_BEAT)
        
        # Transmitted data.
        self.destination_dealer_id_tag = destination_dealer_id_tag
        self.sender_time_stamp = sender_time_stamp
        self.sender_queue_size_snapshot_dict = sender_queue_size_snapshot_dict
        
        # Internal data.
        self.sender_id_string = sender_id_string
            
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination(self, socket, self.destination_dealer_id_tag, self.sender_time_stamp, bson.dumps(self.sender_queue_size_snapshot_dict))        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message, sender_id_string):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            raw_message[2] = cast_bytes(raw_message[2])
            return PeerHeartBeatMessage(None, raw_message[1], bson.loads(raw_message[2]), sender_id_string)
        except:
            raise ExceptionFormatter.get_full_exception()
    
            
class PeerMasterControlDataMessage(BaseMessage):
    
    def __init__(self, destination_dealer_id_tag, pecking_order_list, queue_lock_owner_dict,  
                 ordered_queue_owners_dict, push_rejection_queue_name_set, accepting_data_owner_id_list, 
                 frozen_push_queue_list, frozen_pull_queue_list, sender_id_string = None):
        
        # Build base.
        super(PeerMasterControlDataMessage, self).__init__(message_types.PEER_MASTER_CONTROL_DATA) 
        
        # Transmitted data.
        self.destination_dealer_id_tag = destination_dealer_id_tag
        self.pecking_order_list = pecking_order_list
        self.queue_lock_owner_dict = queue_lock_owner_dict
        self.ordered_queue_owners_dict = ordered_queue_owners_dict
        self.push_rejection_queue_name_set = push_rejection_queue_name_set
        self.accepting_data_owner_id_list = accepting_data_owner_id_list
        self.frozen_push_queue_list = frozen_push_queue_list
        self.frozen_pull_queue_list = frozen_pull_queue_list
        
        # Internal data.
        self.sender_id_string = sender_id_string
    
            
    def dump(self, include_destination_tag = True):
        """
        Dumps the message into a format in which it can be recreated via the "load" method.
        """
        
        try:
            
            dump_dict = dict()
            if include_destination_tag == True:
                dump_dict["ddit"] = self.destination_dealer_id_tag
            dump_dict["pol"] = self.pecking_order_list
            dump_dict["qlod"] = self.queue_lock_owner_dict
            dump_dict["oqod"] = self.ordered_queue_owners_dict
            dump_dict["prqns"] = list(self.push_rejection_queue_name_set)
            dump_dict["adol"] = self.accepting_data_owner_id_list
            dump_dict["fpush"] = self.frozen_push_queue_list
            dump_dict["fpull"] = self.frozen_pull_queue_list
            return bson.dumps(dump_dict)     
        
        except:            
            
            raise ExceptionFormatter.get_full_exception()
            
            
    @staticmethod
    def load(dumped_string):
        """
        Returns an instance object of this class built from data which was created in the "dump" method.
        """

        dumped_string = cast_bytes(dumped_string)
        dump_dict = bson.loads(dumped_string)
        destination_dealer_id_tag = dump_dict.get("ddit", None)
        pecking_order_list = dump_dict["pol"]
        queue_lock_owner_dict = dump_dict["qlod"]
        ordered_queue_owners_dict = dump_dict["oqod"]
        push_rejection_queue_name_set = set(dump_dict["prqns"])
        accepting_data_owner_id_list = dump_dict["adol"]
        frozen_push_queue_list = dump_dict["fpush"]
        frozen_pull_queue_list = dump_dict["fpull"]
        return PeerMasterControlDataMessage(destination_dealer_id_tag, pecking_order_list, queue_lock_owner_dict, ordered_queue_owners_dict, 
                                            push_rejection_queue_name_set, accepting_data_owner_id_list,
                                            frozen_push_queue_list, frozen_pull_queue_list)
    
    
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination(self, socket, self.destination_dealer_id_tag, self.dump(False))
        except:            
            raise ExceptionFormatter.get_full_exception()
            
            
    @staticmethod
    def create_from_received(raw_message, sender_id_string):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            message = PeerMasterControlDataMessage.load(raw_message[1])
            message.sender_id_string = sender_id_string
            return message
        except:
            raise ExceptionFormatter.get_full_exception()
    
            
class PeerMasterSetupDataMessage(BaseMessage):
    
    def __init__(self, destination_dealer_id_tag, exchange_wrapper_list, queue_wrapper_list, sender_id_string = None):
        
        # Build base.
        super(PeerMasterSetupDataMessage, self).__init__(message_types.PEER_MASTER_SETUP_DATA) 
        
        # Transmitted data.
        self.destination_dealer_id_tag = destination_dealer_id_tag
        self.exchange_wrapper_list = exchange_wrapper_list
        self.queue_wrapper_list = queue_wrapper_list
        
        # Internal data.
        self.sender_id_string = sender_id_string
    
            
    def dump(self):
        """
        Dumps the message into a format in which it can be recreated via the "load" method.
        """
        
        try:
            
            dump_dict = dict()
            dump_dict["ddit"] = self.destination_dealer_id_tag
            dump_dict["ewl"] = [ew.dump() for ew in self.exchange_wrapper_list]
            dump_dict["qwl"] = [qw.dump() for qw in self.queue_wrapper_list]
            return bson.dumps(dump_dict)     
        
        except:            
            
            raise ExceptionFormatter.get_full_exception()
            
            
    @staticmethod
    def load(dumped_string):
        """
        Returns an instance object of this class built from data which was created in the "dump" method.
        """

        dumped_string = cast_bytes(dumped_string)
        dump_dict = bson.loads(dumped_string)
        destination_dealer_id_tag = dump_dict["ddit"]
        exchange_wrapper_list = [ExchangeWrapper.load(dew) for dew in dump_dict["ewl"]]
        queue_wrapper_list = [DataQueueWrapper.load(dqw) for dqw in dump_dict["qwl"]]
        return PeerMasterSetupDataMessage(destination_dealer_id_tag, exchange_wrapper_list, queue_wrapper_list)
    
    
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination(self, socket, self.destination_dealer_id_tag, self.dump())
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message, sender_id_string):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            message = PeerMasterSetupDataMessage.load(raw_message[1])
            message.sender_id_string = sender_id_string
            return message
        except:
            raise ExceptionFormatter.get_full_exception()
            
                    
class PeerOnlineHandshakeReplyMessage(BaseMessage):
    
    def __init__(self, reply_id_tag, settings_dict, sender_dealer_id_tag, 
                 sender_master_flag, master_setup_data_message, master_control_data_message, master_synchronization_failure_flag,
                 ping_back_success_flag):
        
        # Build base.
        super(PeerOnlineHandshakeReplyMessage, self).__init__(message_types.PEER_ONLINE_HANDSHAKE_REPLY)
        
        # Transmitted data.
        self.reply_id_tag = reply_id_tag
        self.settings_dict = settings_dict
        self.sender_dealer_id_tag = sender_dealer_id_tag
        self.sender_master_flag = sender_master_flag
        self.master_setup_data_message = master_setup_data_message
        self.master_control_data_message = master_control_data_message
        self.master_synchronization_failure_flag = master_synchronization_failure_flag
        self.ping_back_success_flag = ping_back_success_flag
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            if self.master_setup_data_message != None:
                master_setup_data_message = self.master_setup_data_message.dump()
            else:
                master_setup_data_message = ""
                
            if self.master_control_data_message != None:
                master_control_data_message = self.master_control_data_message.dump()
            else:
                master_control_data_message = ""
                
                
            BaseMessage._send_with_destination_and_delimiter(self, socket, self.reply_id_tag, 
                                                             bson.dumps(self.settings_dict), 
                                                             self.sender_dealer_id_tag,  
                                                             str(self.sender_master_flag),
                                                             master_setup_data_message, master_control_data_message,
                                                             str(self.master_synchronization_failure_flag),  
                                                             str(self.ping_back_success_flag))
        except:
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            if raw_message[4] != "":
                master_setup_data_message = PeerMasterSetupDataMessage.load(raw_message[4])
            else:
                master_setup_data_message = None
                
            if raw_message[5] != "":
                master_control_data_message = PeerMasterControlDataMessage.load(raw_message[5])
            else:
                master_control_data_message = None
            return PeerOnlineHandshakeReplyMessage(None, bson.loads(raw_message[1]), raw_message[2], 
                                                   BaseMessage.bool_from_string(raw_message[3]), 
                                                   master_setup_data_message, master_control_data_message,
                                                   BaseMessage.bool_from_string(raw_message[6]),
                                                   BaseMessage.bool_from_string(raw_message[7]))
        except:
            raise ExceptionFormatter.get_full_exception()
            

class PeerOnlineHandshakeRequestMessage(BaseMessage): 
    
    def __init__(self, settings_dict, sender_dealer_id_tag, receiver_dealer_id_tag = None):
        
        # Build base.
        super(PeerOnlineHandshakeRequestMessage, self).__init__(message_types.PEER_ONLINE_HANDSHAKE_REQUEST)
        
        # Transmitted data.
        self.settings_dict = settings_dict
        self.sender_dealer_id_tag = sender_dealer_id_tag
        
        # Internal data.
        self.receiver_dealer_id_tag = receiver_dealer_id_tag
        self.sending_thread_name = None
            
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, bson.dumps(self.settings_dict), self.sender_dealer_id_tag)        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message, qm_dealer_id_tag):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return PeerOnlineHandshakeRequestMessage(bson.loads(raw_message[1]), raw_message[2], qm_dealer_id_tag)
        except:
            raise ExceptionFormatter.get_full_exception()
            
            
class PeerOfflineMessage(BaseMessage): 
    
    def __init__(self, destination_dealer_id_tag, sender_id_string = None):
        
        # Build base.
        super(PeerOfflineMessage, self).__init__(message_types.PEER_OFFLINE)
        
        # Transmitted data.
        self.destination_dealer_id_tag = destination_dealer_id_tag
        
        # Internal data.
        self.sender_id_string = sender_id_string
            
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination(self, socket, self.destination_dealer_id_tag)        
        except:            
            raise ExceptionFormatter.get_full_exception()
    
            
    @staticmethod
    def create_from_received(raw_message, sender_id_string):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return PeerOfflineMessage(None, sender_id_string)
        except:
            raise ExceptionFormatter.get_full_exception()
        

class PeerRequestMasterDataMessage(BaseMessage):
    
    def __init__(self, destination_dealer_id_tag, sender_id_string = None):
        
        # Build base.
        super(PeerRequestMasterDataMessage, self).__init__(message_types.PEER_REQUEST_MASTER_DATA)
        
        # Store data.
        self.destination_dealer_id_tag = destination_dealer_id_tag
        
        # Internal data.
        self.sender_id_string = sender_id_string
                
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination(self, socket, self.destination_dealer_id_tag)
        except:            
            raise ExceptionFormatter.get_full_exception()
            
    @staticmethod
    def create_from_received(raw_message, sender_id_string):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return PeerRequestMasterDataMessage(None, sender_id_string)
        except:
            raise ExceptionFormatter.get_full_exception()
