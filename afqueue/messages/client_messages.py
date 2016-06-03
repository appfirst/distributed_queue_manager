from afqueue.common.encoding_utilities import cast_bytes
from afqueue.messages.base_message import BaseMessage #@UnresolvedImport
from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.client_exchange import ClientExchange #@UnresolvedImport
from afqueue.common.client_queue import ClientQueue #@UnresolvedImport
from afqueue.common.client_queue_lock import ClientQueueLock #@UnresolvedImport
from afqueue.messages import message_types #@UnresolvedImport

import simplejson as json #@UnresolvedImport
import bson #@UnresolvedImport


class ReplyTranslationException(Exception):
    """
    Denotes an exception while trying to translate a reply from a QM.
    """
    pass


class ClientGetPeckingOrderRequestMessage(BaseMessage): 
    
    def __init__(self):
        
        # Build base.
        super(ClientGetPeckingOrderRequestMessage, self).__init__(message_types.CLIENT_GET_PECKING_ORDER_REQUEST)
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket)   
        except:         
            raise ExceptionFormatter.get_full_exception()
             
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return ClientGetPeckingOrderRequestMessage()        
        except:                        
            return None
        

class ClientGetPeckingOrderReplyMessage(BaseMessage): 
    
    def __init__(self, reply_id_tag, pecking_order_list, response_code):
        
        # Build base.
        super(ClientGetPeckingOrderReplyMessage, self).__init__(message_types.CLIENT_GET_DATA_STORES_REPLY)
        
        # Store data.
        self.reply_id_tag = reply_id_tag
        self.pecking_order_list = pecking_order_list
        self.response_code = response_code
            
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination_and_delimiter(self, socket, self.reply_id_tag, 
                                                             json.dumps(self.pecking_order_list), 
                                                             self.response_code)    
        except:
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            # Break the data tuples into a data and routing key dictionary.
            return ClientGetPeckingOrderReplyMessage(None, json.loads(raw_message[1]), raw_message[2])
        except:
            raise ReplyTranslationException(ExceptionFormatter.get_message()) 


class ClientGetDataStoresRequestMessage(BaseMessage): 
    
    def __init__(self):
        
        # Build base.
        super(ClientGetDataStoresRequestMessage, self).__init__(message_types.CLIENT_GET_DATA_STORES_REQUEST)
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket)   
        except:         
            raise ExceptionFormatter.get_full_exception()
             
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return ClientGetDataStoresRequestMessage()        
        except:                        
            return None
        

class ClientGetDataStoresReplyMessage(BaseMessage): 
    
    def __init__(self, reply_id_tag, accepting_data_store_list, rejecting_data_store_list, response_code):
        
        # Build base.
        super(ClientGetDataStoresReplyMessage, self).__init__(message_types.CLIENT_GET_DATA_STORES_REPLY)
        
        # Store data.
        self.reply_id_tag = reply_id_tag
        self.accepting_data_store_list = accepting_data_store_list
        self.rejecting_data_store_list = rejecting_data_store_list
        self.response_code = response_code
            
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination_and_delimiter(self, socket, self.reply_id_tag, 
                                                             json.dumps(self.accepting_data_store_list), json.dumps(self.rejecting_data_store_list), 
                                                             self.response_code)    
        except:
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            # Break the data tuples into a data and routing key dictionary.
            return ClientGetDataStoresReplyMessage(None, json.loads(raw_message[1]), json.loads(raw_message[2]), raw_message[3])
        except:
            raise ReplyTranslationException(ExceptionFormatter.get_message())  
        
        
class ClientDataStoreRequestMessage(BaseMessage): 
    
    def __init__(self, store_key_to_data_dict, expire_seconds):
        
        # Build base.
        super(ClientDataStoreRequestMessage, self).__init__(message_types.CLIENT_DATA_STORE_REQUEST)
        
        # Store data.
        self.store_key_to_data_dict = store_key_to_data_dict
        if expire_seconds == None:
            expire_seconds = 0
        if expire_seconds < 0:
            expire_seconds = -1
        self.expire_seconds = expire_seconds
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, bson.dumps(self.store_key_to_data_dict), self.expire_seconds)   
        except:         
            raise ExceptionFormatter.get_full_exception()
             
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return ClientDataStoreRequestMessage(bson.loads(raw_message[1]), raw_message[2])        
        except:                        
            return None
        

class ClientDataStoreReplyMessage(BaseMessage): 
    
    def __init__(self, reply_id_tag, location_key, response_code):
        
        # Build base.
        super(ClientDataStoreReplyMessage, self).__init__(message_types.CLIENT_DATA_STORE_REPLY)
        
        # Store data.
        self.reply_id_tag = reply_id_tag
        self.location_key = location_key
        self.response_code = response_code
            
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination_and_delimiter(self, socket, self.reply_id_tag, self.location_key, self.response_code)    
        except:
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            # Break the data tuples into a data and routing key dictionary.
            return ClientDataStoreReplyMessage(None, raw_message[1], raw_message[2])
        except:
            raise ReplyTranslationException(ExceptionFormatter.get_message())     


class ClientDataRetrieveRequestMessage(BaseMessage): 
    
    def __init__(self, full_key_get_list, full_key_delete_list):
        
        # Build base.
        super(ClientDataRetrieveRequestMessage, self).__init__(message_types.CLIENT_DATA_RETRIEVE_REQUEST)
        
        # Store data.
        self.full_key_get_list = full_key_get_list
        self.full_key_delete_list = full_key_delete_list
        

    def get_queue_name_list(self):
        """
        Returns a copy of all queue names the request is working with.
        """
        
        return json.loads(self.client_data_request_key_list_dumped)
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, json.dumps(self.full_key_get_list), json.dumps(self.full_key_delete_list))   
        except:         
            raise ExceptionFormatter.get_full_exception()
             
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:            
            return ClientDataRetrieveRequestMessage(json.loads(raw_message[1]), json.loads(raw_message[2]))        
        except:                        
            return None
        

class ClientDataRetrieveReplyMessage(BaseMessage): 
    
    def __init__(self, reply_id_tag, full_key_to_data_dict, response_code):
        
        # Build base.
        super(ClientDataRetrieveReplyMessage, self).__init__(message_types.CLIENT_DATA_RETRIEVE_REPLY)
        
        # Store data.
        self.reply_id_tag = reply_id_tag
        self.full_key_to_data_dict = full_key_to_data_dict
        self.response_code = response_code
            
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination_and_delimiter(self, socket, self.reply_id_tag, bson.dumps(self.full_key_to_data_dict), self.response_code)    
        except:
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            # Break the data tuples into a data and routing key dictionary.
            return ClientDataRetrieveReplyMessage(None, bson.loads(raw_message[1]), raw_message[2])
        except:
            raise ReplyTranslationException(ExceptionFormatter.get_message())           
        

class ClientDataPullRequestMessage(BaseMessage): 
    
    def __init__(self, client_data_request_key_list_dumped, request_count=10):
        
        # Build base.
        super(ClientDataPullRequestMessage, self).__init__(message_types.CLIENT_DATA_PULL_REQUEST)
        
        # Store data.
        self.client_data_request_key_list_dumped = client_data_request_key_list_dumped
        self.request_count = request_count
        
        # Internal data (not sent across the network).
        self.queue_data_count_dict = dict()
        self.response_code_dict = dict()
        self.replied_count = 0
        

    def get_queue_name_list(self):
        """
        Returns a copy of all queue names the request is working with.
        """
        
        return json.loads(self.client_data_request_key_list_dumped)
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.client_data_request_key_list_dumped, str(self.request_count))   
        except:         
            raise ExceptionFormatter.get_full_exception()
             
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:            
            return ClientDataPullRequestMessage(raw_message[1], int(raw_message[2]))        
        except:                        
            return None


class ClientDataPullReplyMessage(BaseMessage): 
    
    def __init__(self, reply_id_tag, queue_data_pulled_dict, response_code_dict, status_message):
        
        # Build base.
        super(ClientDataPullReplyMessage, self).__init__(message_types.CLIENT_DATA_PULL_REPLY)
        
        # Store data.
        self.reply_id_tag = reply_id_tag
        self.queue_data_pulled_dict = queue_data_pulled_dict
        self.response_code_dict = response_code_dict
        self.status_message = status_message
        
        # When data is pushed, the routing key is wrapped by the client into a tuple with the data and dumped.
        # The data is never loaded back into a tuple from the queue.
        # We get this dumped tuple as a 
        self.queue_data_dict = dict()
        self.routing_key_dict = dict()
            
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination_and_delimiter(self, socket, self.reply_id_tag, bson.dumps(self.queue_data_pulled_dict), 
                                                             json.dumps(self.response_code_dict), self.status_message)    
        except:
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            # Break the data tuples into a data and routing key dictionary.
            bson_data_string = cast_bytes(raw_message[1])
            return ClientDataPullReplyMessage(None, bson.loads(bson_data_string), json.loads(raw_message[2]), raw_message[3])
        except:
            raise ReplyTranslationException(ExceptionFormatter.get_message())
        

class ClientDataPushRequestMessage(BaseMessage): 
    
    def __init__(self, exchange_name, routing_key, data, reply_id_tag = None):
        
        # Build base.
        super(ClientDataPushRequestMessage, self).__init__(message_types.CLIENT_DATA_PUSH_REQUEST)
        
        # Store data.e
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.data = data
        self.reply_id_tag = reply_id_tag
        
        # Internal data (not sent across the network).
        self.queue_size = 0
        self.queue_name = ""
        
        # We force the client to do the work of wrapping the routing key into the tuple.
        # On the receiving side (QM), we track the data as a data tuple so we don't have to unwrap it when storing in the queue.
        self.data_tuple_dumped = None  
            
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, self.exchange_name, self.routing_key, bson.dumps({self.routing_key: self.data}))
        except:
            raise ExceptionFormatter.get_full_exception()

    
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            
            message = ClientDataPushRequestMessage(raw_message[1], raw_message[2], None)
            message.data_tuple_dumped = raw_message[3]
            return message
        
        except:
                        
            return None
            

class ClientDataPushReplyMessage(BaseMessage): 
    
    def __init__(self, reply_id_tag, response_code, status_message):
        
        # Build base.
        super(ClientDataPushReplyMessage, self).__init__(message_types.CLIENT_DATA_PUSH_REPLY)
        
        # Store data.
        self.reply_id_tag = reply_id_tag
        self.response_code = response_code
        self.status_message = status_message
            
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination_and_delimiter(self, socket, self.reply_id_tag, self.response_code, self.status_message)
        except:            
            pass 
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return ClientDataPushReplyMessage(None, raw_message[1], raw_message[2])
        except:
            raise ReplyTranslationException(ExceptionFormatter.get_message() + str(raw_message))


class ClientDeclareExchangesRequestMessage(BaseMessage): 
    
    def __init__(self, client_exchange_list):
        
        # Build base.
        super(ClientDeclareExchangesRequestMessage, self).__init__(message_types.CLIENT_DECLARE_EXCHANGES_REQUEST)
        
        # Store data.
        self.client_exchange_list = client_exchange_list
                
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, json.dumps(ClientExchange.create_network_tuple_list(self.client_exchange_list)))
        except:
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:            
            return ClientDeclareExchangesRequestMessage(ClientExchange.create_client_exchange_list(json.loads(raw_message[1])))        
        except:                        
            return None


class ClientDeclareExchangesReplyMessage(BaseMessage): 
    
    def __init__(self, reply_id_tag, response_code_dict, status_message):
        
        # Build base.
        super(ClientDeclareExchangesReplyMessage, self).__init__(message_types.CLIENT_DECLARE_EXCHANGES_REPLY)
        
        # Store data.
        self.reply_id_tag = reply_id_tag
        self.response_code_dict = response_code_dict
        self.status_message = status_message
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination_and_delimiter(self, socket, self.reply_id_tag, json.dumps(self.response_code_dict), self.status_message)  
        except:
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return ClientDeclareExchangesReplyMessage(None, json.loads(raw_message[1]), raw_message[2])
        except:
            raise ReplyTranslationException(ExceptionFormatter.get_message())


class ClientDeclareQueuesRequestMessage(BaseMessage): 
    
    def __init__(self, client_queue_list):
        
        # Build base.
        super(ClientDeclareQueuesRequestMessage, self).__init__(message_types.CLIENT_DECLARE_QUEUES_REQUEST)
        
        # Store data.
        self.client_queue_list = client_queue_list
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, json.dumps(ClientQueue.create_network_tuple_list(self.client_queue_list)))
        except:
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return ClientDeclareQueuesRequestMessage(ClientQueue.create_client_queue_list(json.loads(raw_message[1])))
        except:            
            return None 


class ClientDeclareQueuesReplyMessage(BaseMessage): 
    
    def __init__(self, reply_id_tag, response_code_dict, status_message):
        
        # Build base.
        super(ClientDeclareQueuesReplyMessage, self).__init__(message_types.CLIENT_DECLARE_QUEUES_REPLY)
        
        # Store data.
        self.reply_id_tag = reply_id_tag
        self.response_code_dict = response_code_dict
        self.status_message = status_message
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination_and_delimiter(self, socket, self.reply_id_tag, json.dumps(self.response_code_dict), self.status_message)        
        except:            
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return ClientDeclareQueuesReplyMessage(None, json.loads(raw_message[1]), raw_message[2])
        except:
            raise ReplyTranslationException(ExceptionFormatter.get_message())


class ClientDeleteQueuesRequestMessage(BaseMessage): 
    
    def __init__(self, queue_name_list):
        
        # Build base.
        super(ClientDeleteQueuesRequestMessage, self).__init__(message_types.CLIENT_DELETE_QUEUES_REQUEST)
        
        # Store data.
        self.queue_name_list = queue_name_list
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, json.dumps(self.queue_name_list))
        except:         
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:
            return ClientDeleteQueuesRequestMessage(json.loads(raw_message[1]))
        except:                        
            return None
        

class ClientDeleteQueuesReplyMessage(BaseMessage): 
    
    def __init__(self, reply_id_tag, response_code_dict, status_message):
        
        # Build base.
        super(ClientDeleteQueuesReplyMessage, self).__init__(message_types.CLIENT_DELETE_QUEUES_REPLY)
        
        # Store data.
        self.reply_id_tag = reply_id_tag
        self.response_code_dict = response_code_dict
        self.status_message = status_message
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination_and_delimiter(self, socket, self.reply_id_tag, json.dumps(self.response_code_dict), self.status_message)        
        except:            
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return ClientDeleteQueuesReplyMessage(None, json.loads(raw_message[1]), raw_message[2])
        except:
            raise ReplyTranslationException(ExceptionFormatter.get_message())


class ClientLockQueuesRequestMessage(BaseMessage): 
    
    def __init__(self, client_queue_lock_list):
        
        # Build base.
        super(ClientLockQueuesRequestMessage, self).__init__(message_types.CLIENT_LOCK_QUEUES_REQUEST)
        
        # Store data.
        self.client_queue_lock_list = client_queue_lock_list
        
        # Internal data.
        self.owner_id_string = None
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, json.dumps(ClientQueueLock.create_network_tuple_list(self.client_queue_lock_list)))
        except:        
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:            
            return ClientLockQueuesRequestMessage(ClientQueueLock.create_client_queue_lock_list(json.loads(raw_message[1])))   
        except:                        
            return None


class ClientLockQueuesReplyMessage(BaseMessage): 
    
    def __init__(self, reply_id_tag, response_code_dict, status_message):
        
        # Build base.
        super(ClientLockQueuesReplyMessage, self).__init__(message_types.CLIENT_LOCK_QUEUES_REPLY)
        
        # Store data.
        self.reply_id_tag = reply_id_tag
        self.response_code_dict = response_code_dict
        self.status_message = status_message
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination_and_delimiter(self, socket, self.reply_id_tag, json.dumps(self.response_code_dict), self.status_message)        
        except:        
            raise ExceptionFormatter.get_full_exception()
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return ClientLockQueuesReplyMessage(None, json.loads(raw_message[1]), raw_message[2])
        except:
            raise ReplyTranslationException(ExceptionFormatter.get_message())
        

class ClientRequeueDataRequestMessage(BaseMessage): 
    
    def __init__(self, queue_name, routing_key, data):
        
        # Build base.
        super(ClientRequeueDataRequestMessage, self).__init__(message_types.CLIENT_REQUEUE_DATA_REQUEST)
        
        # Store data.
        self.queue_name = queue_name 
        self.routing_key = routing_key
        self.data = data 
      
        # Internal data (not sent across the network).
        self.queue_size = 0
        self.data_tuple_dumped = None
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        try:
            BaseMessage._send(self, socket, self.queue_name, self.routing_key, bson.dumps({self.routing_key: self.data}))        
        except: 
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:            
            message = ClientRequeueDataRequestMessage(raw_message[1], raw_message[2], None)
            message.data_tuple_dumped = raw_message[3]
            return message
        except:                        
            return None


class ClientRequeueDataReplyMessage(BaseMessage): 
    
    def __init__(self, reply_id_tag, response_code, status_message):
        
        # Build base.
        super(ClientRequeueDataReplyMessage, self).__init__(message_types.CLIENT_REQUEUE_DATA_REPLY)
        
        # Store data.
        self.reply_id_tag = reply_id_tag
        self.response_code = response_code
        self.status_message = status_message
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination_and_delimiter(self, socket, self.reply_id_tag, self.response_code, self.status_message)        
        except:            
            raise ExceptionFormatter.get_full_exception()
            
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return ClientRequeueDataReplyMessage(None, raw_message[1], raw_message[2])
        except:
            raise ReplyTranslationException(ExceptionFormatter.get_message())


class ClientUnlockQueuesRequestMessage(BaseMessage): 
    
    def __init__(self, queue_name_list, process_id_string):
        
        # Build base.
        super(ClientUnlockQueuesRequestMessage, self).__init__(message_types.CLIENT_UNLOCK_QUEUES_REQUEST)
        
        # Store data.
        self.queue_name_list = queue_name_list
        self.process_id_string = process_id_string
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send(self, socket, json.dumps(self.queue_name_list), self.process_id_string)        
        except:         
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        Returns None if the message fails to create.
        """
        
        try:            
            return ClientUnlockQueuesRequestMessage(json.loads(raw_message[1]), raw_message[2])  
        except:                        
            return None


class ClientUnlockQueuesReplyMessage(BaseMessage): 
    
    def __init__(self, reply_id_tag, response_code_dict, status_message):
        
        # Build base.
        super(ClientUnlockQueuesReplyMessage, self).__init__(message_types.CLIENT_UNLOCK_QUEUES_REPLY)
        
        # Store data.
        self.reply_id_tag = reply_id_tag
        self.response_code_dict = response_code_dict
        self.status_message = status_message
        
            
    def send(self, socket):
        """
        Sends the message over the socket.
        """
        
        try:
            BaseMessage._send_with_destination_and_delimiter(self, socket, self.reply_id_tag, json.dumps(self.response_code_dict), self.status_message)       
        except:       
            raise ExceptionFormatter.get_full_exception()
        
            
    @staticmethod
    def create_from_received(raw_message):
        """
        Returns a new message of this type from the raw message data.
        """
        
        try:
            return ClientUnlockQueuesReplyMessage(None, json.loads(raw_message[1]), raw_message[2])
        except:
            raise ReplyTranslationException(ExceptionFormatter.get_message())
