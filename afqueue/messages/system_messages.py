from afqueue.messages.base_message import BaseMessage #@UnresolvedImport
from afqueue.messages import message_types #@UnresolvedImport


class SystemOrderedQueueExhaustedOwnersMessage(BaseMessage): 
    
    def __init__(self, thread_name, queue_name, exhausted_owner_id_string_list):
        
        # Build base.
        super(SystemOrderedQueueExhaustedOwnersMessage, self).__init__(message_types.SYSTEM_ORDERED_QUEUE_OWNERS_EXHAUSTED)
        
        # Store data.
        self.thread_name = thread_name
        self.queue_name = queue_name
        self.exhausted_owner_id_string_list = exhausted_owner_id_string_list


class SystemPushLocalQueueData(BaseMessage): 
    
    def __init__(self, queue_push_dict):
        
        # Build base.
        super(SystemPushLocalQueueData, self).__init__(message_types.SYSTEM_PUSH_LOCAL_QUEUE_DATA)
        
        # Store data.
        self.queue_push_dict = queue_push_dict


class SystemUpdateQmQueueSizeDictionaryMessage(BaseMessage): 
    
    def __init__(self, queue_size_dictionary):
        
        # Build base.
        super(SystemUpdateQmQueueSizeDictionaryMessage, self).__init__(message_types.SYSTEM_UPDATE_QM_QUEUE_SIZE_DICTIONARY)
        
        # Store data.
        self.queue_size_dictionary = queue_size_dictionary


class SystemUpdatePqmQueueSizeDictionariesMessage(BaseMessage): 
    
    def __init__(self, queue_size_dictionaries):
        
        # Build base.
        super(SystemUpdatePqmQueueSizeDictionariesMessage, self).__init__(message_types.SYSTEM_UPDATE_PQM_QUEUE_SIZE_DICTIONARIES)
        
        # Store data.
        self.queue_size_dictionaries = queue_size_dictionaries


class SystemUpdateDataWorkerControlDataMessage(BaseMessage): 
    
    def __init__(self, pecking_order_list, queue_lock_owner_dict,  
                 ordered_queue_owners_dict, push_rejection_queue_name_set, routing_key_rejection_list, accepting_data_owner_id_list,
                 frozen_push_queue_list, frozen_pull_queue_list):
        
        # Build base.
        super(SystemUpdateDataWorkerControlDataMessage, self).__init__(message_types.SYSTEM_UPDATE_DATA_WORKER_CONTROL_DATA)
        
        # Store data.
        self.pecking_order_list = pecking_order_list
        self.queue_lock_owner_dict = queue_lock_owner_dict
        self.ordered_queue_owners_dict = ordered_queue_owners_dict
        self.push_rejection_queue_name_set = push_rejection_queue_name_set
        self.routing_key_rejection_list = routing_key_rejection_list
        self.accepting_data_owner_id_list = accepting_data_owner_id_list
        self.frozen_push_queue_list = frozen_push_queue_list
        self.frozen_pull_queue_list = frozen_pull_queue_list


class SystemUpdateDataWorkerSetupDataMessage(BaseMessage): 
    
    def __init__(self, exchange_wrapper_dict, data_queue_wrapper_dict):
        
        # Build base.
        super(SystemUpdateDataWorkerSetupDataMessage, self).__init__(message_types.SYSTEM_UPDATE_DATA_WORKER_SETUP_DATA)
        
        # Store data.
        self.exchange_wrapper_dict = exchange_wrapper_dict
        self.data_queue_wrapper_dict = data_queue_wrapper_dict


class SystemUpdateSharedMemoryConnectionsMessage(BaseMessage): 
    
    def __init__(self, shared_memory_connection_string_dict):
        
        # Build base.
        super(SystemUpdateSharedMemoryConnectionsMessage, self).__init__(message_types.SYSTEM_UPDATE_SHARED_MEMORY_CONNECTIONS)
        
        # Store data.
        self.shared_memory_connection_string_dict = shared_memory_connection_string_dict


class SystemSetPqmQueueAccessDataMessage(BaseMessage): 
    
    def __init__(self, pqm_queue_access_dictionary):
        
        # Build base.
        super(SystemSetPqmQueueAccessDataMessage, self).__init__(message_types.SYSTEM_SET_PQM_QUEUE_ACCESS_DATA)
        
        # Store data.
        self.pqm_queue_access_dictionary = pqm_queue_access_dictionary


class SystemDataWorkerStatusReportMessage(BaseMessage): 
    
    def __init__(self, thread_name, queue_data_pushed_status_report, queue_data_popped_status_report, queue_data_requeued_status_report,
                 synchronization_status_report, updated_remote_shared_memory_connections_status_report):
        
        # Build base.
        super(SystemDataWorkerStatusReportMessage, self).__init__(message_types.SYSTEM_DATA_WORKER_STATUS_REPORT)
        
        # Store data.
        self.thread_name = thread_name
        self.queue_data_pushed_status_report = queue_data_pushed_status_report
        self.queue_data_popped_status_report = queue_data_popped_status_report
        self.queue_data_requeued_status_report = queue_data_requeued_status_report
        self.synchronization_status_report = synchronization_status_report
        self.updated_remote_shared_memory_connections_status_report = updated_remote_shared_memory_connections_status_report
        

class SystemPeerConnectionUpdateMessage(BaseMessage): 
    
    def __init__(self, connect_flag, peer_id_string, peer_socket_connection_string, dealer_id_string = None):
        
        # Build base.
        super(SystemPeerConnectionUpdateMessage, self).__init__(message_types.SYSTEM_PEER_CONNECTION_UDPATE)
        
        # Store data.
        self.connect_flag = connect_flag
        self.peer_id_string = peer_id_string
        self.peer_socket_connection_string = peer_socket_connection_string
        self.dealer_id_string = dealer_id_string


class SystemStopThreadMessage(BaseMessage): 
    
    def __init__(self):
        
        # Build base.
        super(SystemStopThreadMessage, self).__init__(message_types.SYSTEM_STOP_THREAD)
        

class SystemConnectPeerRequestSocketMessage(BaseMessage): 
    
    def __init__(self, peer_queue_manager_id_string, connection_string):
        
        # Build base.
        super(SystemConnectPeerRequestSocketMessage, self).__init__(message_types.SYSTEM_CONNECT_PEER_REQUEST_SOCKET)
        
        # Store data.
        self.peer_queue_manager_id_string = peer_queue_manager_id_string
        self.connection_string = connection_string


class SystemDisconnectPeerRequestSocketMessage(BaseMessage): 
    
    def __init__(self, peer_queue_manager_id_string):
        
        # Build base.
        super(SystemDisconnectPeerRequestSocketMessage, self).__init__(message_types.SYSTEM_DISCONNECT_PEER_REQUEST_SOCKET)
        
        # Store data.
        self.peer_queue_manager_id_string = peer_queue_manager_id_string


class SystemBridgeWorkerTimedOut(BaseMessage): 
    
    def __init__(self, thread_name, notification_string):
        
        # Build base.
        super(SystemBridgeWorkerTimedOut, self).__init__(message_types.SYSTEM_BRIDGE_WORKER_TIMED_OUT)
        
        # Store data.
        self.thread_name = thread_name
        self.notification_string = notification_string


class SystemStatsMessage(BaseMessage): 
    
    def __init__(self, thread_name, stat_type, stat_value):
        
        # Build base.
        super(SystemStatsMessage, self).__init__(message_types.SYSTEM_STATS_MESSAGE)
        
        # Store data.
        self.thread_name = thread_name
        self.stat_type = stat_type
        self.stat_value = stat_value


class SystemThreadStateMessage(BaseMessage): 
    
    def __init__(self, thread_name, started_flag, process_id = -1, notification_string = None):
        
        # Build base.
        super(SystemThreadStateMessage, self).__init__(message_types.SYSTEM_THREAD_STATE)
        
        # Store data.
        self.thread_name = thread_name
        self.started_flag = started_flag
        self.process_id = process_id
        self.notification_string = notification_string


class SystemSocketStateMessage(BaseMessage): 
    
    def __init__(self, thread_name, opened_flag, notification_string = None):
        
        # Build base.
        super(SystemSocketStateMessage, self).__init__(message_types.SYSTEM_SOCKET_STATE)
        
        # Store data.
        self.thread_name = thread_name
        self.opened_flag = opened_flag
        self.notification_string = notification_string


class SystemErrorMessage(BaseMessage): 
    
    def __init__(self, thread_name, error_message):
        
        # Build base.
        super(SystemErrorMessage, self).__init__(message_types.SYSTEM_ERROR_MESSAGE)
        
        # Store data.
        self.thread_name = thread_name
        self.message = error_message


class SystemNotificationMessage(BaseMessage): 
    
    def __init__(self, thread_name, notification_message):
        
        # Build base.
        super(SystemNotificationMessage, self).__init__(message_types.SYSTEM_NOTIFICATION_MESSAGE)
        
        # Store data.
        self.thread_name = thread_name
        self.message = notification_message

        
