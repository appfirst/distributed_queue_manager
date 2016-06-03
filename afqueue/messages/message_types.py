# Primary Message Types
COMMAND = "0"
SYSTEM = "1"
PEER = "2"
CLIENT = "4"

# Command based messages.
COMMAND_SHUT_DOWN_REQUEST = "001"
COMMAND_SET_MASTER_REQUEST = "002"
COMMAND_REMOTE_CONNECT_REQUEST = "010"
COMMAND_REMOTE_DISCONNECT_REQUEST = "011"
COMMAND_REMOTE_CONNECT_PIKA_REQUEST = "012"
COMMAND_REMOTE_DISCONNECT_PIKA_REQUEST = "013"
COMMAND_REPLY = "020"
COMMAND_GET_QUEUE_SIZE_REQUEST = "021"
COMMAND_GET_QUEUE_SIZE_REPLY_MESSAGE = "022"
COMMAND_PURGE_QUEUES_REQUEST = "023"
COMMAND_DELETE_QUEUES_REQUEST = "024"
COMMAND_UNLOCK_QUEUE_REQUEST = "025"
COMMAND_FREEZE_QUEUE_REQUEST = "026"
COMMAND_LIST_QUEUES_REQUEST = "030"
COMMAND_LIST_QUEUES_REPLY = "031"
COMMAND_ADD_WORKERS_REQUEST = "040"
COMMAND_REMOVE_WORKERS_REQUEST = "041"
COMMAND_GET_PECKING_ORDER_REQUEST = "050"
COMMAND_GET_PECKING_ORDER_REPLY = "051"
COMMAND_GET_SETUP_DATA_REQUEST = "052"
COMMAND_GET_SETUP_DATA_REPLY_MESSAGE = "053"
COMMAND_GET_STATISTICS_REQUEST = "060"
COMMAND_GET_STATISTICS_REPLY = "061"

# System based messages (internal).
SYSTEM_NOTIFICATION_MESSAGE = "100"
SYSTEM_ERROR_MESSAGE = "101"
SYSTEM_STATS_MESSAGE = "102"
SYSTEM_THREAD_STATE = "110"
SYSTEM_STOP_THREAD = "111"
SYSTEM_SOCKET_STATE = "112"
SYSTEM_PEER_CONNECTION_UDPATE = "120"
SYSTEM_CONNECT_PEER_REQUEST_SOCKET = "130"
SYSTEM_DISCONNECT_PEER_REQUEST_SOCKET = "131"
SYSTEM_BRIDGE_WORKER_TIMED_OUT = "170"
SYSTEM_ORDERED_QUEUE_OWNERS_EXHAUSTED = "171"
SYSTEM_UPDATE_SHARED_MEMORY_CONNECTIONS = "181"
SYSTEM_SET_PQM_QUEUE_ACCESS_DATA = "182"
SYSTEM_DATA_WORKER_STATUS_REPORT = "190"
SYSTEM_UPDATE_DATA_WORKER_CONTROL_DATA = "191"
SYSTEM_UPDATE_DATA_WORKER_SETUP_DATA = "192"
SYSTEM_UPDATE_QM_QUEUE_SIZE_DICTIONARY = "195"
SYSTEM_UPDATE_PQM_QUEUE_SIZE_DICTIONARIES = "196"    
SYSTEM_PUSH_LOCAL_QUEUE_DATA = "197"    
        
# Peer based messages (networked).
PEER_ONLINE_HANDSHAKE_REQUEST = "200"
PEER_ONLINE_HANDSHAKE_REPLY = "201"
PEER_OFFLINE = "202"
PEER_HEART_BEAT = "210"
PEER_HEART_BEAT_FAILURE = "211"
PEER_DISCONNECTING_DUE_TO_HEART_BEAT_FAILURE = "212"
PEER_CLIENT_DECLARE_EXCHANGES_REQUEST = "230"
PEER_CLIENT_DECLARE_QUEUES_REQUEST = "231"
PEER_CLIENT_DELETE_QUEUES_REQUEST = "232"
PEER_CLIENT_LOCK_QUEUES_REQUEST = "233"
PEER_CLIENT_UNLOCK_QUEUES_REQUEST = "234"
PEER_MASTER_SETUP_DATA = "240"
PEER_MASTER_CONTROL_DATA = "241"
PEER_REQUEST_MASTER_DATA = "242"
PEER_ORDERED_QUEUES_OWNERS_EXHAUSTED = "250"
PEER_FORWARDED_COMMAND_MESSAGE = "260"


# Client based messages (networked).
CLIENT_DECLARE_EXCHANGES_REQUEST = "400"
CLIENT_DECLARE_EXCHANGES_REPLY = "401"
CLIENT_DECLARE_QUEUES_REQUEST = "402"
CLIENT_DECLARE_QUEUES_REPLY = "403"
CLIENT_DELETE_QUEUES_REQUEST = "404"
CLIENT_DELETE_QUEUES_REPLY = "405"
CLIENT_DATA_PUSH_REQUEST = "410"
CLIENT_DATA_PUSH_REPLY = "411"
CLIENT_DATA_PULL_REQUEST = "412"
CLIENT_DATA_PULL_REPLY = "413"
CLIENT_REQUEUE_DATA_REQUEST = "414"
CLIENT_REQUEUE_DATA_REPLY = "415"
CLIENT_LOCK_QUEUES_REQUEST = "420"
CLIENT_LOCK_QUEUES_REPLY = "421"
CLIENT_UNLOCK_QUEUES_REQUEST = "422"
CLIENT_UNLOCK_QUEUES_REPLY = "423"
CLIENT_DATA_STORE_REQUEST = "430"
CLIENT_DATA_STORE_REPLY = "431"
CLIENT_DATA_RETRIEVE_REQUEST = "432"
CLIENT_DATA_RETRIEVE_REPLY = "433"
CLIENT_GET_DATA_STORES_REQUEST = "434"
CLIENT_GET_DATA_STORES_REPLY = "435"
CLIENT_GET_PECKING_ORDER_REQUEST = "436"
CLIENT_GET_PECKING_ORDER_REPLY = "437"
