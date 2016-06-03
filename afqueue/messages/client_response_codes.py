# Messages which should signal success / notification should be of type 0-499
GENERIC_SUCCESS = "0"

EXCHANGE_DECLARE_SUCCESS_CREATED = "100"
EXCHANGE_DECLARE_SUCCESS_EXISTS = "101"
QUEUE_DECLARE_SUCCESS_EXISTS = "110"
QUEUE_ALREADY_EXISTS_AND_IS_ATTACHED = "111"
QUEUE_CREATED_AND_ATTACHED = "112"
QUEUE_LOCK_GRANTED = "113"
QUEUE_UNLOCKED = "114"
QUEUE_ALREADY_DOES_NOT_EXIST_AND_IS_NOT_ATTACHED = "115"
QUEUE_NO_LONGER_EXISTS_AND_IS_NOT_ATTACHED = "116"
DATA_PUSH_SUCCESS = "200"
DATA_PULL_SUCCESS = "201"
DATA_REQUEUE_SUCCESS = "200"
DATA_STORE_SUCCESS = "202"
DATA_RETRIEVE_SUCCESS = "203"
GET_DATA_STORES_SUCCESS = "204"
GET_DATA_PECKING_ORDER_SUCCESS = "205"

PIKA_DATA_PUSH_SUCCESS = "300"
PIKA_DATA_PUSH_SUCCESS_AFTER_RETRY = "301"
PIKA_DATA_PUSH_SUCCESS_LONG_SEND = "302"
        
# Messages which should signal an error should be of type 500+
OPERATION_TIMED_OUT = "500"
EXCHANGE_DOES_NOT_EXIST = "501"
EXCHANGE_DECLARE_FAILURE_TYPE_MISMATCH = "506"
QUEUE_DOES_NOT_EXIST = "503"
QUEUE_LOCK_REJECTED = "504"
QUEUE_UNLOCK_REJECTED_NOT_OWNED = "505"
QUEUE_MUST_BE_ORDERED_TO_LOCK = "507"
DIRECT_EXCHANGE_ALREADY_HAS_A_QUEUE_ATTACHED = "510"                     
EXCHANGE_TYPE_DOES_NOT_ACCEPT_ROUTING_KEYS = "511"                     
EXCHANGE_TYPE_REQUIRES_ROUTING_KEY = "512"                     
EXCHANGE_HAS_QUEUE_ATTACHED_UNDER_DIFFERENT_ROUTING_KEY = "513"
LOCKED_QUEUE_CAN_NOT_BE_DELETED = "514"
MALFORMED_MESSAGE = "515"
PIKA_DATA_PUSH_FAILED_TWICE = "800"
PUSH_TO_QUEUE_ON_REJECTION_LIST = "801"
PUSH_TO_QUEUE_FAILED = "802"
PUSH_TO_QUEUE_ON_OVERFLOW_LIST = "803"
PUSH_TO_QUEUE_ON_FROZEN_LIST = "804"
PULL_FROM_QUEUE_ON_FROZEN_LIST = "805"
DATA_STORE_FAIL_NO_FREE_SPACE = "900"
DATA_STORE_FAIL_REMOTE = "901"
DATA_RETRIEVE_FAIL_REMOTE = "902"
DATA_DELETE_FAIL_REMOTE = "903"

def convert_response_code_to_string(response):
    
    if response == OPERATION_TIMED_OUT:
        return "Operation timed out"
    elif response == DATA_PUSH_SUCCESS:
        return "Data successfully pushed"
    elif response == DATA_PULL_SUCCESS:
        return "Data successfully pulled"
    elif response == DATA_STORE_SUCCESS:
        return "Data successfully stored"
    elif response == DATA_RETRIEVE_SUCCESS:
        return "Data successfully retrieved"
    elif response == GET_DATA_STORES_SUCCESS:
        return "Data store list successfully retrieved"
    elif response == EXCHANGE_DECLARE_SUCCESS_CREATED:
        return "Exchange declared"
    elif response == EXCHANGE_DECLARE_SUCCESS_EXISTS:
        return "Exchange already exists"
    elif response == EXCHANGE_DECLARE_FAILURE_TYPE_MISMATCH:
        return "Exchange already exists but has different type"
    elif response == EXCHANGE_DOES_NOT_EXIST:
        return "Exchange does not exist"
    elif response == QUEUE_DOES_NOT_EXIST:
        return "Queue does not exist"
    elif response == QUEUE_CREATED_AND_ATTACHED:
        return "Queue created and attached to exchange"
    elif response == QUEUE_ALREADY_EXISTS_AND_IS_ATTACHED:
        return "Queue already exists and is already attached"
    elif response == QUEUE_DECLARE_SUCCESS_EXISTS:
        return "Queue exists; already attached to exchange"
    elif response == QUEUE_LOCK_GRANTED:
        return "Queue locked"
    elif response == QUEUE_UNLOCKED:
        return "Queue unlocked"    
    elif response == QUEUE_UNLOCK_REJECTED_NOT_OWNED:
        return "Queue could not be unlocked: lock not owned"    
    elif response == QUEUE_LOCK_REJECTED:
        return "Queue lock rejected"
    elif response == DIRECT_EXCHANGE_ALREADY_HAS_A_QUEUE_ATTACHED:
        return "Direct exchange already has a different queue attached"
    elif response == EXCHANGE_TYPE_DOES_NOT_ACCEPT_ROUTING_KEYS:
        return "Exchange type does not accept routing keys"
    elif response == EXCHANGE_TYPE_REQUIRES_ROUTING_KEY:
        return "Exchange type requires a routing key"
    elif response == EXCHANGE_HAS_QUEUE_ATTACHED_UNDER_DIFFERENT_ROUTING_KEY:
        return "Exchange already has queue attached under a different routing key"  
    elif response == QUEUE_ALREADY_DOES_NOT_EXIST_AND_IS_NOT_ATTACHED:
        return "Queue already does not exist and is not attached"  
    elif response == QUEUE_NO_LONGER_EXISTS_AND_IS_NOT_ATTACHED:
        return "Queue no longer exists and is not attached"    
    elif response == PIKA_DATA_PUSH_SUCCESS:
        return "Data pushed via pika successfully"
    elif response == PIKA_DATA_PUSH_SUCCESS_AFTER_RETRY:
        return "Data pushed via pika successfully after retry"
    elif response == PIKA_DATA_PUSH_SUCCESS_LONG_SEND:
        return "Data pushed via pika successfully; long send"
    elif response == PIKA_DATA_PUSH_FAILED_TWICE:
        return "Data could not be pushed via pika; two attempts made"  
    elif response == LOCKED_QUEUE_CAN_NOT_BE_DELETED:
        return "Locked queues can not be deleted; unlock and try again"  
    elif response == QUEUE_MUST_BE_ORDERED_TO_LOCK:
        return "Queue lock rejected - can only lock ordered queues"
    elif response == PUSH_TO_QUEUE_ON_REJECTION_LIST:
        return "CRITICAL: Routing key supplied can not push - contains queue on the push rejection list"
    elif response == PUSH_TO_QUEUE_ON_OVERFLOW_LIST:
        return "CRITICAL: Routing key supplied can not push - contains queue on the push overflow list"
    elif response == PUSH_TO_QUEUE_ON_FROZEN_LIST:
        return "Routing key supplied can not push - contains queue on the frozen list"
    elif response == PULL_FROM_QUEUE_ON_FROZEN_LIST:
        return "Can not pull from queue - queue is on the frozen list"
    elif response == PUSH_TO_QUEUE_FAILED:
        return "CRITICAL: Queue push failed"
    elif response == MALFORMED_MESSAGE:
        return "CRITICAL: Malformed message received"    
    elif response == DATA_STORE_FAIL_NO_FREE_SPACE:
        return "Data storage failed due to no free space in QM network"    
    elif response == DATA_STORE_FAIL_REMOTE:
        return "Data storage failed during remote request"     
    elif response == DATA_RETRIEVE_FAIL_REMOTE:
        return "Data retrieval failed during remote request"   
    elif response == DATA_DELETE_FAIL_REMOTE:
        return "Data deletion failed during remote request"    
    elif response == GENERIC_SUCCESS:
        return "Generic success response code"        
    else:
        print(response)
        return "Unknown response type could not be converted"
