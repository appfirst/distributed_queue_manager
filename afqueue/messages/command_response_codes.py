# Messages which should signal success / notification should be of type 0-499
COMMAND_RECEIVED = "100"

# Messages which should signal an error should be of type 500+
COMMAND_FAILED = "500"
COMMAND_UNKNOWN = "501" 
MALFORMED_MESSAGE = "502"
                
                
def convert_response_code_to_string(response):
    
    if response == COMMAND_RECEIVED:
        return "Command received"
    elif response == COMMAND_FAILED:
        return "Command failed"
    elif response == COMMAND_UNKNOWN:
        return "Command not recognized"
    elif response == MALFORMED_MESSAGE:
        return "CRITICAL: Malformed message received" 
    else:
        return "Unknown response type could not be converted"