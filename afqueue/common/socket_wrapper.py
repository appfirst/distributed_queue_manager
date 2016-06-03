from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.encoding_utilities import cast_string

class SocketWrapper(object):
    
    @staticmethod
    def pull_message(socket):
        """
        Pulls a message off the socket, if one exists, in its entirety. 
        Returns a list of message components.
        """
        try:
            message = socket.recv_multipart()
            return_message = []
            for element in message:
                try:
                    return_message.append(cast_string(element))
                except:
                    return_message.append(element)

            return return_message

        except:
            raise ExceptionFormatter.get_full_exception()
        
        
        
