from afqueue.common.zmq_utilities import ZmqUtilities
try:
    import zmq
except ImportError:
    # Jython compatibilty using jzmq
    from org.zeromq import ZMQ as zmq

class BaseMessage(object):
    
    def __init__(self, message_type):
        self._type = message_type
        
        
    def get_primary_type(self):        
        return self._type[0]
            
        
    def get_type(self):        
        return self._type
    
    
    @staticmethod
    def bool_from_string(value):
        if value == "True":
            return True
        return False
    
        
    @staticmethod
    def get_raw_message_primary_type(raw_message):
        return raw_message[0][0]


    @staticmethod
    def get_raw_message_type(raw_message):
        return raw_message[0]


    @staticmethod
    def get_raw_message_type_delimited(raw_message):
        return raw_message[2]


    def _send_wrapped(self, socket, *data_arguments):
        if len(data_arguments) > 0:
            ZmqUtilities.send_data(socket, self._type, zmq.SNDMORE)

            for data in data_arguments[:-1]:
                ZmqUtilities.send_data(socket, data, zmq.SNDMORE)
            ZmqUtilities.send_data(socket, data_arguments[-1])
        else:
            ZmqUtilities.send_data(socket, self._type)


    def _send(self, socket, *data_arguments):
        self._send_wrapped(socket, *data_arguments)
        
        
    def _send_with_destination(self, socket, destination, *data_arguments):
        ZmqUtilities.send_data(socket, destination, zmq.SNDMORE)
        self._send_wrapped(socket, *data_arguments)
        
        
    def _send_with_destination_and_delimiter(self, socket, destination, *data_arguments):
        ZmqUtilities.send_data(socket, destination, zmq.SNDMORE)
        socket.send(b"", zmq.SNDMORE)
        self._send_wrapped(socket, *data_arguments)
        
        
    def __str__(self):
        
        return str(self.__dict__)
