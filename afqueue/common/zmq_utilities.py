from . import client_exchange_types


class ZmqUtilities:

    @staticmethod
    def get_socket_connection_string(ip_address, port):
        return "tcp://{0}:{1}".format(ip_address, port)

    
    @staticmethod
    def get_socket_bind_string(port):
        return "tcp://*:{0}".format(port)

    
    @staticmethod
    def get_dealer_id_tag(id_string, current_time_stamp):
        return "{0}:{1}".format(id_string, current_time_stamp)


    @staticmethod
    def convert_exchange_type(exchange_type):
        if exchange_type == "topic":
            return client_exchange_types.TOPIC 
        elif exchange_type == "direct":
            return client_exchange_types.DIRECT 
        elif exchange_type == "fanout":
            return client_exchange_types.FANOUT
        else:
            raise Exception("Unsupported exchange type, %s" % exchange_type)


    @staticmethod
    def get_full_storage_key(store_key, location_key):
        return "{0}::{1}".format(store_key, location_key)


    @staticmethod
    def get_store_and_location_keys(full_store_key):
        return full_store_key.split("::")


    @staticmethod
    def send_data(socket, *message):
        try:
            socket.send(*message)
        except TypeError:
            socket.send_string(*message)
    
    
    
                        
