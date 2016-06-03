from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport

class ClientExchange(object):    
    
    def __init__(self, name, exchange_type):
        
        self.name = name
        self.type = exchange_type


    def __str__(self):
        
        return "Name: {0}; Type: {1}".format(self.name, self.type)
    
        
    @staticmethod
    def create_network_tuple_list(client_exchange_list):
        """
        Converts a list of objects of this class type to a list of tuples ready for network transmission.
        """
        
        try:
            
            # Setup the queue list as a list of network tuples.
            network_tuple_list = list()
            for client_exchange in client_exchange_list:
                network_tuple_list.append((client_exchange.name, client_exchange.type))
                
            # Return the resulting list.
            return network_tuple_list
        
        except:
            
            raise ExceptionFormatter.get_full_exception()

    
    @staticmethod
    def create_client_exchange_list(network_tuple_list):
        """
        Converts a list of tuples created for network transmission back into a list of objects of this class type.
        """
        
        try:
            
            # Setup the queue list as a list of network tuples.
            client_exchange_list = list()
            for network_tuple in network_tuple_list:
                client_exchange_list.append(ClientExchange(network_tuple[0], network_tuple[1]))
                                                        
            # Return the resulting list.
            return client_exchange_list
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
