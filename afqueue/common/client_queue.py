#from afqueue.messages.base_message import BaseMessage #@UnresolvedImport
from afqueue.common import client_queue_types #@UnresolvedImport

class ClientQueue(object):    
    
    def __init__(self, name, exchange_name, routing_key = "", queue_type = client_queue_types.DISTRIBUTED):
        
        self.name = name
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.type = queue_type


    def __str__(self):
        
        return "Name: {0}; Exchange: {1}; RK: {2}; Type: {3}".format(self.name, self.exchange_name, self.routing_key, self.type)
    
    
    def is_distributed(self):
        """
        Returns True if the queue is an distributed queue.
        """
        
        if self.type == client_queue_types.DISTRIBUTED:
            return True
        
        return False
    
    
    def is_ordered(self):
        """
        Returns True if the queue is an ordered queue.
        """
        
        if self.type == client_queue_types.ORDERED:
            return True
        
        return False

    
    @staticmethod
    def create_network_tuple_list(client_queue_list):
        """
        Converts a list of objects of this class type to a list of tuples ready for network transmission.
        """
        
        # Setup the queue list as a list of network tuples.
        network_tuple_list = list()
        for client_queue in client_queue_list:
            network_tuple_list.append((client_queue.name, client_queue.exchange_name, client_queue.routing_key, client_queue.type))
            
        # Return the resulting list.
        return network_tuple_list
    
    
    @staticmethod
    def create_client_queue_list(network_tuple_list):
        """
        Converts a list of tuples created for network transmission back into a list of objects of this class type.
        """
        
        # Setup the queue list as a list of network tuples.
        client_queue_list = list()
        for network_tuple in network_tuple_list:
            client_queue_list.append(ClientQueue(network_tuple[0], network_tuple[1], network_tuple[2], network_tuple[3]))
            
        # Return the resulting list.
        return client_queue_list
