from afqueue.messages.base_message import BaseMessage #@UnresolvedImport

class ClientQueueLock(object):    
    
    def __init__(self, name, force_unlock_flag = False):
        
        # Assign.
        self.name = name
        self.force_unlock_flag = force_unlock_flag
        self.process_id_string = None


    def __str__(self):
        
        return "Name: {0}; PID: {1}; Force Unlock: {2}".format(self.name, self.process_id_string, self.force_unlock_flag)
    
    
    @staticmethod
    def create_network_tuple_list(client_queue_lock_list):
        """
        Converts a list of objects of this class type to a list of tuples ready for network transmission.
        """
        
        # Setup the queue list as a list of network tuples.
        network_tuple_list = list()
        for client_queue_lock in client_queue_lock_list:
            network_tuple_list.append((client_queue_lock.name, client_queue_lock.process_id_string, str(client_queue_lock.force_unlock_flag)))
            
        # Return the resulting list.
        return network_tuple_list

    
    @staticmethod
    def create_client_queue_lock_list(network_tuple_list):
        """
        Converts a list of tuples created for network transmission back into a list of objects of this class type.
        """
        
        # Setup the queue list as a list of network tuples.
        client_queue_lock_list = list()
        for network_tuple in network_tuple_list:
            client_queue_lock = ClientQueueLock(network_tuple[0], BaseMessage.bool_from_string(network_tuple[2]))
            client_queue_lock.process_id_string = network_tuple[1]
            client_queue_lock_list.append(client_queue_lock)
            
        # Return the resulting list.
        return client_queue_lock_list
