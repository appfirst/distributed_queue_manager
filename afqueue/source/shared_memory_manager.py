from afqueue.common.encoding_utilities import cast_string, cast_list_of_strings
from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.data_objects.exchange_wrapper import ExchangeWrapper #@UnresolvedImport
from afqueue.data_objects.data_queue_wrapper import DataQueueWrapper #@UnresolvedImport
from afqueue.common import client_queue_types #@UnresolvedImport
import bson, redis #@UnresolvedImport


class RedisConnectionFailureException(Exception):
    pass
class RedisConnectionUnknownException(Exception):
    pass


class SharedMemoryManager(object):
    
        
    def __init__(self, qm_connection_string):
                    
        # Connect to shared memory.            
        host, port = qm_connection_string.split(":")
        self._qm_redis_client = redis.StrictRedis(host=host, port=int(port), db=0)
        
        # Create the empty PQM redis client dictionary.
        self._pqm_redis_client_dict = dict()
        self._pqm_redis_connection_information_dict = dict()
        
        
    def add_pqm_connection(self, pqm_id_string, pqm_connection_string): 
        """
        Creates a new connection to the shared memory storage of the given PQM using the PQM connection string.
        Tracks the connection by the PQM ID string for use; use the query_remote_shared_memory to access.
        """
                    
        # Create and add the new connection.    
        host, port = pqm_connection_string.split(":")
        self._pqm_redis_client_dict[pqm_id_string] = redis.StrictRedis(host=host, port=int(port), db=0)
        self._pqm_redis_connection_information_dict[pqm_id_string] = pqm_connection_string
            
        
    def add_qm_to_pecking_order_list(self, qm_id_string):
        """
        Adds the given QM ID string to the end of the pecking order list.
        """
        
        # Ensure the QM ID string doesn't already appear in the list.
        if qm_id_string not in self.get_pecking_order_list():
            self._add_qm_to_pecking_order_list(qm_id_string)
        
    
    def attach_queues_to_exchanges(self, client_queue_list):
        """
        Attaches each queue in the given client queue list to the exchange designated in that queue.
        Note: Manipulating object containers in memory should all be done from one thread due to the nature of the shared memory.
        """
        
        try:
            
            # Attach each queue to its declared exchange and update storage.
            for client_queue in client_queue_list:
                
                # Get the exchange, add the queue to the routing key, and update it.
                exchange_wrapper = self._get_exchange_wrapper(client_queue.exchange_name)
                exchange_wrapper.attach_queue(client_queue.routing_key, client_queue.name)
                self._set_exchange_wrapper(exchange_wrapper)
                
                # Add the queue.
                self._add_queue(client_queue)

        except:
            
            raise ExceptionFormatter.get_full_exception()
                
    
    def clear_connection_reply_data(self):
        """
        Removes all connection reply data from shared memory.
        """
        
        self._clear_connection_reply_data()
    
        
    @staticmethod
    def convert_queue_lock_owner_dict_to_network_format(shared_memory_queue_lock_owner_dict):
        """
        We convert the queue lock owner dictionary to a network format for more efficient network transmission.
        Supply the current owner dictionary in shared memory to this method to get the network form of the dictionary.
        """
        
        # Convert the owner dictionary from being keyed off queue name to being keyed off owner.
        queue_lock_owner_dict = dict()
        for queue_name, qm_id_string in list(shared_memory_queue_lock_owner_dict.items()):
            if qm_id_string != "":
                queue_lock_owner_dict.setdefault(qm_id_string, list()).append(queue_name)
                
        return queue_lock_owner_dict
    
    
    @staticmethod
    def convert_queue_lock_owner_dict_from_network_format(network_queue_lock_owner_dict):
        """
        We convert the queue lock owner dictionary to a network format for more efficient network transmission.
        Supply the a the network format of the owner dictionary to this method to get the shared memory form of the dictionary.
        """
        
        # Convert the owner dictionary from being keyed off owner to being keyed off queue name.
        queue_lock_owner_dict = dict()
        for qm_id_string, queue_name_list in list(network_queue_lock_owner_dict.items()):
            for queue_name in queue_name_list:
                queue_lock_owner_dict[queue_name] = qm_id_string
        
        return queue_lock_owner_dict
        
            
    @staticmethod
    def create_queue_lock_string(qm_id_string, process_id_string):        
        """
        Creates a queue lock string from the given parameters.
        """
        
        return "{0}.{1}".format(qm_id_string, process_id_string) 
        

    def delete_value(self, key):
        """
        Deletes a previously stored value.
        """
        
        self._delete_value(key)
    
    
    def get_and_delete_value(self, key):
        """
        Returns and deletes a previously stored value.
        """
        
        self._delete_value(key)
        return self._get_value(key)
        
        
    def get_connected_pqm_id_string_list(self): 
        """
        Returns a list of the PQM ID strings of all remotely connected shared memory storage.
        """
        
        return list(self._pqm_redis_client_dict.keys())
    
    
    def get_connection_reply_data(self, thread_name, qm_id_string):
        """
        Returns the connection reply data for the given thread / QM ID string from shared memory.
        """
        
        return self._get_connection_reply_data(thread_name, qm_id_string)
    
    
    def get_current_master_id_string(self):
        """
        Returns the QM ID String, denoted by the first item in the pecking order list, in the given shared memory object.
        """
        
        current_pecking_order_list = self._get_pecking_order_list()
        if current_pecking_order_list == None or len(current_pecking_order_list) == 0:
            return None
        return current_pecking_order_list[0]
    
        
    def get_exchange_wrapper_name_list(self):
        """
        Returns a list of all exchanges currently in shared memory.
        """
        
        # Forward.
        return self._get_exchange_wrapper_name_list()
    
    
    def get_exchange_wrapper(self, exchange_name):
        """
        Returns the exchange with the given name in shared memory.
        Returns None if the exchange does not exist.
        """
        
        # Forward.
        return self._get_exchange_wrapper(exchange_name)
                
        
    def get_exchange_wrapper_dict(self):
        """
        Returns a dictionary of all exchanges currently in shared memory.
        """
            
        # Forward.
        return self._get_exchange_wrapper_dict()
    
    
    def get_locked_queue_names(self):
        """
        Returns the list of queues which have current locks in shared memory.
        """
        
        # Return the keys of the current queue lock owner dictionary.
        queue_lock_owner_dict = self.get_queue_lock_owner_dict()
        return list(queue_lock_owner_dict.keys())
        
        
    def get_memory_usage(self):
        """
        Returns the total memory, in bytes, currently in use by shared memory.
        """
        
        info = self._get_info()
        if info == None:
            return None
        return info["used_memory"]
    
        
    def get_network_memory_statistics_dict(self):
        """
        Returns the current network statistics dictionary.
        Returns an empty dictionary if no entry exists.
        """
        
        return self._get_network_memory_statistics_dict()
        
        
    def get_ordered_queue_owners_dict(self):
        """
        Gets the current ordered queue owners dictionary from shared memory.
        """
        
        # Forward.
        ordered_queue_owners_dict = self._get_ordered_queue_owners_dict()
        if ordered_queue_owners_dict == None:
            return dict()
        return ordered_queue_owners_dict
    
                    
    def get_pecking_order_list(self):
        """
        Returns the current pecking order list (QM ID strings) in shared memory.
        """
        
        # Forward.
        return self._get_pecking_order_list()
        
        
    def get_queue_lock_data(self, queue_name):
        """
        Gets the current queue lock owner data of the given queue in shared memory.
        Returns two items: the owner ID string and the owner's process ID.
        """
        
        queue_lock_owner_string = self._get_queue_lock_owner_string(queue_name)
        if queue_lock_owner_string == None:
            queue_lock_owner_string = "."
        return self.split_queue_lock_string(queue_lock_owner_string)
        
                
    def get_queue_locks_owned_sets(self, test_queue_lock_owner_dict):
        """
        Expects the queue lock owner dictionary to be keyed off of queue names, valued with QM owner ID string.
        Tests each entry in the given dictionary against the data in shared memory.
        Returns two sets: A set of all queue names which have locks matching those given; a set of the queue names which do not have locks matching those given.
        """
        
        try:
            
            queue_lock_owner_dict = self.get_queue_lock_owner_dict()
            owned_set = set()
            not_owned_set = set()
            for queue_name, owner_id_string in list(test_queue_lock_owner_dict.items()):
                if queue_lock_owner_dict.get(queue_name, "") != owner_id_string:
                    not_owned_set.add(queue_name)
                else:
                    owned_set.add(queue_name)
            return owned_set, not_owned_set
        except:
            raise ExceptionFormatter.get_full_exception()
    
        
    def get_queue_lock_owner_dict(self):
        """
        Returns the current queue lock owner dictionary in shared memory.
        """
                
        return self._get_queue_lock_owner_dict() 
    
    
    def get_queue_name_list(self):
        """
        Returns a list of all queue currently in shared memory.
        """
        
        # Forward.
        return list(self._get_queue_name_set())
        
            
    def get_queue_wrapper(self, queue_name):
        """
        Returns the queue with the given name in shared memory.
        Returns None if the queue does not exist.
        """
        
        # Get the full queue wrapper dictionary.
        data_queue_wrapper_dict = self.get_queue_wrapper_dict()
        
        # Return the queue wrapper requested.
        return data_queue_wrapper_dict.get(queue_name, None)
        
            
    def get_queue_wrapper_dict(self):
        """
        Returns a dictionary of all queue wrappers.
        Note this call builds the queue wrappers; they aren't stored in shared memory like exchange wrappers.
        """
        
        # Fill out the current queue names. 
        queue_name_list = self.get_queue_name_list()
        queue_type_dict = self._get_queue_type_dict()
        data_queue_wrapper_dict = dict()
        for queue_name in queue_name_list:
            data_queue_wrapper_dict[queue_name] = DataQueueWrapper(queue_name, queue_type_dict.get(queue_name, client_queue_types.DISTRIBUTED))
        
        # Forward.
        return data_queue_wrapper_dict
    
        
    def get_queue_size(self, queue_name):
        """
        Returns the size of the given queue name.
        Returns -1 if the queue does not exist.
        """
        
        if queue_name not in self.get_queue_name_list():
            return -1
        
        return self._get_queue_size(queue_name)
        
        
    def get_queue_sizes(self, queue_name_list = None):
        """
        Returns the size of each queue currently tracked.
        Returns a dictionary with keys being the queue name and values being the queue size (int).
        Will have no entry for any queues which do not exist.
        """
        
        if queue_name_list == None:
            queue_name_list = self.get_queue_name_list()
            
        return dict(list(zip(queue_name_list, self._get_queue_sizes(queue_name_list))))


    def get_thread_name_dict(self):
        """
        Returns the current thread name dictionary stored in shared memory.
        """
        return self._get_thread_name_dict()
        
    
    def get_value(self, key):
        """
        Returns a previously stored value.
        """
        
        return self._get_value(key)
        
        
    def is_master(self, qm_id_string):
        """
        Returns True/False based on whether or not the given QM ID String matches the current master in the given shared memory object.
        """
        
        return self.get_current_master_id_string() == qm_id_string
    
    
    def ping(self):
        """
        Pings the shared memory.  
        Returns True if successful.
        Note: Intended to be used in a remote query.
        """
        
        return self._ping()
    
                     
    def pop_queue_data(self, queue_name, requested_count):
        """
        Attempts to pop the requested count of data from the given queue name's queue. 
        Returns two items: 
            the data in a list
            the count of data remaining in the queue
        """
        
        try:
            
            # Forward.
            data_list, queue_size = self._pop_queue(queue_name, requested_count)
                
            # Return the data and the remaining queue size.            
            return data_list, queue_size
                
        except:
            
            raise ExceptionFormatter.get_full_exception()    
    
        
    def purge_queue(self, queue_name):
        """
        Purges the given queue of all its data in shared memory.
        """
        
        self._purge_queue(queue_name)
    
        
    def push_queue_data_list(self, queue_name, data_list):
        """
        Attempts to push the data in the given data list into the queue of the given name in shared memory.
        Returns the current size of the queue after the push (integer).
        """
        
        return self._push_queue_data_list(queue_name, data_list)
            
    
    def query_remote_shared_memory(self, pqm_id_string, query_method, *arguments):
        """
        Directs the shared memory object to query the remote shared memory of the given PQM.
        Supply the method to run against the remote shared memory and the arguments required to normally make that call.
        Note you will need the full method signature.  For example:
            If you call with shared_memory_manager.query_remote_shared_memory, supply shared_memory_manager.<method_name>.
        Note: If a connection information can't be found, a RedisConnectionUnknownException will be raised.  Be sure to check for this and handle as desired!
        Note: If a connection can't be made to remote shared memory, a RedisConnectionFailureException will be raised.  Be sure to check for this and handle as desired!
        """
        
        # Get the remote client; raise an unknown exception if we have no data mapped for the PQM ID String.
        remote_redis_client = self._pqm_redis_client_dict.get(pqm_id_string, None)
        if remote_redis_client == None:
            raise RedisConnectionUnknownException(pqm_id_string)
            
        # Temporarily overwrite our client and run the command.
        qm_redis_client = self._qm_redis_client
        self._qm_redis_client = remote_redis_client
        
        # Ensure we don't permanently set our main redis client to the remote redis client by trapping
        try:
            
            # Run the query, reset our redis client, and return the result.
            result = query_method(*arguments)
            self._qm_redis_client = qm_redis_client
            return result
        
        except:
            
            # If we hit an exception, reset our redis client, and raise a connection failure exception.
            self._qm_redis_client = qm_redis_client
            #del(self._pqm_redis_client_dict[pqm_id_string])
            raise RedisConnectionFailureException(ExceptionFormatter.get_message())
    
    
    def remove_connection_reply_data(self, thread_name, qm_id_string):
        """
        Removes the connection reply data for the given thread / QM ID string from shared memory.
        """
        self._remove_connection_reply_data(thread_name, qm_id_string)
        
        
    def remove_pqm_connection(self, pqm_id_string): 
        """
        Removes the remote connection to the given PQMs shared memory storage.
        """
        if pqm_id_string in list(self._pqm_redis_client_dict.keys()):
            del(self._pqm_redis_client_dict[pqm_id_string])
            
        if pqm_id_string in list(self._pqm_redis_connection_information_dict.keys()):
            del(self._pqm_redis_connection_information_dict[pqm_id_string])
        
        
    def remove_qm_from_pecking_order_list(self, qm_id_string):
        """
        Removes the given QM ID string from the pecking order list.
        """
        
        # Forward.
        self._remove_qm_from_pecking_order_list(qm_id_string)
        
                
    def remove_queue_names_from_memory(self, queue_name_list):
        """
        Will remove all queues which match the names in the given queue name list.
        Will remove all queue names in the given queue name list from all exchanges.
        Note: Manipulating object containers in memory should all be done from one thread due to the nature of the shared memory.
        """
        
        # Get our current state in memory.
        current_exchange_wrapper_dict = self.get_exchange_wrapper_dict()

        # Remove the queue names from each exchange.
        for queue_name in queue_name_list:
            for exchange_wrapper in list(current_exchange_wrapper_dict.values()):
                if exchange_wrapper.is_queue_attached(queue_name):
                    exchange_wrapper.detach_queue(queue_name)
                    self._set_exchange_wrapper(exchange_wrapper)
            
        # Remove the queue names from our queue sets.
        for queue_name in queue_name_list:
            self._remove_queue(queue_name)  
            
        
    @staticmethod
    def split_queue_lock_string(queue_lock_owner_string):        
        """
        Splits the given queue lock string into its separate components.
        """
        
        return queue_lock_owner_string.split(".") 
    
    
    def set_connection_reply_data(self, thread_name, qm_id_string, data):
        """
        Sets the connection reply data for the given thread / QM ID string in shared memory.
        """
        
        # Forward.
        self._set_connection_reply_data(thread_name, qm_id_string, data)
    
    
    def set_network_memory_statistics_dict(self, network_memory_statistics_dict):
        """
        Sets the current network statistics dictionary.
        Pass a None value to delete the current stored dictionary.
        """
        
        # Forward.
        self._set_network_memory_statistics_dict(network_memory_statistics_dict)
        
        
    def set_ordered_queue_owners_dict(self, ordered_queue_owners_dict):
        """
        Sets the current ordered queue owners dictionary in the given shared memory object to the given dictionary.
        """
        
        # Forward.
        self._set_ordered_queue_owners_dict(ordered_queue_owners_dict)
        
        
    def set_queue_lock_dict(self, queue_lock_owner_dict):
        """
        Sets the current queue lock owner dictionary in the given shared memory object to the given queue lock owner dictionary.
        """
        
        # Forward.
        self._set_queue_lock_dict(queue_lock_owner_dict)

        
    def set_queue_size_snapshot_dict(self, queue_size_snapshot_dict):
        """
        Sets the current snapshot dictionary of queue sizes in shared memory.
        Note: these are queue size snapshots taken at regular intervals; true queue sizes should be obtained by the get_queue_size method.
        """
        
        # Forward.
        self._set_queue_size_snapshot_dict(queue_size_snapshot_dict)
        
        
    def set_queue_lock_owner(self, queue_name, qm_id_string):
        """
        Sets the current queue lock owner for the given queue name in the given shared memory object.
        If an empty string ("") is given for the QM ID String, the current owner data for the given queue name will be deleted.
        """
        
        try:
        
            # If the ID string is blank, just remove the key.
            if qm_id_string == "":
                self._remove_queue_lock_owner(queue_name)
            else:
                self._set_queue_lock_owner(queue_name, qm_id_string)
        
        except:
            
            raise ExceptionFormatter.get_full_exception()  
        
        
    def set_pecking_order_list(self, qm_id_string_list):
        """
        Sets the current pecking order list (QM ID strings).
        """
        
        # Forward.
        self._set_pecking_order_list(qm_id_string_list) 
        
        
    def set_thread_name_dict(self, thread_name_dict):
        """
        Sets the current thread name dictionary.
        """
        
        # Forward.
        self._set_thread_name_dict(thread_name_dict) 
        
        
    def store_value(self, key, data, expire_seconds):
        """
        Stores the given value.
        Include an expire time 
        """
        self._store_value(key, data, expire_seconds)
                     
    
    def update_exchange_wrappers(self, exchange_wrapper_list):
        """
        Updates shared memory with the given exchange wrappers.
        For each wrapper given:
            If the wrapper already exists with the same name and type, no action is taken.
            If the wrapper exists with the given name but has a different type, overwrites.
            If the wrapper does not exist, writes.
        Note: Manipulating object containers in memory should all be done from one thread due to the nature of the shared memory.
        """
        
        # Go through all exchanges.
        for exchange_wrapper in exchange_wrapper_list:
            
            current_exchange_wrapper = self._get_exchange_wrapper(exchange_wrapper.name)
            if current_exchange_wrapper == None:
                self._set_exchange_wrapper(exchange_wrapper)
            elif current_exchange_wrapper.name != exchange_wrapper.name or current_exchange_wrapper.type != exchange_wrapper.type:
                self._set_exchange_wrapper(exchange_wrapper) 
        
        
    def update_from_master_setup_data(self, exchange_wrapper_list, queue_wrapper_list):
        """
        Updates shared memory from the given setup data.
        """
        
        # Completely replace the exchange wrappers.
        for exchange_wrapper_name in self.get_exchange_wrapper_name_list():
            self._remove_exchange(exchange_wrapper_name)
        self.update_exchange_wrappers(exchange_wrapper_list)
            
        # Determine which queues should actually be deleted and delete them.
        current_queue_name_set = set(self.get_queue_name_list())
        given_queue_name_set = set([qw.name for qw in queue_wrapper_list])
        remove_queue_name_set = current_queue_name_set - given_queue_name_set
        self.remove_queue_names_from_memory(list(remove_queue_name_set))

        # Go through all the queue wrappers given and add them.
        # Note that we remove old tracking of the queue but not the queue data itself.
        #  This ensures we don't damage existing data during this process yet get the correct settings for the queue.
        for queue_wrapper in queue_wrapper_list:
            
            # Remove.
            self._remove_queue(queue_wrapper.name, False)
            
            # Add.
            self._add_queue(queue_wrapper)
            
        
    def which_client_queues_exist_and_are_attached(self, client_queue_list):
        """
        Compares the settings in the client queue objects in the given list against what currently exists in shared memory.
        Returns two lists of client queue objects:
            1) Objects whose settings have been validated in shared memory. 
            2) Objects whose settings have been invalidated in shared memory.
        """
        
        try:
            
            # Get the current memory state.
            current_exchange_dict = self.get_exchange_wrapper_dict()
            current_queue_name_list = self.get_queue_name_list()
            
            # Test each client queue given.
            client_queue_validated_list = list()
            client_queue_invalidated_list = list()
            for client_queue in client_queue_list:
                
                exchange = current_exchange_dict.get(client_queue.name, None)
                
                # If the exchange doesn't exist for any client queue in the given list, we have invalidated the test.
                if exchange == None:
                    client_queue_invalidated_list.append(client_queue)
                
                # If the exchange doens't have the queue name attached to the routing key for any client queue in the given list, we have invalidated the test.
                elif exchange.is_queue_attached_to_routing_key(client_queue.routing_key, client_queue.name) == False:
                    client_queue_invalidated_list.append(client_queue)
                
                # If the queue name doesn't exist for any client queue in the given list, we have invalidated the test.
                elif client_queue.name not in current_queue_name_list:
                    client_queue_invalidated_list.append(client_queue)
                
                # If this client queue didn't fail, mark it as validated.
                else:
                    client_queue_validated_list.append(client_queue)
            
            # We have validation if none of the given client queues have failed.
            return client_queue_validated_list, client_queue_invalidated_list
        
        except:
            
            raise ExceptionFormatter.get_full_exception()     
    
    
    def which_exchange_wrapper_names_exist(self, exchange_wrapper_name_set):
        """
        Compares the exchange names in the given set against the exchange names in shared memory.
        Returns two sets:
            1) The exchange names which exist 
            2) The exchange names which do not exist.
        """        
        
        try:            
            does_not_exist_set = exchange_wrapper_name_set - set(self.get_exchange_wrapper_name_list())  
            return exchange_wrapper_name_set - does_not_exist_set, does_not_exist_set
        except:
            raise ExceptionFormatter.get_full_exception()
    
    
    def which_queue_names_are_attached(self, queue_name_set):    
        """
        Compares the queue names in the given set against the queue names in all exchanges in shared memory.
        Returns two sets:
            1) The queue names which are attached to any exchange in shared memory. 
            2) The queue names which are not attached to any exchange in shared memory.
        """
        
        # Create the return list.
        attached_queue_name_set = set()
        
        # Get our current state in memory.
        current_exchange_wrapper_dict = self.get_exchange_wrapper_dict()

        # Remove the queue names from each exchange.
        for queue_name in queue_name_set:
            for exchange_wrapper in list(current_exchange_wrapper_dict.values()):
                if exchange_wrapper.is_queue_attached(queue_name):
                    attached_queue_name_set.add(queue_name)
                    
        # Return out.
        return attached_queue_name_set, queue_name_set - attached_queue_name_set
                        
    
    def which_queue_names_exist_or_are_attached(self, queue_name_set):
        """
        Compares the queue names in the given list against all queue names which currently exist in shared memory, either as queues or in exchanges' attached queue lists.
        Returns two sets of client queue objects:
            1) Queue names which exist as queues and/or attached to any exchanges in shared memory.
            2) Queue names which do not exist as queues and are not attached to any exchanges in shared memory.
        """
        
        # Of the given queue names, get which ones exist as queues in shared memory.
        current_queue_names_set = self._get_queue_name_set().intersection(queue_name_set)
        
        # Testing which queue names exist in exchanges is a more expensive operation.
        # We won't need to test queues which we already found as queues; remove them from the set.
        exchange_test_queue_name_set = queue_name_set - current_queue_names_set    
        
        # Extend the list with the queue names which are attached to exchanges.    
        attached_queue_name_set, _ = self.which_queue_names_are_attached(exchange_test_queue_name_set)
        current_queue_names_set = current_queue_names_set.union(attached_queue_name_set)
        
        # Return the two results.
        return current_queue_names_set, queue_name_set - current_queue_names_set
        
    
    def _add_queue(self, queue_object):
        self._qm_redis_client.sadd("data_queue_set", queue_object.name)    
        self._qm_redis_client.hset("data_queue_types",  queue_object.name,  queue_object.type)

    
    def _add_qm_to_pecking_order_list(self, qm_id_string):
        self._qm_redis_client.rpush("pecking_order", qm_id_string)
        
        
    def _clear_connection_reply_data(self):
        self._qm_redis_client.delete("reply_data")


    def _delete_value(self, key):
        self._qm_redis_client.delete("sv:{0}".format(key))
    
    
    def _get_connection_reply_data(self, thread_name, qm_id_string):
        return self._qm_redis_client.hget("reply_data", self._get_connection_reply_data_key(thread_name, qm_id_string))


    def _get_connection_reply_data_key(self, thread_name, qm_id_string):
        return "{0}.{1}".format(thread_name, qm_id_string)
                
        
    def _get_exchange_wrapper(self, exchange_wrapper_name):
        exchange_wrapper_name = cast_string(exchange_wrapper_name)
        dumped_exchange_wrapper = self._qm_redis_client.get("ew:" + exchange_wrapper_name)
        if dumped_exchange_wrapper == None:
            return None
        return ExchangeWrapper.load(dumped_exchange_wrapper)        
                
    
    def _get_exchange_wrapper_dict(self):      
        
        try:
            
            # Get the list of keys.
            exchange_wrapper_key_list = self._get_exchange_wrapper_key_list()
            
            # Return an empty dictionary if we have no keys.
            if len(exchange_wrapper_key_list) == 0:
                return dict()
            
            # If there are items in the dictionary, get the dumped exchange wrapper list.
            dumped_exchange_wrapper_list = self._qm_redis_client.mget(exchange_wrapper_key_list)
            
            # Form the exchange wrapper dictionary from the full keys and dumped values.
            exchange_wrapper_dict = dict()
            for _ in range(len(exchange_wrapper_key_list)):
                
                # Take the last element off both lists - they will be in order due to the nature of lists.
                full_key = exchange_wrapper_key_list.pop()
                dumped_value = dumped_exchange_wrapper_list.pop()
                
                # If the dumped string value is valid, load the exchange wrapper from the dumped value and set it as the real exchange wrapper name.
                if dumped_value != None:
                    exchange_wrapper_dict[full_key[3:]] = ExchangeWrapper.load(dumped_value)
            
            # Return our exchange wrapper dictionary.
            return exchange_wrapper_dict
            """
            
            # Ensure there are only valid fields in the dumped exchange wrapper list.
            while None in dumped_exchange_wrapper_list:
                dumped_exchange_wrapper_list.remove(None)
            
            # Pull the dumped exchange wrappers from the keys, load, and store in a list.
            exchange_wrapper_list = [ExchangeWrapper.load(dumped) for dumped in dumped_exchange_wrapper_list]
            
            # Get the names from the keys, create a map of names to wrappers, and return the map.
            exchange_wrapper_name_list = [key[3:] for key in exchange_wrapper_key_list]
            return dict(zip(exchange_wrapper_name_list, exchange_wrapper_list))
            """
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
    
    def _get_exchange_wrapper_key_list(self):
        keys_list = self._qm_redis_client.keys("ew:*")
        return cast_list_of_strings(keys_list)


    def _get_exchange_wrapper_name_list(self):
        return [key[3:] for key in self._get_exchange_wrapper_key_list()]
    
                    
    def _get_info(self):
        return self._qm_redis_client.info()   
    
        
    def _get_network_memory_statistics_dict(self):
        raw_network_memory_statistics_dict = self._qm_redis_client.get("net_stats")
        if raw_network_memory_statistics_dict == None:
            return dict()
        
        return bson.loads(raw_network_memory_statistics_dict)
            
    
    def _get_ordered_queue_owners_dict(self):
        #return self._qm_redis_client.hgetall("oq_owners")
        dumped_ordered_queue_owners_dict = self._qm_redis_client.get("oq_owners")
        if dumped_ordered_queue_owners_dict == None:
            return None
        
        return bson.loads(dumped_ordered_queue_owners_dict)
    
        
    def _get_queue_lock_owner_string(self, queue_name):
        queue_lock_owner_string = self._qm_redis_client.hget("queue_locks", queue_name)
        return cast_string(queue_lock_owner_string)


    def _get_queue_lock_owner_dict(self):
        queue_lock_owner_dict = self._qm_redis_client.hgetall("queue_locks")
        return dict([(k.decode(), v.decode()) for k, v in queue_lock_owner_dict.items()])


    def _get_queue_name_set(self):
        queue_name_set = self._qm_redis_client.smembers("data_queue_set")
        return cast_list_of_strings(queue_name_set)


    def _get_queue_size(self, queue_name):
        return self._qm_redis_client.llen(queue_name)
    
    
    def _get_queue_sizes(self, queue_name_list):
        # Utilize a pipeline object to handle the call in one transaction.
        pipeline = self._qm_redis_client.pipeline()
        for queue_name in queue_name_list:
            pipeline.llen(queue_name)
        return pipeline.execute()
        
            
    def _get_queue_type_dict(self):    
        get_queue_type_dict = self._qm_redis_client.hgetall("data_queue_types")
        return dict([(k.decode(), v.decode()) for k, v in get_queue_type_dict.items()])


    def _get_pecking_order_list(self):
        pecking_order_list = self._qm_redis_client.lrange("pecking_order", 0, -1)
        return cast_list_of_strings(pecking_order_list)


    def _get_thread_name_dict(self):
        return bson.loads(self._qm_redis_client.get("thread_names")) 


    def _get_value(self, key):
        value = self._qm_redis_client.get("sv:{0}".format(key))
        return cast_string(value)


    def _ping(self):
        return self._qm_redis_client.echo("") == ""
    
    
    def _pop_queue(self, queue_name, element_count = 1):
        
        try:
        
            # If we are below our threshold for using a bulk pop, do singular pops.
            if element_count < 3:
                
                # Form the return list and pop our element count.
                data_list = list()
                for _ in range(element_count):
                    
                    # Add data if valid; break if not.
                    data = self._qm_redis_client.lpop(queue_name)
                    if data == None:
                        break
                    data_list.append(data)
            
                # Get the end queue size.
                if len(data_list) == 0:
                    queue_size = 0
                else:
                    queue_size = self._qm_redis_client.llen(queue_name)
                
            # Bulk pop if we are allowed to.
            else:
                
                p = self._qm_redis_client.pipeline()
                p.lrange(queue_name, 0, element_count - 1)
                p.ltrim(queue_name, element_count, -1)
                p.llen(queue_name)
                data_list, _, queue_size = p.execute()
                
            # Return result.
            return data_list, queue_size
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
    
    def _purge_queue(self, queue_name):
        queue_size = self._get_queue_size(queue_name)
        if queue_size > 0:
            self._qm_redis_client.ltrim(queue_name, queue_size, -1)
    
    
    def _push_queue_data_list(self, queue_name, data_list):
        return self._qm_redis_client.rpush(queue_name, *data_list)


    def _remove_connection_reply_data(self, thread_name, qm_id_string):
        self._qm_redis_client.hdel("reply_data", self._get_connection_reply_data_key(thread_name, qm_id_string))
        
        
    def _remove_exchange(self, exchange_wrapper_name):
        exchange_wrapper_name = cast_string(exchange_wrapper_name)
        self._qm_redis_client.delete("ew:" + exchange_wrapper_name)
                    
    
    def _remove_qm_from_pecking_order_list(self, qm_id_string):
        self._qm_redis_client.lrem("pecking_order", 1, qm_id_string)
    
      
    def _remove_queue(self, queue_name, remove_data = True):
        self._qm_redis_client.srem("data_queue_set", queue_name)
        self._qm_redis_client.hdel("data_queue_types", queue_name)
        if remove_data == True:
            self._qm_redis_client.delete(queue_name)   
        
        
    def _remove_queue_lock_owner(self, queue_name):
        self._qm_redis_client.hdel("queue_locks", queue_name)
        
        
    def _set_connection_reply_data(self, thread_name, qm_id_string, data):
        self._qm_redis_client.hset("reply_data", self._get_connection_reply_data_key(thread_name, qm_id_string), data)
        
    
    def _set_exchange_wrapper(self, exchange_wrapper):
        self._qm_redis_client.set("ew:" + cast_string(exchange_wrapper.name), exchange_wrapper.dump())
    
        
    def _set_network_memory_statistics_dict(self, network_memory_statistics_dict):
        if network_memory_statistics_dict == None:
            self._qm_redis_client.delete("net_stats")
        else:
            self._qm_redis_client.set("net_stats",bson.dumps(network_memory_statistics_dict))
        
    
    def _set_pecking_order_list(self, qm_id_string_list):
        self._qm_redis_client.delete("pecking_order")
        if len(qm_id_string_list) > 0:
            self._qm_redis_client.rpush("pecking_order", *qm_id_string_list)
            
    
    def _set_queue_lock_dict(self, queue_lock_owner_dict):
        self._qm_redis_client.delete("queue_locks")
        if len(queue_lock_owner_dict) > 0:
            self._qm_redis_client.hmset("queue_locks", queue_lock_owner_dict)
            
    
    def _set_ordered_queue_owners_dict(self, ordered_queue_owners_dict):
        self._qm_redis_client.delete("oq_owners")
        if len(ordered_queue_owners_dict) > 0:
            self._qm_redis_client.set("oq_owners", bson.dumps(ordered_queue_owners_dict))
            #self._qm_redis_client.hmset("oq_owners", ordered_queue_owners_dict)


    def _set_queue_lock_owner(self, queue_name, owner_string):
        self._qm_redis_client.hset("queue_locks", queue_name, owner_string)
        
        
    def _set_queue_size_snapshot_dict(self, queue_size_snapshot_dict):
        self._qm_redis_client.set("queue_size_snapshot", bson.dumps(queue_size_snapshot_dict))


    def _set_thread_name_dict(self, thread_name_dict):
        self._qm_redis_client.set("thread_names", bson.dumps(thread_name_dict))    
        
        
    def _store_value(self, key, data, expire_seconds):
        if expire_seconds == None:
            self._qm_redis_client.set("sv:{0}".format(key), data)  
        else:
            self._qm_redis_client.setex("sv:{0}".format(key), data, expire_seconds)  
            
                
