#from common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.encoding_utilities import cast_bytes
import re #@UnresolvedImport
import bson #@UnresolvedImport
from afqueue.common import client_exchange_types #@UnresolvedImport

class ExchangeWrapper(object):
    
    def __init__(self, exchange_name, exchange_type):
        
        self.name = exchange_name
        self.type = exchange_type
        
        self._routing_key_to_queue_names_dict = dict()
        self._queue_count = 0
    
        
    def attach_queue(self, routing_key, queue_name):
        """
        Attaches the given queue to the exchange under the given routing key.
        """
        
        queue_name_list = self._routing_key_to_queue_names_dict.get(routing_key, list())
        if queue_name not in queue_name_list:
            queue_name_list.append(queue_name)
            self._routing_key_to_queue_names_dict[routing_key] = queue_name_list
            self._queue_count += 1
    
        
    def detach_queue(self, queue_name):
        """
        Detaches the given queue from all previously registered routing keys within the exchange.
        """
        
        for routing_key, queue_name_list in list(self._routing_key_to_queue_names_dict.items()):
            if queue_name in queue_name_list:
                queue_name_list.remove(queue_name)
                self._routing_key_to_queue_names_dict[routing_key] = queue_name_list
                self._queue_count -= 1
    
    
    def dump(self):
        """
        Returns a string which can be used in the "load" method to rebuild this object.
        """
        
        return bson.dumps(self.__dict__)
    
        
    def get_queue_count(self):
        """
        Returns the total number of queues attached to the routing keys within the exchange.
        """
        
        return self._queue_count

    
    def get_routing_key_queue_names(self, routing_key):
        """
        Returns all routing keys within the exchange.
        """
        
        return self._routing_key_to_queue_names_dict.get(routing_key, list())
    
    
    def get_routed_queue_names(self, routing_key):
        """
        Returns a list of all queue names the given routing key resolves to.
        """
        
        # Do no matching if the key is empty.
        if routing_key == "":
            
            return self._routing_key_to_queue_names_dict.get(routing_key, list()) 
        
        else:
            
            # Reform the routing key as a regular expression, adding anchors.
            regular_expression = "^" + routing_key.replace("*.","\w+\\.").replace("*","\w+") + "$"
            
            # Match the routing keys we have against the regular expression we generated from the routing key we were given.
            routing_key_list = list(self._routing_key_to_queue_names_dict.keys())
            matched_routing_key_set = set()
            for routing_key_test in routing_key_list:
                if routing_key_test == "":
                    continue
                if re.search(regular_expression, routing_key_test) != None:
                    matched_routing_key_set.add(routing_key_test)
                    
            # Create the return list, populate, and return.
            queue_name_set = set()
            for matched_routing_key in matched_routing_key_set:
                for routing_key in self._routing_key_to_queue_names_dict[matched_routing_key]:
                    queue_name_set.add(routing_key)
            return queue_name_set
    
        
    def get_type(self):
        """
        Returns the type of the exchange.
        """
        
        return self.type
        
        
    def is_queue_attached(self, queue_name):
        """
        Returns True if the given queue is attached to the exchange under any routing key.
        """
        
        for queue_name_list in list(self._routing_key_to_queue_names_dict.values()):
            if queue_name in queue_name_list:
                return True
        
        return False

        
    def is_queue_attached_to_routing_key(self, routing_key, queue_name):
        """
        Returns true if the given queue is attached to the given routing key within the exchange.
        """
        
        queue_name_list = self.get_routing_key_queue_names(routing_key)
        return queue_name in queue_name_list
        
    
    @staticmethod
    def load(dumped_string):
        """
        Returns an instance object of this class built from string which was created in the "dump" method.
        """

        dumped_string = cast_bytes(dumped_string)
        loaded_dictionary = bson.loads(dumped_string)
        exchange_wrapper = ExchangeWrapper("", "")
        exchange_wrapper.__dict__.update(loaded_dictionary)
        return exchange_wrapper
    
        
    def __str__(self):
        return "{0}({1}): RK Dict: {2}".format(self.name, client_exchange_types.convert_enum_string_to_name(self.type), self._routing_key_to_queue_names_dict)
        
