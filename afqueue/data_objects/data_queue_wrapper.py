from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
import bson #@UnresolvedImport
from afqueue.common import client_queue_types #@UnresolvedImport


class DataQueueWrapper(object):
    """
    Wrapping class over our abstract notion of a queue.
    Intended to extend basic queue functionality and tracking capabilities.
    """
    
    def __init__(self, name, queue_type):
        
        try:
            
            self.name = name
            self.type = queue_type
                
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
    
    def __str__(self): 
        
        return "{0}({1})".format(self.name, str(self.type)) 
       
    
    def dump(self):
        """
        Returns a string which can be used in the "load" method to rebuild this object.
        """
        
        return bson.dumps(self.__dict__)
    
    
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
    def load(dumped_string):
        """
        Returns an instance object of this class built from string which was created in the "dump" method.
        """
        
        loaded_dictionary = bson.loads(dumped_string)
        queue_wrapper = DataQueueWrapper("", 1)
        queue_wrapper.__dict__.update(loaded_dictionary)
        return queue_wrapper
        