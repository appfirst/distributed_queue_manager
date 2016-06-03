#from common.exception_formatter import ExceptionFormatter #@UnresolvedImport

class ClientDataSubscription(object):    
    
    def __init__(self, queue_name, routing_key, handler_method):
        
        self.queue_name = queue_name
        self.routing_key = routing_key
        self.handler_method = handler_method


    def __str__(self):
        
        return "Queue: {0}; Routing Key: {1}".format(self.queue_name, self.routing_key)
    
    
    @staticmethod
    def get_callback_key(queue_name, routing_key):
        
        return "{0}::{1}".format(queue_name, routing_key)
        
    
    @staticmethod
    def create_from_dumped_string(self, dumped_string):
        
        raw_split = dumped_string.split("::")
        return ClientDataSubscription(raw_split[0], raw_split[1])
    
    
    @staticmethod
    def create_from_dumped_string_list(raw_dumped_string_list):
        
        # Break the dumped string list into separate dumped strings.
        client_data_subscription_list = list()
        for dumped_string in raw_dumped_string_list.split(":::"):
            client_data_subscription_list.append(ClientDataSubscription.create_from_dumped_string(dumped_string))
            
        return client_data_subscription_list
        
    