#from afmessage.queue import AFMessageQueue
from afqueue.common import amqpclient #@UnresolvedImport
from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport

class PikaPublisher(object):    
            
    class AFMessagePublisher(object):
        
        def __init__(self, queue_servers = None, queue_mode = "rabbitmq_cluster"):
            self.queue_servers = queue_servers
            self.queue_mode = queue_mode
            self.connection = amqpclient.create_connection(self.queue_servers, self.queue_mode)
            self.client = amqpclient.AmqpClient(self.connection)
    
    
        def close_connection(self):
            if self.connection is not None:
                self.connection.close()
            
            
        def publish(self, exchange, routing_key, data, properties=None):
            self._publish(exchange, routing_key, data, properties)
    
    
        def _publish(self, exchange, routing_key, data, properties=None):
            if routing_key != None:
                self.client.publish(data, exchange, routing_key, properties=properties)
            else:
                self.client.publish(data, exchange, properties=properties)
                
    
        def _publishx(self, exchange, exchange_type, routing_key, data, properties=None):
            if exchange_type == "direct":
                self.client.publish(data, exchange, routing_key, properties=properties)
            elif exchange_type == "topic":
                self.client.publish(data, exchange, routing_key, properties=properties)
            elif exchange_type == "fanout":
                self.client.publish(data, exchange, properties=properties)
            else:
                raise Exception("Exchange type not supported: " + exchange_type)
                
                
    class AFMessageQueue(object):
        
        def __init__(self, queue_servers, queue_mode = "rabbitmq_cluster"):
            self.queue_servers = queue_servers
            self.queue_mode = queue_mode
            self.publisher = None
            self.subscribers = {}
    
    
        def publish(self, exchange, routing_key, data, properties=None):
            try:
                if self.publisher is None:
                    self.publisher = PikaPublisher.AFMessagePublisher(self.queue_servers, self.queue_mode)
                self.publisher.publish(exchange, routing_key, data, properties=properties)
            except:
                
                raise ExceptionFormatter.get_full_exception()
    
    
        def close(self):
            if self.publisher is not None:
                self.publisher.close_connection()
            for subscriber in self.subscribers.values():
                subscriber.close_connection()
            
            
    def __init__(self, server_list, server_mode, logger):
        """
        rabbitmq_cluster/rabbitmq_singular
        """
        
        self.server_list = server_list
        self.server_mode = server_mode
        self.logger = logger
        self._queue = None
        
        pass
        
        
    def declare_queue(self, server_list, server_mode = "rabbitmq_cluster"):
        return PikaPublisher.AFMessageQueue(server_list, server_mode)#rabbitmq_singular   
    
    
    def publish(self, exchange, routing_key, data, logger, queue = None):
        
        try:
            
            # If we were given a queue, publish.
            if queue:
                queue.publish(exchange, routing_key, data)
                
            # If we were not given a queue.
            else:
    
                try:
                    
                    # If we don't have a member queue defined, define.
                    if not self._queue:
                        logger.log_warning("creating new queue connections")
                        self._queue = self.declare_queue(self.server_list, self.server_mode)
                    
                    # Publish.
                    self._queue.publish(exchange, routing_key, data)
                    
                except:
                    
                    # If we failed, create a local queue and retry.
                    logger.log_warning("unable to use the global queue instance, creating one ourself")
                    queue = self.declare_queue(self.server_list, self.server_mode)
                    queue.publish(exchange, routing_key, data)
                    
        except:
            
            raise ExceptionFormatter.get_full_exception()
            
                
    def close(self):
        
        if self._queue != None:
            self._queue.close()
                                
        