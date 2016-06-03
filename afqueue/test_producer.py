from .common.exception_formatter import ExceptionFormatter
from .common.zmq_client import ZmqClient
from .common.client_queue import ClientQueue 
from . import messages.message_types as message_types
import os
import time
import zmq #@UnresolvedImport
from .common import client_exchange_types
from .common.client_exchange import ClientExchange #@UnresolvedImport
from .common import client_queue_types #@UnresolvedImport
import json


class TestProducer():
        
        
    def _make_data(self, file_data = None):
        
        # Make some random data; make the creation of the data take one second.
        #time.sleep(1)
        if file_data == None:
            
            #garbage_json = {'garbage' : 101,
            #            'sts':  self.message_count}
            #file_data = json.dumps(garbage_json)
            
            pid = os.getpid()
            file_data = "{0} : {1}".format(pid, int(time.time()))
            
        # Add.
        return str(self.message_count) + " ::: " + file_data
    
    
    def __init__(self, base_port, exchange_type, file_name):
        
        # Init message count.
        self.message_count = 0
        self.message_count_max = 80000
        
        # Create some real data.
        file_data = None
        if file_name != None:
            file_data = file(file_name).read()
        #else:
        #    file_data = file("test_data").read()    
        
            
        # Set up the exchange/queue information.
        exchange_name_set = set()
        queue_list = list()
        
        # Define exchanges.
        exchange_name_direct = "direct.test"
        exchange_name_direct2 = "direct.test"
        exchange_name_fanout = "fanout.test"
        exchange_name_topic = "topic.test"
        exchange_name_ordered_fanout = "fanout.ordered.test"
        exchange_name_ordered_fanout2 = "fanout.ordered.test2"
        exchange_name_ordered_fanout3 = "fanout.ordered.test3"
        client_exchange_list = list()
        client_exchange_list.append(ClientExchange(exchange_name_direct, client_exchange_types.DIRECT))
        client_exchange_list.append(ClientExchange(exchange_name_direct2, client_exchange_types.DIRECT))
        client_exchange_list.append(ClientExchange(exchange_name_fanout, client_exchange_types.FANOUT))
        client_exchange_list.append(ClientExchange(exchange_name_topic, client_exchange_types.TOPIC))
        client_exchange_list.append(ClientExchange(exchange_name_ordered_fanout, client_exchange_types.FANOUT))
        client_exchange_list.append(ClientExchange(exchange_name_ordered_fanout2, client_exchange_types.FANOUT)) 
        client_exchange_list.append(ClientExchange(exchange_name_ordered_fanout3, client_exchange_types.FANOUT)) 
        
        # Define queues. 
        client_queue_list = list()
        client_queue_list.append(ClientQueue("direct.test", exchange_name_direct))
        client_queue_list.append(ClientQueue("direct.test2", exchange_name_direct2))
        client_queue_list.append(ClientQueue("fanout.test.0", exchange_name_fanout))
        client_queue_list.append(ClientQueue("fanout.test.1", exchange_name_fanout))
        client_queue_list.append(ClientQueue("fanout.test.ou", exchange_name_ordered_fanout3, "", client_queue_types.ORDERED))  
        client_queue_list.append(ClientQueue("topic.test.x", exchange_name_topic, "x.1"))
        client_queue_list.append(ClientQueue("topic.test.x", exchange_name_topic, "x.2"))
        client_queue_list.append(ClientQueue("topic.test.y", exchange_name_topic, "y.1"))
        client_queue_list.append(ClientQueue("topic.test.z", exchange_name_topic, "y.2"))
        client_queue_list.append(ClientQueue("fanout.ordered.q1", exchange_name_ordered_fanout, "", client_queue_types.ORDERED))  
        client_queue_list.append(ClientQueue("fanout.ordered.q2", exchange_name_ordered_fanout, "", client_queue_types.ORDERED))  
        client_queue_list.append(ClientQueue("fanout.ordered.q3", exchange_name_ordered_fanout, "", client_queue_types.ORDERED))  
        client_queue_list.append(ClientQueue("fanout.ordered.q4", exchange_name_ordered_fanout2, "", client_queue_types.ORDERED))  
        
        # Create the queue client and connect.
        queue_server_list = ["localhost:{0}".format(base_port),]
        zmq_client = ZmqClient(queue_server_list)
        zmq_client.connect()
        
        # Declare our previously defined queues and exchanges.
        zmq_client.declare_exchanges(client_exchange_list)        
        zmq_client.declare_queues(client_queue_list)
        
        # Set the correct exchange we will push to for this producer.
        push_exchange = exchange_name_direct
        routing_key = ""
        if exchange_type == client_exchange_types.FANOUT:
            push_exchange = exchange_name_fanout
            #push_exchange = exchange_name_ordered_fanout2
            push_exchange = exchange_name_ordered_fanout3
        elif exchange_type == client_exchange_types.TOPIC:
            push_exchange = exchange_name_topic
            routing_key = "x.*"
            
        # Set up loop data and enter the loop.
        data = self._make_data(file_data)
        time_out = 1 * 1000
        
        # Create the Redis client.
        begin_time = time.time()
        while True:
            
            try:
                
                # Push a message.
                result = zmq_client.send_data(push_exchange, routing_key, data, time_out)
                
                # If the result is None, the push succeeded.  
                # A BE process would allow itself to continue working at this point.
                # This test object creates new data which it will push in the next iteration.
                if result == None:
                    
                    # DEBUG
                    print("Successfully sent data to QM; First 32 chars: {0}".format(data[:32]))
                    
                    # Make data.
                    data = self._make_data(file_data)
                    self.message_count += 1
                    
                    if self.message_count_max != None:
                        if self.message_count >= self.message_count_max:
                            break
                    
                # If we failed, print the result and do nothing else - this will effectively make us try to send the same data again.
                else:
                    
                    # DEBUG
                    print(result[-300:])
                    time.sleep(0.5)
                    
            except KeyboardInterrupt:
                
                print("Breaking due to keyboard interrupt.")
                break
                                
            except:
                
                # If an exception is raised out of the queue client, crash out of the loop. 
                # A BE process would need to crash out as well - this is the same as a PIKA exception; process needs to crash. 
                print(ExceptionFormatter.get_message())
                break           
            
        # Print out stats of time elapsed.
        end_time = time.time()
        elapsed = end_time - begin_time
        print("Took {0} seconds to send {1} messages".format(elapsed, self.message_count)) 
           
        # Try deleting a queue.
        #zmq_client.delete_queues()#["fanout.test.1"])#, "fanout.ordered.q4"])
        
        # Disconnect the queue client.
        zmq_client.disconnect()        
        
        