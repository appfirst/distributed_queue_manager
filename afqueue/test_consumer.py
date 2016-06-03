from .common import client_exchange_types
from .common.client_exchange import ClientExchange #@UnresolvedImport
from .common.client_queue import ClientQueue
from .common.client_queue_lock import ClientQueueLock
from .common.client_data_subscription import ClientDataSubscription
from .common.exception_formatter import ExceptionFormatter
from .common.zmq_client import ZmqClient
import json
from . import messages.message_types as message_types
from .common import client_queue_types #@UnresolvedImport
import os
import time
import zmq #@UnresolvedImport


class TestConsumer():
        
        
    # Define a callback handler.
    def handle_data(self, raw_data):
        
        # Do some testing...
        try:
            test = raw_data[:32]
            index = test.find(":::")
            value = int(test[:index-1])
            if self._last_value_found == None:
                print("Seed value: ", str(value))
            elif value != self._last_value_found + 1:
                print("Value did not increment properly.  Last {0}, current {1}".format(self._last_value_found, value))
            elif self._last_value_logged > 100:
                print("Value did increment properly.  Last {0}, current {1}".format(self._last_value_found, value))
                self._last_value_logged -= 100
            else:
                self._last_value_logged += 1
                
            self._last_value_found = value
        except:
            print("Got data back; First 32 chars: {0}".format(raw_data[:32]))
            
            
    def handle_data_locked(self, raw_data):
        
        print("Got locked data back; First 32 chars: {0}".format(raw_data[:32]))
    
    
    def __init__(self, base_port, exchange_type, use_unique_queue_flag, force_unlock_flag):
        """
        This class is intended to be used purely for testing QM functionality.
        This is a consumer.
        Set the base port to the QM to connect to.
        Set the host master flag to True to use a HM-simulated queues (with locking).
        Set the host master flag to False to use worker-simulated queues (non-locking).
        Set the force unlock flag to True if queue lock requests from the QM should force any existing locks to be overwritten.
        """
        
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
        """   
        client_queue_list.append(ClientQueue("fanout.test.unique0", exchange_name_fanout, "", True))      
        client_queue_list.append(ClientQueue("fanout.test.unique1", exchange_name_fanout, "", True))      
        client_queue_list.append(ClientQueue("fanout.test.unique2", exchange_name_fanout, "", True))      
        client_queue_list.append(ClientQueue("fanout.test.unique3", exchange_name_fanout, "", True))      
        client_queue_list.append(ClientQueue("fanout.test.unique4", exchange_name_fanout, "", True))
        client_queue_list.append(ClientQueue("fanout.test.unique5", exchange_name_fanout, "", True))
        #"""        
        client_queue_list.append(ClientQueue("topic.test.x", exchange_name_topic, "x.1"))
        client_queue_list.append(ClientQueue("topic.test.x", exchange_name_topic, "x.2"))
        client_queue_list.append(ClientQueue("topic.test.y", exchange_name_topic, "y.1"))
        client_queue_list.append(ClientQueue("topic.test.z", exchange_name_topic, "y.2"))
        client_queue_list.append(ClientQueue("fanout.ordered.q1", exchange_name_ordered_fanout, "", client_queue_types.ORDERED))  
        client_queue_list.append(ClientQueue("fanout.ordered.q2", exchange_name_ordered_fanout, "", client_queue_types.ORDERED))  
        client_queue_list.append(ClientQueue("fanout.ordered.q3", exchange_name_ordered_fanout, "", client_queue_types.ORDERED))  
        client_queue_list.append(ClientQueue("fanout.ordered.q4", exchange_name_ordered_fanout2, "", client_queue_types.ORDERED))  
        
        # Create the queue client and open its socket.
        queue_server_list = ["localhost:{0}".format(base_port),]
        zmq_client = ZmqClient(queue_server_list, 0)
        zmq_client.connect()
        
        # Declare our previously defined queues and exchanges.
        zmq_client.declare_exchanges(client_exchange_list)        
        zmq_client.declare_queues(client_queue_list)
        
        # Setup our subscription.
        client_data_subscription_list = list()
        if exchange_type == client_exchange_types.FANOUT:
            #client_data_subscription_list.append(ClientDataSubscription("fanout.test.0", "", self.handle_data))
            #client_data_subscription_list.append(ClientDataSubscription("fanout.ordered.q1", "", self.handle_data))
            #client_data_subscription_list.append(ClientDataSubscription("fanout.ordered.q2", "", self.handle_data))
            #client_data_subscription_list.append(ClientDataSubscription("fanout.ordered.q3", "", self.handle_data))
            client_data_subscription_list.append(ClientDataSubscription("fanout.ordered.q4", "", self.handle_data))
        elif exchange_type == client_exchange_types.TOPIC:
            client_data_subscription_list.append(ClientDataSubscription("topic.test.x", "", self.handle_data))
        else:
            client_data_subscription_list.append(ClientDataSubscription("direct.test", "", self.handle_data))
            
        # Overwrite our settings to the unique queue if we are using the unique queue.
        lock_queue_name_list = list()
        if use_unique_queue_flag == True:
            client_data_subscription_list.append(ClientDataSubscription("fanout.test.ou", "", self.handle_data_locked))
            """
            client_data_subscription_list.append(ClientDataSubscription("fanout.test.unique0", "", self.handle_data_locked))
            client_data_subscription_list.append(ClientDataSubscription("fanout.test.unique1", "", self.handle_data_locked))
            client_data_subscription_list.append(ClientDataSubscription("fanout.test.unique2", "", self.handle_data_locked))
            client_data_subscription_list.append(ClientDataSubscription("fanout.test.unique3", "", self.handle_data_locked))
            client_data_subscription_list.append(ClientDataSubscription("fanout.test.unique4", "", self.handle_data_locked))
            client_data_subscription_list.append(ClientDataSubscription("fanout.test.unique5", "", self.handle_data_locked))
            #"""
            #client_data_subscription_list.append(ClientDataSubscription("fanout.test.unique", "", self.handle_data_locked))
            lock_queue_name_list = ["fanout.test.ou",]
        
        # Set the queue locks.
        queue_lock_list = list()
        for queue_name in lock_queue_name_list:
            queue_lock_list.append(ClientQueueLock(queue_name, force_unlock_flag))
        if len(queue_lock_list) > 0:
            zmq_client.lock_queues(queue_lock_list)
        
        # Subscribe.
        for client_data_subscription in client_data_subscription_list:
            zmq_client.subscribe_to_queue(client_data_subscription)
        
        # Set up loop data.
        message_count = 100
        time_out = 5 * 1000
        
        # Enter the main loop of consuming data.
        begin_time = time.time()
        time_test_complete = False
        self._messages_found = 0
        self._last_value_found = None
        self._last_value_logged = 101
        messages_to_find = 30000
        while True:
            
            try:
         
                # Request 
                result = zmq_client.request_data(message_count, time_out)
                if type(result) == int:
                    #esult = zmq_client.send_data(exchange_name_direct2, "", "TEST", time_out)                   
                    if time_test_complete == False:
                        self._messages_found += message_count
                        if self._messages_found >= messages_to_find:
                            elapsed = time.time() - begin_time
                            print("Took {0} seconds to consume {1} messages".format(elapsed, messages_to_find)) 
                            time_test_complete = True
                
                    #print "Total data remaining in all queues subscribed to: {0}".format(result)
                    if result == 0:
                        time.sleep(1)
                        
                else:
                    print("Notification received from ZmqClient: ", str(result))
                    # Sleep.
                    time.sleep(0.1)
                    
            except KeyboardInterrupt:
                
                print("Breaking due to keyboard interrupt.")
                break

            except:
                
                print(ExceptionFormatter.get_message())
                break

        # Try deleting a queue.
        #zmq_client.delete_queues(["fanout.test.unique"])
        #zmq_client.delete_queues(["fanout.test.ou"])
        
        elapsed = time.time() - begin_time
        print("Took {0} seconds to consume {1} messages".format(elapsed, self._messages_found)) 
        
        # Close the queue client socket.
        zmq_client.disconnect()
