from afmessage.collector import CollectorData
from afqueue.common.exception_formatter import ExceptionFormatter
from afqueue.common.zmq_client import ZmqClient
import random
# python test_streamer.py 

class TestStreamer():
    
    
    def _load_data(self):
        
        f = open("test_data_worker0", "r")
        data = f.read()
        return data
    
    def _push(self, zmq_client, exchange_type):
        
        
        """
        # Create and publish.
        import time
        xtime = int(time.time())
        c = CollectorData(tenant=1,
                hostname="p0-mndev-frontend0", host=19,
                filetime=1363988113,
                version=92, filename="AppAccess-2013-03-22-H21-M35-S13-MS276", data="TESTDATA",
                uploadtime=xtime, keepraw=False,
                flags=0)
                
        # Get the exchange.
        push_exchange = c.exchange
        exchange_type = c.exchange_type
        
        
        # Get the routing key depending on the exchange type.
        if exchange_type == "direct":
            routing_key = c.routing_key
        elif exchange_type == "topic":
            routing_key = c.routing_key
        elif exchange_type == "fanout":
            routing_key = ".."
        else:
            raise Exception("Invalid exchange type: " + exchange_type)
                                
        # Get the data.
        data = c.serialize().encode('base64')
        """
        
        #push_exchange = "collector.data"
        data = self._load_data()
        push_exchange = "collector.data"
        exchange_type = "fanout"
        routing_key = ".."

        # Set push data time out.
        time_out = 1 * 1000
        
        try:

            # Push a message.
            result = zmq_client.send_data_to_pika_endpoint(push_exchange, routing_key, data, time_out)
            
            # If the result is None, the push succeeded.  
            # A BE process would allow itself to continue working at this point.
            # This test object creates new data which it will push in the next iteration.
            if result == None:
                
                # DEBUG
                print("Successfully sent data to QM; First 32 chars: {0}".format(data[:32]))
                
            # If we failed, print the result and do nothing else - this will effectively make us try to send the same data again.
            else:
                
                # DEBUG
                print(result)
                            
        except:
            
            # If an exception is raised out of the queue client, crash out of the loop. 
            # A BE process would need to crash out as well - this is the same as a PIKA exception; process needs to crash. 
            print(ExceptionFormatter.get_message())
    
    
    def _simulate_shortlived_connection_push(self, bridge_ip, bridge_port, exchange_type):
                    
        # Create and connect the queue client.
        zmq_client = self._setup_connection(bridge_ip, bridge_port)
        
        # Push.
        self._push(zmq_client, exchange_type)
        
        # Disconnect the queue client.
        zmq_client.disconnect()
        
        
    def _setup_connection(self, bridge_ip, bridge_port):
    
        # Create the queue client and connect.
        queue_server_list = ["{0}:{1}".format(bridge_ip, bridge_port),]
        zmq_client = ZmqClient(queue_server_list)
        zmq_client.connect()
        return zmq_client
    
    
    def _run_short_lived_connection(self, bridge_connection_information_list, exchange_type):

        bridge_connection_information_list_length = len(bridge_connection_information_list)
        
        import time
        start_time = time.time()
        message_count = 0
        
        while True:
            
            try:
                
                # Sleep.
                time.sleep(0.01)
                
                # Get a random port.
                bridge_connection = bridge_connection_information_list[int(random.random() * bridge_connection_information_list_length)]
                
                # Make a new connection and push data.
                self._simulate_shortlived_connection_push(bridge_connection[0], bridge_connection[1], exchange_type)
                message_count += 1             
                if time.time() - start_time > 60:
                    break

            except KeyboardInterrupt:
                
                print("Breaking due to keyboard interrupt.")
                break
                                
            except:
                
                # If an exception is raised out of the queue client, crash out of the loop. 
                # A BE process would need to crash out as well - this is the same as a PIKA exception; process needs to crash. 
                print(ExceptionFormatter.get_message())
                break           
             
        end_time = time.time()
            
        print("total time: {0}; total messages: {1}".format(end_time - start_time, message_count))
    
    
    def _run_persisted_connection(self, bridge_connection_information_list, exchange_type):

        # Create and connect ZMQ clients, one for each connection.
        zmq_client_list = list()
        for bridge_ip, bridge_port in bridge_connection_information_list:
            zmq_client_list.append(self._setup_connection(bridge_ip, bridge_port))
                
        zmq_client_list_length = len(zmq_client_list)
        
        #
        import time
        start_time = time.time()
        message_count = 0
        
        while True:
            
            try:
                
                # Sleep.
                time.sleep(0.01)
                
                # Get a random client.
                zmq_client = zmq_client_list[int(random.random() * zmq_client_list_length)]
                
                # Make a new connection and push data.
                self._push(zmq_client, exchange_type)
                message_count += 1             
                if time.time() - start_time > 60:
                    break
                #break

            except KeyboardInterrupt:
                
                print("Breaking due to keyboard interrupt.")
                break
                                
            except:
                
                # If an exception is raised out of the queue client, crash out of the loop. 
                # A BE process would need to crash out as well - this is the same as a PIKA exception; process needs to crash. 
                print(ExceptionFormatter.get_message())
                break           
             
        end_time = time.time()
            
        print("total time: {0}; total messages: {1}".format(end_time - start_time, message_count))
        
        # Disconnect the queue clients.
        for zmq_client in zmq_client_list:
            zmq_client.disconnect()
    
    
    def __init__(self, connection_information, exchange_type, file_name):
        
        # Split the connection information into separate connections.
        split_connection_data = connection_information.split(",")
        bridge_connection_information_list = list()
        for data in split_connection_data:
            split_data = data.split(":")
            bridge_connection_information_list.append((split_data[0], int(split_data[1])))
            
        #
        #self._run_short_lived_connection(bridge_connection_information_list, exchange_type)
        self._run_persisted_connection(bridge_connection_information_list, exchange_type)
        
        
        