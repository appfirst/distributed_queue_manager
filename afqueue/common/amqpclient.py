import pika


class AmqpClientError(Exception):
    pass


def create_connection(queue_servers, queue_mode = "rabbitmq_cluster"):
    
    if queue_mode == "rabbitmq_cluster":
        
        credentials = pika.PlainCredentials('guest', 'guest')
        for address in queue_servers:
            addr, port = address.split(":")
            conn = pika.AsyncoreConnection(
                pika.ConnectionParameters(host=addr, port=int(port), credentials=credentials)
            )
            if conn.connection_open:
                return conn
    
    else:
        
        queue_address, queue_port = queue_servers[0].split(":")
        return pika.AsyncoreConnection(
            pika.ConnectionParameters(
                host=queue_address, port=int(queue_port),
                credentials=pika.PlainCredentials("guest", "guest")))
       
       
def _create_callback(callback, queue_mode):

    def amqp_callback(ch, method, header, body):
        callback(body, method.routing_key, header)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    return amqp_callback
    

class AmqpClient(object):

    def __init__(self, conn):
        self.conn = conn
        self.chan = conn.channel()

    def exchange_declare(self, exchange, exchange_type, auto_delete=False):
        self.chan.exchange_declare(exchange=exchange, type=exchange_type, auto_delete=auto_delete)

    def queue_declare(self, qname, exclusive=False, durable=False):
        self.chan.queue_declare(queue=qname, exclusive=exclusive, durable=durable)

    def queue_bind(self, exchange, qname, binding_key=None):
        if binding_key is None:
            self.chan.queue_bind(
                    exchange=exchange, queue=qname)
        else:
            self.chan.queue_bind(
                    exchange=exchange, queue=qname,
                    routing_key=binding_key)

    def publish(self, msg, exchange, routing_key="", properties=None):
        kwargs = {
            "exchange": exchange,
            "routing_key": routing_key,
            "body": msg
        }
        if properties:
            kwargs["properties"] = properties
        self.chan.basic_publish(**kwargs)

    def subscribe(self, callback, queue_name, queue_mode="rabbitmq_cluster"):
        self.chan.basic_qos(prefetch_count=1)
        self.chan.basic_consume(_create_callback(callback, queue_mode), queue=queue_name)


def start(connections, count=100, timeout=1):
    socket_map = {}
    for conn in connections:
        socket_map[conn.dispatcher._fileno] = conn.dispatcher
    pika.asyncore_loop(socket_map, count=count, timeout=timeout)
    
