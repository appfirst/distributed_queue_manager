class BridgeWorker():
    
    def __init__(self, thread_name, connection_string, process, action_queue):
        
        self.thread_name = thread_name
        self.connection_string = connection_string
        self.process = process
        self.action_queue = action_queue
        self.should_shutdown = False
        self.pika_queue_mode = None
        
        
    def is_pika(self):
        
        return self.queue_mode != None