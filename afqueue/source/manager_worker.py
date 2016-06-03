class ManagerWorker():
    
    def __init__(self, thread_name, process, action_queue):
        
        self.thread_name = thread_name
        self.process = process
        self.action_queue = action_queue
        self.should_shutdown = False