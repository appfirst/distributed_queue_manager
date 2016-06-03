from afqueue.source.bridge_worker import BridgeWorker #@UnresolvedImport
from multiprocessing import Process, Queue
from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.messages import system_messages #@UnresolvedImport
from afqueue.threads.bridge_worker_thread import bridge_worker_thread #@UnresolvedImport
from afqueue.threads.bridge_pika_worker_thread import bridge_pika_worker_thread #@UnresolvedImport


class BridgeWorkerManager():
    
    def __init__(self, config, zmq_context, worker_router_socket_port, child_thread_notification_queue, logger):
        
        
        self.config = config
        self._zmq_context = zmq_context
        self._worker_router_socket_port = worker_router_socket_port
        self._child_thread_notification_queue = child_thread_notification_queue
        self.logger = logger
        
        self._connection_string_last_thread_index_map = dict()
        self._connection_string_workers_map = dict()
        self._master_thread_name_list = list()
        
    
    def create_and_add_worker(self, connection_string):
        
        try:
            
            # Get the next thread index; update the tracker.
            thread_index = self._connection_string_last_thread_index_map.get(connection_string, 0) + 1
            self._connection_string_last_thread_index_map[connection_string] = thread_index
            
            # Get the thread name.
            thread_name = connection_string + "_" + str(thread_index)
            
            # Create the process.
            notification_queue = Queue()
            data_worker_process = Process(target=bridge_worker_thread, 
                                             args=(thread_name, 
                                                   self._zmq_context, 
                                                   self._worker_router_socket_port,
                                                   connection_string, self.config.bridge_worker_remote_poll_timeout,
                                                   notification_queue, self._child_thread_notification_queue,),
                                             name=thread_name)
            
            # Create the bridge worker; update the tracker.
            bridge_worker = BridgeWorker(thread_name, connection_string, data_worker_process, notification_queue)
            self._connection_string_workers_map.setdefault(connection_string, list()).append(bridge_worker)
            
            # Register in our master thread list.
            self._master_thread_name_list.append(thread_name)
            
            # Return the worker.
            return bridge_worker
        
        except:

            raise ExceptionFormatter.get_full_exception()
    
    
    def create_and_add_pika_worker(self, connection_string, queue_mode):
        
        try:
            
            # Get the next thread index; update the tracker.
            thread_index = self._connection_string_last_thread_index_map.get(connection_string, 0) + 1
            self._connection_string_last_thread_index_map[connection_string] = thread_index
            
            # Get the thread name.
            thread_name = connection_string + "_" + str(thread_index)
            
            # Create the process.
            notification_queue = Queue()
            data_worker_process = Process(target=bridge_pika_worker_thread, 
                                             args=(thread_name, 
                                                   self._zmq_context, 
                                                   self._worker_router_socket_port,
                                                   connection_string, queue_mode, self.config.bridge_worker_remote_poll_timeout,
                                                   notification_queue, self._child_thread_notification_queue,
                                                   self.logger),
                                             name=thread_name)
            
            # Create the bridge worker; update the tracker.
            bridge_worker = BridgeWorker(thread_name, connection_string, data_worker_process, notification_queue)
            bridge_worker.pika_queue_mode = queue_mode
            self._connection_string_workers_map.setdefault(connection_string, list()).append(bridge_worker)
            
            # Register in our master thread list.
            self._master_thread_name_list.append(thread_name)
            
            # Return the worker.
            return bridge_worker
            
        except:

            raise ExceptionFormatter.get_full_exception()
        
        
    def check_master_thread_list(self, thread_name):
        
        return thread_name in self._master_thread_name_list
                    

    def get_bridge_workers(self, connection_string):
        
        return self._connection_string_workers_map.get(connection_string, list())
                    

    def get_bridge_worker_from_thread_name(self, thread_name):

        # Go through all bridge workers to find the worker with the matching thread name.
        for bridge_worker_list in list(self._connection_string_workers_map.values()):
            for bridge_worker in bridge_worker_list:
                if bridge_worker.thread_name == thread_name:     
                    return bridge_worker
        
        # Return no worker if our loop didn't find one.
        return None


    def signal_all_stop(self):
        
        # Go through all bridge workers we are tracking and signal a stop.
        for bridge_worker_list in list(self._connection_string_workers_map.values()):
            for bridge_worker in bridge_worker_list:
                bridge_worker.action_queue.put(system_messages.SystemStopThreadMessage(), False)


    def verify_all_stopped(self):
        
        # Go through all bridge workers we are tracking and signal a stop.
        for bridge_worker_list in list(self._connection_string_workers_map.values()):
            for bridge_worker in bridge_worker_list:
                bridge_worker.process.join()
            

    def stop_and_remove_bridge_workers_by_connection_string(self, connection_string, count):
                
        try:
            
            # For each count given.
            for _ in range(count):
                
                # Get the remaining workers; if we have none left, return out.
                bridge_worker_list = self._connection_string_workers_map.get(connection_string, list())
                if len(bridge_worker_list) == 0:
                    return
                
                # Get the bridge worker.
                bridge_worker = bridge_worker_list[0]
                
                # Shut down the bridge worker.                        
                bridge_worker.should_shutdown = True
                bridge_worker.action_queue.put(system_messages.SystemStopThreadMessage(), False)
                bridge_worker.process.join()
                
                # Remove the bridge worker from our list.
                self._connection_string_workers_map[connection_string].remove(bridge_worker)
        
        except:
            
            raise ExceptionFormatter.get_full_exception()    


    def stop_and_remove_bridge_worker_by_thread_name(self, thread_name):
        """
        Finds the bridge worker with the given thread name.
        Denotes the worker should shut down, ensures it is stopped/stopping via a stop thread command, joins the thread, and removes the worker from tracking.
        Returns the worker object.
        """
        
        try:
            
            # Go through all bridge workers to find the worker with the matching thread name.
            for bridge_worker_list in list(self._connection_string_workers_map.values()):
                for bridge_worker in bridge_worker_list:
                    if bridge_worker.thread_name == thread_name:                        
                        bridge_worker.should_shutdown = True
                        bridge_worker.action_queue.put(system_messages.SystemStopThreadMessage(), False)
                        bridge_worker.process.join()
                        bridge_worker_list.remove(bridge_worker)
                        return bridge_worker
                    
            # If we didn't return from the above loop, we did not find a matching worker to the thread name given.
            return None#raise Exception("Bridge worker does not exist: {0}".format(thread_name))
        
        except:
            
            raise ExceptionFormatter.get_full_exception()     