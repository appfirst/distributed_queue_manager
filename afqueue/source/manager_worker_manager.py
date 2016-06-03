from afqueue.source.manager_worker import ManagerWorker #@UnresolvedImport
from multiprocessing import Process, Queue #@UnresolvedImport
from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.messages import system_messages #@UnresolvedImport
from afqueue.threads.data_worker_thread import DataWorkerThread #@UnresolvedImport


class ManagerWorkerManager():
    
    def __init__(self, remote_ip_address, qm_id_string, qm_start_up_time, config, worker_router_socket_port, child_thread_notification_queue, logger):
                                                
        # Assign.
        self._remote_ip_address = remote_ip_address
        self._qm_id_string = qm_id_string
        self._qm_start_up_time = qm_start_up_time
        self.config = config
        self._worker_router_socket_port = worker_router_socket_port
        self._action_out_queue = child_thread_notification_queue
        self.logger = logger
               
        # Create trackers
        self._last_thread_index = 0
        self._worker_list = list()
        self._master_thread_name_list = list()
        
        
    def check_master_thread_list(self, thread_name):
        """
        Returns true if the given thread name has ever been seen by the manager; checks both active and inactive.
        """
        
        return thread_name in self._master_thread_name_list
    
    
    def create_and_add_worker(self, redis_connection_string, current_setup_data_message, current_control_data_message):
        """
        Creates and adds a new worker.
        Supply the redis connection string the data worker should use for its shared memory connection.
        Supply valid setup messages to initialize the worker with data; supply None to do no initialization.
        Returns the new worker.
        Note the worker's process is not started automatically; worker.process.start() must be called to start the worker.
        """
        
        try:
            
            # Get the thread name; update the tracker.
            thread_name = "{0}.{1}".format(self.config.data_worker_thread_name, self._last_thread_index)
            self._last_thread_index += 1
            
            # Create the action queue and shared memory manager.
            action_in_queue = Queue()
            if current_setup_data_message != None:
                action_in_queue.put(current_setup_data_message)
            if current_control_data_message != None:
                action_in_queue.put(current_control_data_message)
    
            # Create the data worker thread.
            data_worker = DataWorkerThread(thread_name, 
                                    self._remote_ip_address, self._qm_id_string, self._qm_start_up_time, self.config, redis_connection_string,
                                    self._worker_router_socket_port,
                                    action_in_queue, self._action_out_queue)           

            # Create the process.
            data_worker_process = Process(target=data_worker.run_logic, name=thread_name)
            
            # Create the bridge worker; update the tracker.
            manager_worker = ManagerWorker(thread_name, data_worker_process, action_in_queue)
            self._worker_list.append(manager_worker)
            
            # Register in our master thread list.
            self._master_thread_name_list.append(thread_name)
            
            # Return the worker.
            return manager_worker
        
        except:

            raise ExceptionFormatter.get_full_exception()


    def get_workers(self):
        """
        Returns the current list of running workers being managed by the manager.
        """
        
        return self._worker_list
                    

    def get_worker_from_thread_name(self, thread_name):
        """
        Returns the worker which has the given thread name.
        Returns None if no worker exists.
        """

        # Go through all workers to find the worker with the matching thread name.
        for worker in self._worker_list:
            if worker.thread_name == thread_name:     
                return worker
        
        # Return no worker if our loop didn't find one.
        return None
                
                
    def send_message_to_workers(self, message):
        """
        Sends the given command to all active workers.
        """
        
        # Go through all workers and put the message in their action queue.
        for worker in self._worker_list:
            worker.action_queue.put(message)


    def signal_all_stop(self):
        """
        Signals all worker threads to stop via their action queue.
        """
        
        # Go through all workers we are tracking and signal a stop.
        for worker in self._worker_list:
            worker.action_queue.put(system_messages.SystemStopThreadMessage(), False)
            

    def stop_and_remove_workers(self, count):
        """
        Stops and removes up to <count> workers.
        """
        
        process_list = list()
        # For each count given.
        for _ in range(count):
            
            # Get the remaining workers; if we have none left, return out.
            if len(self._worker_list) == 0:
                break
            
            # Get the bridge worker.
            worker = self._worker_list[0]
            
            # Shut down the bridge worker.                        
            worker.should_shutdown = True
            worker.action_queue.put(system_messages.SystemStopThreadMessage(), False)
            process_list.append(worker.process)
            
            # Remove the bridge worker from our list.
            self._worker_list.remove(worker)

        return process_list

    def stop_and_remove_worker_by_thread_name(self, thread_name):
        """
        Stops and removes the worker with the given thread name.
        Raises an exception if no such worker exists.
        """
        
        # Go through all workers to find the worker with the matching thread name.
        for worker in self._worker_list:
            if worker.thread_name == thread_name:                        
                worker.should_shutdown = True
                worker.action_queue.put(system_messages.SystemStopThreadMessage(), False)
                worker.process.join()
                self._worker_list.remove(worker)
                return
                
        # If we didn't return from the above loop, we did not find a matching worker to the thread name given.
        raise Exception("Worker does not exist: {0}".format(thread_name))
                
                
    def update_workers_shared_memory_connections(self, shared_memory_connection_string_dict):
        """
        Sends a command to all workers to synchronize PQM shared memory storage connections to the given list.
        """
        
        # Go through all workers to find the worker with the matching thread name.
        for worker in self._worker_list:
            worker.action_queue.put(system_messages.SystemUpdateSharedMemoryConnectionsMessage(shared_memory_connection_string_dict))
                
                
    def update_workers_pqm_queue_access_dictionaries(self, pqm_queue_access_dictionary):
        """
        Sends the given list of queues in the PQM's shared memory which workers should have access to to all data workers.
        """
        
        # Go through all workers to find the worker with the matching thread name.
        for worker in self._worker_list:
            worker.action_queue.put(system_messages.SystemSetPqmQueueAccessDataMessage(pqm_queue_access_dictionary))


    def verify_all_stopped(self):
        """
        Verifies all workers are stopped.
        Note: This method will block until all workers have signaled they have stopped.  
        """
        
        # Go through all workers we are tracking and signal a stop.
        for worker in self._worker_list:
            worker.process.join()