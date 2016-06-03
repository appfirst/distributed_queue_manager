from queue import Empty as EmptyQueueException #@UnresolvedImport
from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.socket_wrapper import SocketWrapper #@UnresolvedImport
from afqueue.common.zmq_utilities import ZmqUtilities #@UnresolvedImport
from afqueue.data_objects.exchange_wrapper import ExchangeWrapper #@UnresolvedImport
from afqueue.data_objects.peer_queue_manager import PeerQueueManager #@UnresolvedImport
from afqueue.messages import client_messages, command_messages, peer_messages, system_messages #@UnresolvedImport
from afqueue.messages.base_message import BaseMessage #@UnresolvedImport
from multiprocessing import Process, Queue#, Manager #@UnresolvedImport
from afqueue.manager_threads.data_broker_thread import DataBrokerThread #@UnresolvedImport
#from afqueue.threads.peer_request_thread import peer_request_thread #@UnresolvedImport
from afqueue.threads.manager_command_request_thread import MannagerCommandRequestThread  #@UnresolvedImport
import afqueue.messages.message_types as message_types #@UnresolvedImport
from afqueue.threads.pqm_incoming_message_thread import pqm_incoming_message_thread #@UnresolvedImport
from afqueue.threads.pqm_outgoing_message_thread import pqm_outgoing_message_thread #@UnresolvedImport
from afqueue.source.manager_worker_manager import ManagerWorkerManager #@UnresolvedImport
from afqueue.source.shared_memory_manager import SharedMemoryManager, RedisConnectionFailureException, RedisConnectionUnknownException #@UnresolvedImport
from afqueue.common.utility import Utility #@UnresolvedImport
import time, datetime, random, copy #@UnresolvedImport
import zmq #@UnresolvedImport
#from afqueue.threads.data_worker_thread import DataWorkerThread #@UnresolvedImport
import bson #@UnresolvedImport


class QueueManager():       
    
 
    def __init__(self, remote_ip_address, base_port, peer_connection_address_list, config, logger, statsd_library, redis_connection_string):
        
        try:
            
            # Tag the start up time of this QM.
            # Race conditions within the QM network are decided based on age.
            # The start up time of each QM is shared with peers to assist in this process.
            # Note that we use a 2 decimal float version of time to get a more exact value (less conflicts).
            self._start_up_time = float(int(time.time() * 100)) / 100
            
            # Assign.
            self.remote_ip_address = remote_ip_address
            self.base_port = base_port
            self.config = config
            self.logger = logger
            self.statsd_library = statsd_library
            self.redis_connection_string = redis_connection_string
            
            # Denote not shut down.
            self.is_running = True
            self.shut_down_ran = False
            
            # Set initial synchronization and monitoring timers.
            current_time_stamp = self._get_current_time()
            self._last_synchronize_queue_sizes_time = current_time_stamp
            self._last_synchronize_worker_pqm_access_queue_lists = current_time_stamp    
            self._last_shared_memory_monitoring_time = -1      
            self._last_ordered_queues_exhausted_owners_check_time = -1
    
            # Set up our socket information.
            self._qm_socket_ip_address = remote_ip_address
            self._data_client_request_socket_port = base_port
            self._pqm_outgoing_socket_port = self._data_client_request_socket_port + 1
            self._data_worker_router_socket_port = self._data_client_request_socket_port + 2
            self._command_request_socket_port = self._data_client_request_socket_port + self.config.manager_command_request_port_offset
            
            # Set our ID string.
            # Each QM has an ID string, based on its IP address and primary request port.
            self._id_string = self._get_id_string_from_connection_details(self._qm_socket_ip_address, self._data_client_request_socket_port)      
                        
            #
            # Create our shared memory constructs.
            #
            
            # Initialize shared memory.
            self._shared_memory_manager = SharedMemoryManager(self.redis_connection_string)
            
            # Always dump the pecking order list on startup; we need to rebuild it during our discovery.
            self._shared_memory_manager.set_pecking_order_list(list())
            
            # Always dump the queue lock data.
            # A new master QM will inherently need a fresh list; a new slave QM will inherently be given the latest lock data from the master upon connection.
            self._shared_memory_manager.set_queue_lock_dict(dict())
            
            # Always clear all previous connection reply data on start up - old data could result in incorrect initial data being sent throughout the system.
            self._shared_memory_manager.clear_connection_reply_data()

            #
            # Create our local memory copies of data in shared memory.
            #
            
            # Create our peer dictionary.
            self._peer_queue_manager_dict = dict()
            
            # Object trackers.
            self._current_exchange_wrapper_dict = dict()
            self._current_data_queue_wrapper_dict = dict()
            
            # Pecking order. 
            # The order at which QMs entered the system.  The first element is always the master QM.
            self._current_pecking_order_list = list()
            
            # The current amount of shared memory this QM's shared memory store is using.
            self._current_shared_memory_bytes_used = self._shared_memory_manager.get_memory_usage()
            
            # The current list of all QM IDs which are accepting data; QMs on this list have room in their shared memory storage for more data. 
            self._current_accepting_data_owner_id_list = list()
            
            # The current owner list for each ordered queue in the system; key: queue name; value: ordered list of QMs which have owned data for the queue.
            self._current_ordered_queue_owners_dict = dict()
            
            # The current exhausted owner list for each queue in the system; key: queue name; value: list of QMs which have been reported as having no data remaining for the queue.
            # Data worker threads will report ordered queues as exhausted when any but the most recent owner of the queue has no data remaining.
            # The master QM will use this information to test and send validation out to the system on the true exhausted state.
            self._current_exhausted_queue_owner_dict = dict() 
            
            # The current lock owners for ordered queues within the system.
            # The master QM will prioritize lock owners owning ordered queues.
            self._current_ordered_queues_lock_owner_dict = dict()
                        
            # X 
            self._current_routing_key_rejection_list = list()
            self._current_push_rejection_queue_name_set = set()
                
            # Create our queue size snapshot trackers.
            self._queue_size_snapshot_dict = dict()
            self._queue_size_snapshot_delta_dict = dict()
            
            # When a PQM is disconnected, we must make sure its shared memory has no queue data in it.
            # Maintain a list PQM ID strings which have been disconnected from us and we have no verified their shared memory has been depleted of data.
            # Note that all disconnected PQMs ID Strings are added to this list and then removed once it has been verified that their shared memory has been depleted.
            self._remote_shared_memory_to_deplete_list = list()
            
            #
            self._current_frozen_push_queue_list = list()
            self._current_frozen_pull_queue_list = list()
            
            #
            # Set up our threads and sockets.
            #
    
            # Create the thread process ID tracker.
            self._thread_process_id_dict = dict() 
                
            # Always dump the thread name dictionary to an empty dictionary on start up; it will be populated as threads start up / shut down.
            self._dump_thread_name_dict()
        
            # Create the ZMQ context.
            self._zmq_context = zmq.Context(1)
                            
            # Create the action queue all child processes will place messages in when they want the main thread to perform an action.
            self._child_process_action_out_queue = Queue()
            
            # Translate the peer connection address(es).
            if peer_connection_address_list == None:
                peer_connection_address_list = self.config.manager_default_peer_list
            elif peer_connection_address_list == "":
                peer_connection_address_list = []
            else:
                peer_connection_address_list = peer_connection_address_list.split(" ")
           
            # Split the connection addresses into tuples of IPs and ports.
            peer_connection_tuple_list = []
            for peer_connection_address in peer_connection_address_list:
                split_data = peer_connection_address.split(":")
                peer_connection_tuple_list.append((split_data[0], int(split_data[1])))

            # Discover and create peers.
            peer_connection_tuple_list, master_flag = self._discover_peer_queue_managers(peer_connection_tuple_list)
            for peer_connection_tuple in peer_connection_tuple_list:
                self._add_pqm(peer_connection_tuple[0], peer_connection_tuple[1])
                
            # If this is the master QM, ensure we are set as such in the pecking order.
            # Also ensure we are in the available ordered owner data list.
            if master_flag == True:
                self._shared_memory_manager.add_qm_to_pecking_order_list(self._id_string)
                #self._current_accepting_data_owner_id_list.append(self._id_string) REMOVED - set flag to check and add us right away.
            
            # X (read thread method's description for processing information).
            """
            NOTE: Peer request thread is not used currently - no message types being sent across, so we won't waste resources running it.
                  Make sure to start/stop the process if use becomes necessary again.
            self._peer_request_action_queue = Queue()
            self._peer_request_process = Process(target=peer_request_thread, 
                                                 args=(self.config.peer_request_thread_name, 
                                                       self._zmq_context, None,
                                                       self._peer_request_action_queue, 
                                                       self._child_process_action_out_queue,),
                                                 name=self.config.peer_request_thread_name)
            """
            
            # Create the command thread; utilized to receive commands from external sources.
            self._command_request_action_queue = Queue()
            self._manager_command_request_thread = MannagerCommandRequestThread(self.config.manager_command_request_thread_name, 
                                                      self.config, self.redis_connection_string, 
                                                      self._command_request_socket_port, 
                                                      self._command_request_action_queue, self._child_process_action_out_queue)
            
            # Create the peer queue manager outgoing message thread (read thread method's description for processing information).
            self._pqm_outgoing_action_queue = Queue()
            self._pqm_outgoing_message_process = Process(target=pqm_outgoing_message_thread, 
                                                         args=(self.config.pqm_outgoing_message_thread_name, 
                                                               self._pqm_outgoing_socket_port, 
                                                               self._pqm_outgoing_action_queue, 
                                                               self._child_process_action_out_queue,),
                                                         name=self.config.pqm_outgoing_message_thread_name)
            
            # Create the peer queue manager incoming message thread (read thread method's description for processing information).
            self._pqm_incoming_action_queue = Queue()
            self._pqm_received_message_queue = Queue()
            self._pqm_incoming_message_process = Process(target=pqm_incoming_message_thread, 
                                                         args=(self.config.pqm_incoming_message_thread_name, 
                                                               self._id_string, 
                                                               self.config.pqm_incoming_message_poll_interval, self.config.pqm_incoming_message_no_activity_sleep,
                                                               self._pqm_incoming_action_queue, 
                                                               self._child_process_action_out_queue, 
                                                               self._pqm_received_message_queue,),
                                                         name=self.config.pqm_incoming_message_thread_name)
                                    
            # Create the data broker thread (read thread method's description for processing information). 
            self._data_broker_action_queue = Queue()
            self._data_broker_process = DataBrokerThread(self.config.data_broker_thread_name, 
                                                         self.config, self.config.zmq_poll_timeout,
                                                         self._data_client_request_socket_port, self._data_worker_router_socket_port,
                                                         self._data_broker_action_queue, self._child_process_action_out_queue,)
                        
            # Create the bridge worker manager.
            self._data_worker_manager = ManagerWorkerManager(self.remote_ip_address, self._id_string, self._start_up_time, 
                                                             self.config, self._data_worker_router_socket_port, self._child_process_action_out_queue, self.logger)
            
            # Create and the worker threads; don't send current setup and control data - we'll do that when we're done initializing.
            for _ in range(self.config.manager_data_worker_thread_count):
                worker = self._data_worker_manager.create_and_add_worker(self.redis_connection_string, None, None)
                
            # Start our process threads.
            #self._peer_request_process.start()    
            self._manager_command_request_thread.start()
            self._pqm_outgoing_message_process.start()
            self._pqm_incoming_message_process.start()
            self._data_broker_process.start()
            for worker in self._data_worker_manager.get_workers():
                worker.process.start()                
                
            # We should give our threads some time to start up and the handle all notifications from them before proceeding.
            time.sleep(0.5)
            
            # We are only interested in handling system notifications during initialization - we want to crash out if a system message tells us to.
            # Exhaust the queue, capturing messages appropriately.
            system_message_list = list()
            non_system_message_list = list()
            while True:
                
                try:
                    
                    message = self._child_process_action_out_queue.get(False)
                    if message.get_primary_type() == message_types.SYSTEM:
                        system_message_list.append(message)
                    else:
                        non_system_message_list.append(message)
            
                except EmptyQueueException:
                    
                    # Break from the loop; there are no more messages to handle.
                    break
                
                except:
                    
                    raise ExceptionFormatter.get_full_exception()
            
            # Re-queue all system messages and handle them.
            failed_threads = []
            for system_message in system_message_list:
                
                # Re-queue.
                self._child_process_action_out_queue.put(system_message)
                
                # Track if we had a failure.
                try:
                    if system_message.started_flag == False:
                        failed_threads.append(system_message.thread_name)
                except:
                    pass
              
            # Handle all notifications
            self._handle_notifications()

            # Fail out if any threads failed.
            if len(failed_threads) > 0:
                raise Exception("Could not initialize properly.  Threads failed to start: {0}".format(failed_threads))
            
            # Connect each PQM (push data socket, dealer socket)
            current_time_stamp = int(time.time())
            current_qm_dealer_id_tag = ZmqUtilities.get_dealer_id_tag(self._id_string, current_time_stamp)
            for peer_queue_manager in list(self._peer_queue_manager_dict.values()):     
                self._connect_peer_queue_manager(peer_queue_manager, current_qm_dealer_id_tag, True)                
                
            # Ensure we are completely synchronized after all initialization code has ran.
            if master_flag == True:
                
                # The master needs to synchronize with shared memory, update ordered queue data, then update the workers.
                self._synchronize_setup_data(False)
                self._current_ordered_queue_owners_dict = self._shared_memory_manager.get_ordered_queue_owners_dict()
                self._update_current_accepting_data_owner_id_list(True, False)
                self._send_current_setup_data_to_workers()
                
            else:
                
                # Slaves can synchronize with shared memory and update data workers immediately.
                self._synchronize_setup_data(True)
                
            # Synchronize control data.
            self._synchronize_control_data(True)
            self._synchronize_queue_sizes(current_time_stamp)
                
            # Re-queue non system messages.
            for non_system_message in non_system_message_list:
                message = self._child_process_action_out_queue.put(non_system_message)
                    
                
        except:
            
            # Capture the exception message.
            exception_message = "Initialization failed: " + ExceptionFormatter.get_message()
            
            # Ensure we call shut down to destroy any sockets we may have created.
            try:
                self.shut_down()
            except:
                pass
            
            # Raise out.
            raise Exception(exception_message)
    
        
    def run_loop(self):

        try:

            # Forward to the loop handler; no other logic for now.
            return self._handle_loop()
        
        except:

            raise ExceptionFormatter.get_full_exception()
    
    
    def shut_down(self):
        
        # Notify.
        self.logger.log_info("AFQM Shutdown method called.")
        
        # Denote no longer running.
        self.is_running = False
        
        # Send a message to all connected PQMs to signal we are going off-line.
        self._send_message_to_connected_pqms(list(self._peer_queue_manager_dict.values()), peer_messages.PeerOfflineMessage(None))

        # Shut down the AFQM components.
        self.logger.log_info("Stopping main request socket process.")
        
        # Send shut down messages to each thread.            
        self._command_request_action_queue.put(system_messages.SystemStopThreadMessage(), False)
        self._pqm_outgoing_action_queue.put(system_messages.SystemStopThreadMessage(), False)
        self._pqm_incoming_action_queue.put(system_messages.SystemStopThreadMessage(), False)
        self._data_broker_action_queue.put(system_messages.SystemStopThreadMessage(), False)
        self._data_worker_manager.signal_all_stop()
            
        #self._peer_request_process.join()
        #self._command_request_action_queue.put(system_messages.SystemStopThreadMessage(), False)
        self._manager_command_request_thread.stop()
        self._pqm_outgoing_message_process.join()
        self._pqm_incoming_message_process.join()
        self._data_broker_process.stop()
        self._data_worker_manager.verify_all_stopped()
                    
        # Handle remaining messages
        self._handle_loop()    
    
        # Shut down our main context.
        self._zmq_context.term()
                
        # Notify completion.
        self.logger.log_info("Shut down complete.")
        
        # Set shut down.
        self.shut_down_ran = True
    
    
    def _add_monitored_remote_shared_memory(self, pqm_id_string):
        """
        Adds the given PQM ID String to the list of remote shared memories which will be monitored until depleted.
        This tracker is used to track shared memory which no longer has a PQM connected to it.
        Note this method will forcibly update all data workers with the latest remote shared memory connections.
        """
        
        # Update the tracker.
        self._remote_shared_memory_to_deplete_list.append(pqm_id_string)
        
        # Synchronize the workers.
        self._synchronize_worker_remote_shared_memory_connections()
            
    
    def _add_pqm(self, peer_ip_address, peer_request_port):
        """
        Creates and adds a new peer queue manager with the given connection information.
        Note that this method does not connect to the peer queue manager.
        """
        
        try:
        
            # Create the peer queue manager object.
            peer_queue_manager = PeerQueueManager(peer_ip_address, peer_request_port, peer_request_port + 1)
            
            # Add the PQM to our tracker dictionary and list.
            self._peer_queue_manager_dict[peer_queue_manager.get_id_string()] = peer_queue_manager
            
        except:
            
            raise ExceptionFormatter.get_full_exception()    
    
    
    def _add_qm_to_accepting_data_list(self, added_qm_id_string, synchronize_workers_on_change):
        """
        Adds the given QM ID string to the list of available ordered queue owners.
        Handles the effects of making a new QM available for ownership.  
        """
            
        try:
            
            # Only allow an addition on a QM which wasn't already denoted as accepting data.
            if added_qm_id_string not in self._current_accepting_data_owner_id_list:
                
                # Add the owner to the available list.
                self._current_accepting_data_owner_id_list.append(added_qm_id_string)
               
                # Update from changes.
                change_made = False
                                            
                # Clear unused queue owners.
                queues_cleared_set = self._clear_exhausted_ordered_queue_owners(False)
                if len(queues_cleared_set) > 0:
                    change_made = True
                
                # Handle a new owner accepting data for ordered queues.
                if len(self._current_ordered_queue_owners_dict) > 0:
                    
                    # Balance the ordered queue owners with the added QM.
                    # Track the set of queues which had the added QM assigned as their current owner.
                    assigned_queue_set = self._balance_ordered_queue_owners_with_added_qm(added_qm_id_string)
                    
                    # Check all the current push rejected queues to see if the changes made during the balancing resulted in them having an owner which is accepting data.
                    remove_from_rejection_list = list()
                    for queue_name in self._current_push_rejection_queue_name_set:
                        owner_id_list = self._current_ordered_queue_owners_dict.get(queue_name, list())
                        if len(owner_id_list) > 0 and owner_id_list[-1] in self._current_accepting_data_owner_id_list:
                            remove_from_rejection_list.append(queue_name)
                
                    # If we should remove some currently push rejected queues, do so.
                    if len(remove_from_rejection_list):
                        self._remove_queues_from_push_rejection_set(remove_from_rejection_list, False)
                        change_made = True
                        
                    # If we changed our queue owner dictionary, do so.
                    if len(assigned_queue_set) > 0:
                        self._shared_memory_manager.set_ordered_queue_owners_dict(self._current_ordered_queue_owners_dict)
                        change_made = True
    
                # If a change was made, update.
                if change_made == True:
                    
                    # Synchronize if we should.
                    if synchronize_workers_on_change == True:
                        self._synchronize_control_data(True)
                    
                # Log.
                self._log_accepting_data_owners()
                self.logger.log_info("Added QM to accepting data list processed.")
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _add_queue_names_to_ordered_queue_owner_dict(self, queue_name_list, synchronize_control_data): 
        """
        Adds the given queues to the current ordered queue owner dictionary.
        Will assign QM owners for each new queue.
        If no new available owners are available for a queue, no data can be written to that queue; the queue will be added to the push rejection list.
        Master method; does not update peers.
        """       
            
        try:
        
            # Get the free space dictionary for all shared memories for ordered queue owners
            qm_free_space_dict = self._get_available_owners_shared_memory_free_space_dictionary()
            
            # If there are not items in our sorted list or the last item in the list has no free space, we have no available QMs for these new queues.
            # Add all the queues to our rejection list.
            if len(qm_free_space_dict) == 0:
                
                # Add; pass our synchronization flag along so we don't have to test for synchronization here.
                self._add_queue_names_to_push_rejection_set(queue_name_list, synchronize_control_data)
            
            # If there are items in our sorted list, assign.
            else:
                
                # Equalize.
                #test_value = 500
                #equalized_dict = dict()
                #for qm_id_string, free_space in qm_free_space_dict.items():
                #    qm_free_space_dict[qm_id_string] = (free_space / test_value) * test_value
                    
                # Sort our free size dictionary by the QMs with the most space free to the QMs with the most space free (we will pop the last item off as we assign).
                
                # Normalize the free space values into a dictionary, keyed off the possible values, valued off a list of QMs with that space available.
                equalized_dict = dict()
                for qm_id_string, free_space in list(qm_free_space_dict.items()):
                    normalized_value = (free_space / self.config.manager_shared_memory_system_normalization_size) * self.config.manager_shared_memory_system_normalization_size
                    equalized_dict.setdefault(normalized_value, list()).append(qm_id_string)
                
                # Sort by the possible normalized values.
                sorted_free_space_tuples = sorted(list(equalized_dict.items()), key=lambda x: x[1])
                
                # Randomize.
                sorted_free_space_tuples_master = list()
                for free_space, qm_id_string_list in list(equalized_dict.items()):
                    random.shuffle(qm_id_string_list)
                    for qm_id_string in qm_id_string_list:
                        sorted_free_space_tuples_master.append((qm_id_string, free_space))
                
                # Go through each queue, assign in round-robin order.
                no_new_owner_available_list = list()
                sorted_free_space_tuples = list(sorted_free_space_tuples_master)
                for queue_name in queue_name_list:                        
                    
                    # Refresh our sorted free space tuples if we have no items left to pop.
                    # Sort our free size dictionary by the QMs with the most space free to the QMs with the most space free (we will pop the last item off as we assign).
                    if len(sorted_free_space_tuples) == 0:
                        sorted_free_space_tuples = list(sorted_free_space_tuples_master)
                        #sorted_free_space_tuples = sorted(qm_free_space_dict.items(), key=lambda x: x[1])
                
                    # Get the current owner list.
                    current_owner_id_list = self._current_ordered_queue_owners_dict.get(queue_name, list())
                    
                    # Find the first owner which is not in our current owner list.
                    valid_tuple = None
                    for test_tuple in sorted_free_space_tuples:
                        if test_tuple[0] not in current_owner_id_list:
                            valid_tuple = test_tuple
                            break
                    
                    # If no QM ID string was found, we could not assign a new owner for this queue and must put it on our rejection list.
                    if valid_tuple == None:
                        no_new_owner_available_list.append(queue_name)
                    
                    # If a QM ID string was found, assign it and remove the entry in our tracker.
                    else:
                        self._current_ordered_queue_owners_dict.setdefault(queue_name, list()).append(valid_tuple[0])
                        sorted_free_space_tuples.remove(valid_tuple)
                        
                # If we had queues we could not find a new owner for, register on our rejection list.
                if len(no_new_owner_available_list) > 0:
                    self._add_queue_names_to_push_rejection_set(no_new_owner_available_list, False)
                                                    
                # Write the results to shared memory.
                self._shared_memory_manager.set_ordered_queue_owners_dict(self._current_ordered_queue_owners_dict)

                # Synchronize if we should.
                if synchronize_control_data == True:
                    self._synchronize_control_data(True)
                                               
            # Log.
            self.logger.log_info("Add queue to ordered queue owners dictionary processed.")
            self._log_ordered_queue_owners()
                
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
        
    def _add_queue_names_to_push_rejection_set(self, queue_name_list, synchronize_control_data):
        """
        Adds the given queue names to the set of queues which currently should not receive data.
        Master method; does not update peers.        
        Returns true/false depending on if a change was actually made or not.
        """
            
        try:

            # Convert the list to a set.
            queue_name_set = set(queue_name_list)
            
            # Determine if a change will be made.
            change_required = not queue_name_set.issubset(self._current_push_rejection_queue_name_set)
            if change_required == True:
            
                # Update.
                self._current_push_rejection_queue_name_set = self._current_push_rejection_queue_name_set.union(queue_name_set)
            
                # Synchronize.
                if synchronize_control_data == True:
                    self._synchronize_control_data(True)
                    
                # Log.
                self._log_push_rejection_queues()
            
            # Return that a change was made.
            return change_required
    
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
    
    def _balance_ordered_queue_owners_with_added_qm(self, added_qm_id_string):
        """
        Balances the ordered queue owners based off a new QM being added to the list of QMs accepting data.
        * The added QM ID string should already be added into the list of QMs currently accepting data before calling this method.
        Returns the set of queues which were assigned to the added QM.
        """
        
        try:
            
            # We will set the added QM as the owner of ordered queues which need a new owner; track which queues have been assigned.
            assigned_queue_set = set()
            
            # If this is the only QM currently accepting data, it means all ordered queues have either no owner or an owner not accepting data.
            # Set this QM as the owner of all queues, provided it is not already in the owner list.
            if len(self._current_accepting_data_owner_id_list) == 1:
                for queue_name, owner_id_list in list(self._current_ordered_queue_owners_dict.items()):
                    if added_qm_id_string not in owner_id_list:
                        owner_id_list.append(added_qm_id_string)
                        assigned_queue_set.add(queue_name)
                        
            # If there is more than one QM, we must balance the system with this added QM.
            else:
                
                # The first step is to ensure all queues which are rejecting data get the added QM as an owner.
                for queue_name in self._current_push_rejection_queue_name_set:
                    if queue_name in list(self._current_ordered_queue_owners_dict.keys()):
                        self._current_ordered_queue_owners_dict.setdefault(queue_name, list()).append(added_qm_id_string)
                        assigned_queue_set.add(queue_name)
                        self.logger.log_info("Balancing ordered queues: Set {0} from rejected list.".format(queue_name))
                        
                # Next, we need to make sure the added QM owns all queues it has locks on.
                for queue_name, owner_id_string in list(self._current_ordered_queues_lock_owner_dict.items()):
                    if owner_id_string == added_qm_id_string:
                        owner_id_list = self._current_ordered_queue_owners_dict.get(queue_name, list())
                        
                        # We have to validate the added QM can actually own the queue.
                        # If there is no owner currently or if the lock owner doesn't own the ordered queue currently, we can potentially set the owner. 
                        if len(owner_id_list) == 0 or owner_id_list[-1] != added_qm_id_string:
                            
                            # The added QM must not already appear in the list to be set as the owner.
                            # If the QM is not the current owner but is in the list, setting it as the owner would create an unordered situation.
                            # IF the QM is the current owner and is in the list, there is nothing to set.
                            if added_qm_id_string not in owner_id_list:
                                self._current_ordered_queue_owners_dict.setdefault(queue_name, list()).append(added_qm_id_string)
                                assigned_queue_set.add(queue_name)
                                self.logger.log_info("Balancing ordered queues: Set {0} because lock owner.".format(queue_name))
                
                # Get the free space dictionary for all shared memories for ordered queue owners; remove the added QM from the free space dictionary.
                qm_free_space_dict = self._get_available_owners_shared_memory_free_space_dictionary()
                added_qm_free_space = qm_free_space_dict[added_qm_id_string]
                del(qm_free_space_dict[added_qm_id_string])                
                
                # Remove all owners which have more free space than the added QM.
                for qm_id_string, free_space in list(qm_free_space_dict.items()):
                    if free_space > added_qm_free_space:
                        del(qm_free_space_dict[qm_id_string])
                                                
                # If there are QMs with less free space than the added QM, continue.
                if len(qm_free_space_dict) > 0:
                
                    # We need the number of queues each QM currently owns.
                    current_queues_owned_dict = dict()
                    for queue_name, owner_id_list in list(self._current_ordered_queue_owners_dict.items()):
                        for owner_id in owner_id_list:
                            current_queues_owned_dict.setdefault(owner_id, list()).append(queue_name)
                    
                    # Get the total owner count, incorporating the new QM into the count if it isn't already present.
                    total_owner_count = len(list(current_queues_owned_dict.keys()))
                    if added_qm_id_string not in list(current_queues_owned_dict.keys()):
                        total_owner_count += 1
                    
                    # Get the current number of ordered queues in the system.
                    # Get the number of queues the added QM should have in the balanced system and use it to determine the number of queues we will assign to it.
                    total_queue_count = len(list(self._current_ordered_queue_owners_dict.keys()))                                    
                    desired_queue_count = int(total_queue_count / total_owner_count)
                    assign_queue_count = desired_queue_count - len(current_queues_owned_dict.get(added_qm_id_string, list()))
                        
                    # If we have queues to assign.
                    if assign_queue_count > 0:
                        
                        # We will want to round robin from our current QMs which own queues based on their free space.
                        # We will pop items off to ensure each QM is equally decremented an owned queue (even if one has drastically less space, we do it in this manner).
                        sorted_free_space_tuples_list = list()    
                        last_loop_queues_assigned_count = -1
                        while assign_queue_count > 0:
                                            
                            # Refresh our sorted free space tuples if we have no items left to pop.
                            if len(sorted_free_space_tuples_list) == 0:
                                
                                # Any time we refresh our sorted list, we must check to see if we actually assigned any queues in the previous loop.
                                # If we haven't, break the loop.
                                # If we have, update our assigned count and process.
                                if len(assigned_queue_set) == last_loop_queues_assigned_count:
                                    self.logger.log_info("Balancing ordered queues: Broke assignment due to no assignments done in last iteration.")
                                    break
                                else:
                                    last_loop_queues_assigned_count = len(assigned_queue_set)
                                
                                # Sort our free size dictionary.
                                # Order with the QM with the most space first, QM with the least space last; we will pop off this list as we assign.
                                sorted_free_space_tuples_list = sorted(list(qm_free_space_dict.items()), key=lambda x: x[1], reverse=True)
                                                    
                            # Determine the QM with the least space.
                            least_space_qm_id_string, _ = sorted_free_space_tuples_list.pop()
                            
                            # Get the set of queues owned the the QM with the least space.
                            current_queues_owned_set = set(current_queues_owned_dict.get(least_space_qm_id_string, list()))
                            
                            # Remove the queues which we have already assigned to the added QM.
                            available_queues_to_switch_set = current_queues_owned_set - assigned_queue_set
                            
                            # If there are no available queues for assignment from the owner, go to the next iteration in the loop.
                            if len(available_queues_to_switch_set) == 0:
                                continue
                            
                            # We want the queue with the fewest number of owners overall which do not already have the added QM as an owner; find now.
                            owner_count = None
                            least_owned_queue_name = None
                            for queue_name in available_queues_to_switch_set:
                                
                                # We can only use queues which aren't currently owned by their queue lock owner.
                                # If there is an owner and it matches the current owner, do not process this queue in our search.
                                owner_id_list = self._current_ordered_queue_owners_dict.get(queue_name, list())
                                if len(owner_id_list) > 0 and owner_id_list[-1] == self._current_ordered_queues_lock_owner_dict.get(queue_name, None):
                                    self.logger.log_info("Balancing ordered queues: Skipped {0} from {1} due to lock status.".format(queue_name, least_space_qm_id_string))
                                    continue
                                     
                                # If we are not already in the owner ID list, consider this queue for ownership.
                                if added_qm_id_string not in owner_id_list:
                                    this_owner_count = len(owner_id_list)
                                    if owner_count == None or this_owner_count < owner_count:
                                        least_owned_queue_name = queue_name
                                        owner_count = this_owner_count
                                        
                            # If we got no owner, reset the loop.
                            if least_owned_queue_name == None:
                                continue
                            
                            # Assign the queue to the new QM.
                            self._current_ordered_queue_owners_dict.setdefault(least_owned_queue_name, list()).append(added_qm_id_string)
                            self.logger.log_info("Balancing ordered queues: Set {0}.".format(least_owned_queue_name))
                            
                            # Register the queue in the assigned set.
                            assigned_queue_set.add(least_owned_queue_name)
                            
                            # Decrement our count.
                            assign_queue_count -= 1
                    
            # Return the queues we assigned.
            return assigned_queue_set
            
        except:
            
            raise ExceptionFormatter.get_full_exception()  
        
    
    def _can_qm_accept_data(self, qm_id_string, allowing_ownership, current_shared_memory_bytes_used, 
                                            ordered_ownership_stop_threshold, ordered_ownership_start_threshold):
        """
        The master QM calls this method to check a QM's ability to accept further data.
        This method will return a boolean if a state change is required; ordered data ownership should be started if True, stopped if False.
        This method will return None if no change is required.
        """
        
        try:
            
            # Check to see if we have exceeded our ordered data threshold.
            # If shared memory has grown too large, we must no longer be the recipient of ordered data.
            # If we haven't previously sent out a stop message...
            if allowing_ownership == True:
                
                # If we have exceeded our stop threshold, return that we should stop.
                if current_shared_memory_bytes_used > ordered_ownership_stop_threshold:
                    return False
                
            # If we have previously sent out a stop message...
            # If we have dipped below our start threshold.
            elif current_shared_memory_bytes_used < ordered_ownership_start_threshold:
                return True
                
            # If we made it here, no state change was warranted.
            return None

        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _check_ordered_queues_exhausted_owners(self):
        """
        Checks the current ordered queues exhausted queue owners tracker to determine if any of the exhausted owners should truly be removed from ownership of a queue.
        Master method; Slaves should not call this method.
        Synchronizes with shared memory, data workers, and peers upon completion.
        """
        
        try:
            
            # If we have ordered queues exhausted owners data ...
            if len(list(self._current_exhausted_queue_owner_dict.items())) > 0:
                                
                # Run the clear command.
                # We don't care if data was changed or not - we have to update the system either way.
                # If the exhausted owners still have data, we need to update all QMs/worker threads so they know to use those owners.
                # If the exhausted owners no longer have data, we need to update all QMs/worker threads with validation.
                queues_cleared_set = self._clear_exhausted_ordered_queue_owners(False)
                
                # We might have to add a QM to the owner list of some of our ordered queues.
                # It's possible that an owner who has been exhausted and removed from an ownership list was already marked as available.
                # In this case, when a queue also has only owners who can't receive more data, we need to make sure this newly available owner is set as a new owner to that queue.
                # Test all queues which are currently being rejected.
                # Only test if we modified our queue owners, have available owners, and have rejected queues.
                if len(queues_cleared_set) > 0 and len(self._current_accepting_data_owner_id_list) > 0 and len(self._current_push_rejection_queue_name_set) > 0:
                    
                    # Get a copy of our available owners as a set for easier comparison.
                    available_owner_id_set = set(self._current_accepting_data_owner_id_list)
                    
                    # Go through each currently rejected queue.
                    for queue_name in self._current_push_rejection_queue_name_set:
                        
                        # Get the set of owner IDs for the rejected queue.
                        current_owner_id_set = set(self._current_ordered_queue_owners_dict.get(queue_name, list()))
                        
                        # Get the potential owners.
                        potential_owner_id_set = available_owner_id_set - current_owner_id_set
                        if len(potential_owner_id_set) > 0:
                            potential_owner_id_list = list(potential_owner_id_set)
                            random.shuffle(potential_owner_id_list)
                            self._current_ordered_queue_owners_dict.setdefault(queue_name, list()).append(potential_owner_id_list[0])
                
                # We might have to remove queues from our rejection list.
                remove_from_rejection_list = list()
                for queue_name in queues_cleared_set:
                    if queue_name in self._current_push_rejection_queue_name_set:
                        owner_id_list = self._current_ordered_queue_owners_dict.get(queue_name, list())
                        if len(owner_id_list) > 0 and owner_id_list[-1] in self._current_accepting_data_owner_id_list:
                            remove_from_rejection_list.append(queue_name)
                        
                # If we should remove some currently push rejected queues, do so.
                if len(remove_from_rejection_list):
                    self._remove_queues_from_push_rejection_set(remove_from_rejection_list, False)
                              
                # If we made a change to our queue owners dictionary, update shared memory.
                if len(queues_cleared_set) > 0:
                    self._shared_memory_manager.set_ordered_queue_owners_dict(self._current_ordered_queue_owners_dict)
                    
                # Synchronize workers.
                self._synchronize_control_data(True)
                
                # Clear out our current tracker.
                self._current_exhausted_queue_owner_dict.clear()
                
                # Send to peers.
                self._send_master_control_data_message(list(self._peer_queue_manager_dict.values()))
                                
                # Log.
                self.logger.log_info("Checked ordered queue exhausted owners.")
                self._log_exhausted_queue_owners()
                        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
    
    def _clear_exhausted_ordered_queue_owners(self, synchronize_control_data):
        """
        Check all ordered queue owners and removes any which can no longer be pushed to (ie, aren't the most recent owner) yet have no data in their queues.
        Master method; does not update peers.
        Returns a set of all queues which had at least one owner cleared.
        Does not check any queues which had owner(s) removed to determine if they should be removed from the queue push rejection list (if they are even on the list).
        """
        
        try:
            
            # Build the set of all owners.
            check_queue_size_set = set()
            for owner_id_list in list(self._current_ordered_queue_owners_dict.values()):
                check_queue_size_set = check_queue_size_set.union(set(owner_id_list))
                
            # Get the queue size information from all owners we tracked.
            qm_queue_size_dict, _ = self._query_remote_shared_memory(list(check_queue_size_set), self._shared_memory_manager.get_queue_sizes)     
            
            # Track any queues which had an owner removed from them.
            queue_owner_change_set = set()
                
            # Clear out unused owners which can't be written to anymore.  
            for queue_name, owner_id_list in list(self._current_ordered_queue_owners_dict.items()):
                
                # Check each owner which is not the most recent.
                for qm_id_string in list(owner_id_list[:-1]):
                    
                    # Get the queue size dictionary for the owner.
                    # If no data is returned, we can not proceed.
                    # We can only remove this owner if it appears in our exhausted owners list for this queue.
                    #  When it appears in this list, it means a QM in the system has tried accessing the owner for this queue.
                    #  This means the queue is being actively used, so we must remove it so as to not interrupt data flow.
                    # If the owner doesn't appear in the list, it means no access is being made, so we can not remove it.
                    #  Its possible the queue isn't being read yet on purpose, so we don't want to remove the owner - this could result in data loss. 
                    queue_size_dict = qm_queue_size_dict.get(qm_id_string, None)
                    if queue_size_dict == None:
                        
                        # Check if the queue/owner exist in the exhausted owners list.
                        order_queue_exhausted_owner_id_set = self._current_exhausted_queue_owner_dict.get(queue_name)
                        if order_queue_exhausted_owner_id_set != None:
                            if qm_id_string in order_queue_exhausted_owner_id_set:
                                owner_id_list.remove(qm_id_string)
                                queue_owner_change_set.add(queue_name)
                                self.logger.log_info("Clear exhausted ordered queue owners: Removed owner due to no queue size dictionary - {0} {1}.".format(queue_name, qm_id_string))
                        
                        # Go onto the next iteration.        
                        continue
                    
                    # Get the queue entry in the queue size dictionary; if it doesn't exist, remove the owner and go onto the next owner.
                    queue_size = queue_size_dict.get(queue_name, None)
                    if queue_size == None:
                        owner_id_list.remove(qm_id_string)
                        queue_owner_change_set.add(queue_name)
                        self.logger.log_info("Clear exhausted ordered queue owners: Removed owner due to no queue entry in queue size dictionary - {0} {1}.".format(queue_name, qm_id_string))
                        
                    # If the queue size is zero, remove the owner.
                    elif queue_size == 0:
                        queue_owner_change_set.add(queue_name)
                        owner_id_list.remove(qm_id_string)
                        self.logger.log_info("Clear exhausted ordered queue owners: Removed owner due to zero queue size in queue size dictionary - {0} {1}.".format(queue_name, qm_id_string))
                                        
            # If we made a change.    
            if len(queue_owner_change_set) > 0:
                
                # Write the results to shared memory.
                self._shared_memory_manager.set_ordered_queue_owners_dict(self._current_ordered_queue_owners_dict)
                
                # Synchronize.
                if synchronize_control_data == True:
                    self._synchronize_control_data(True)
                        
            # Log.
            self.logger.log_info("Cleared exhausted ordered queue owners.")
            self._log_ordered_queue_owners()
            
            # Return the set of queue names which have had owners changed.
            return queue_owner_change_set
                
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
                
    def _connect_peer_queue_manager(self, peer_queue_manager, qm_dealer_id_tag, handshake):
        """
        Issues a connection to the given PQMs Request socket.
        Waits for a reply (thread deadlock).
        Logs the acknowledgment message from the PQM.
        Puts a connection message on the incoming PQM data thread's action queue.
        This message will prompt the incoming PQM thread to create a DEALER socket to the ROUTER socket on the PQM.
        This socket relationship will allow the PQM to push data to this QM. 
        """
            
        try:
            
            # Set the QM dealer ID tag.
            peer_queue_manager.set_qm_dealer_id_tag(qm_dealer_id_tag)
            
            # Handshake if we should.
            if handshake == True:
                
                # Handshaking requires us to make a handshake request and wait for a reply.
                # This is a blocking pattern and should only be done on start up of this queue manager.
                
                # Determine the redis connection string we'll send the peer; we have to replace "localhost" with our remote IP address.
                if "localhost" in self.redis_connection_string:
                    redis_connection_string = self.redis_connection_string.replace("localhost", self.remote_ip_address)
                else:
                    redis_connection_string = self.redis_connection_string
                
                # Instruct the peer queue manager to handshake.
                reply_message = peer_queue_manager.handshake(self._zmq_context, peer_messages.build_settings_dictionary(self._id_string, self._start_up_time,
                                                                                                                        redis_connection_string,
                                                                                                                        self.config.manager_shared_memory_max_size,
                                                                                                                        self.config.manager_shared_memory_ordered_data_reject_threshold,
                                                                                                                        self.config.manager_shared_memory_ordered_data_accept_threshold))
                
                # If the PQM told us it is the master during the handshake, set the master information.
                if reply_message.sender_master_flag == True:
                    
                    # Check if we had a master synchronization failure - we have to fail out if this is the case.
                    if reply_message.master_synchronization_failure_flag == True:
                        raise Exception("Master has signaled a synchronization failure during handshaking")
                    
                    # Update the messages with the master's ID string.
                    reply_message.master_control_data_message.sender_id_string = peer_queue_manager.get_id_string()
                    reply_message.master_setup_data_message.sender_id_string = peer_queue_manager.get_id_string()
                    
                    # Handle the data messages from the master.
                    # Note we do not synchronize here - the only time we handshake is during initialization; we know we'll synchronize at the end of that processing.
                    self._handle_peer_master_control_data(reply_message.master_control_data_message, False)
                    self._handle_peer_master_setup_data(reply_message.master_setup_data_message, False)

                # Log out all PQMs.
                self.logger.log_info("Initialized connection with PQM {0}.  Current PQMs: {1}".format(peer_queue_manager.get_id_string(),
                                                                                                      [pqm.get_id_string() for pqm in list(self._peer_queue_manager_dict.values())]))

            # We need to ensure queue data we send to a PQM arrives safely; this requires an immediate acknowledgment from the PQM.
            # Create an action to connect a request socket to the PQM's data broker (reply) socket.
            # Note: Since we required a blocking pattern, the request socket must exist in another thread so our main thread doesn't get blocked.
            #connect_message = system_messages.SystemConnectPeerRequestSocketMessage(peer_queue_manager.get_id_string(), 
            #                                                                        peer_queue_manager.get_data_request_socket_connection_string())
            #self._peer_request_action_queue.put(connect_message)
            
            # We need to grant the PQM the ability to send us data and status information asynchronously.
            # Create an action to connect a dealer socket to the PQM's router socket so data can be pushed to us.
            connect_message = system_messages.SystemPeerConnectionUpdateMessage(True, peer_queue_manager.get_id_string(), 
                                                                                peer_queue_manager.get_dealer_connection_string(), peer_queue_manager.get_qm_dealer_id_tag())
            self._pqm_incoming_action_queue.put(connect_message)
            
            # Denote the PQM as connected.
            peer_queue_manager.set_connected_flag(True, self._get_current_time())
            
            # Make a connection to the peer's shared memory.
            self._shared_memory_manager.add_pqm_connection(peer_queue_manager.get_id_string(), peer_queue_manager.get_shared_memory_connection_string())
            
            # Synchronize workers with the latest PQM shared memory connections.
            self._synchronize_worker_remote_shared_memory_connections()
                            
            # If this PQM was previously on our depletion list, remove it.
            pqm_id_string = peer_queue_manager.get_id_string()
            if pqm_id_string in self._remote_shared_memory_to_deplete_list:
                self._remote_shared_memory_to_deplete_list.remove(pqm_id_string)

            # If this QM is the master QM, we have to do more work with a newly connected PQM.
            if self._is_master(self._id_string) == True:
                
                # Add the PQM's id string into the pecking order of future masters.
                self._shared_memory_manager.add_qm_to_pecking_order_list(pqm_id_string)
                
                # Manage initialization of the PQM's ordered queue ownership status within the system.
                # Force an update, check ownership, and clear unused owners.
                peer_queue_manager.set_ordered_queue_ownership_force_update_flag(True) 
                self._current_shared_memory_bytes_used = self._shared_memory_manager.get_memory_usage()
                self._update_current_accepting_data_owner_id_list(False, False)
                    
                # Synchronize control data.
                # The master has to do this now so it can update the peers; other QMs will receive this updated data asynchronously.
                self._synchronize_control_data(True)
                
                # Update the peers.
                self._send_master_control_data_message(list(self._peer_queue_manager_dict.values()))
                
            # DEBUG
            self.logger.log_info("Connection set up with PQM {0}; PQM Dealer ID: {1}; QM Dealer ID for this connection: {2}.".format(peer_queue_manager.get_id_string(),
                                                                                                                                     peer_queue_manager.get_dealer_id_tag(),
                                                                                                                                     peer_queue_manager.get_qm_dealer_id_tag()))
            
            # Log connections.
            self._log_pqms()
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _create_peer_master_control_data_message(self, destination_dealer_id_tag, shared_memory_manager):
        """
        Creates a PeerMasterControlDataMessage intended for the destination dealer ID tag using the current data in shared memory.
        """

        try:
            
            # Create and return the message.
            return peer_messages.PeerMasterControlDataMessage(destination_dealer_id_tag, shared_memory_manager.get_pecking_order_list(), 
                                                              shared_memory_manager.get_queue_lock_owner_dict(),  
                                                              self._current_ordered_queue_owners_dict, 
                                                              self._current_push_rejection_queue_name_set,
                                                              self._current_accepting_data_owner_id_list,
                                                              self._current_frozen_push_queue_list,
                                                              self._current_frozen_pull_queue_list)
            
        except:

            raise ExceptionFormatter.get_full_exception()
    
    
    def _create_peer_master_setup_data_message(self, destination_dealer_id_tag, shared_memory_manager):
        """
        Creates a PeerMasterSetupDataMessage intended for the destination dealer ID tag using the current data in shared memory.
        """

        try:
            
            # Get the setup data from shared memory.
            exchange_wrapper_list = list(shared_memory_manager.get_exchange_wrapper_dict().values())
            queue_wrapper_list = list(shared_memory_manager.get_queue_wrapper_dict().values())
            return peer_messages.PeerMasterSetupDataMessage(destination_dealer_id_tag, exchange_wrapper_list, queue_wrapper_list)
        
        except:

            raise ExceptionFormatter.get_full_exception()
        
        
    def _delete_queues(self, queue_name_list):
        """
        Updates the QM and its memory via a deletion of all queues with the names given.
        Master method.
        Sends updates to peers.
        """
        
        try:
            
            # Forward to shared memory.
            self._shared_memory_manager.remove_queue_names_from_memory(queue_name_list)
            
            # Get the list of data queue wrappers from the list of queue names.
            data_queue_wrappers_removed = list()
            for queue_name in queue_name_list:
                data_queue_wrapper = self._current_data_queue_wrapper_dict.get(queue_name, None)
                if data_queue_wrapper != None:
                    data_queue_wrappers_removed.append(data_queue_wrapper)
                    
            # Get our ordered queue names.
            removed_ordered_queue_name_list = self._get_ordered_queue_names(data_queue_wrappers_removed)
                                    
            # If there are ordered queue changes, synchronize and update peers.
            if len(removed_ordered_queue_name_list) > 0:
                
                # Remove all ordered queues which had a lock.
                for queue_name in removed_ordered_queue_name_list:
                    if queue_name in list(self._current_ordered_queues_lock_owner_dict.keys()):
                        self._shared_memory_manager.set_queue_lock_owner(queue_name, "") 
                        
                # Remove all ordered queues which were on the push rejection list.
                self._current_push_rejection_queue_name_set = self._current_push_rejection_queue_name_set - set(removed_ordered_queue_name_list)
                
                # Remove the ordered queues.
                self._remove_queues_from_ordered_queue_owner_dict(removed_ordered_queue_name_list, False)
                
                # Synchronize control data and update peers.
                self._synchronize_control_data(True)
                self._send_master_control_data_message(list(self._peer_queue_manager_dict.values()))
                    
            # Synchronize setup data.
            self._synchronize_setup_data(True)
            
            # Send the current set up data to peers.
            self._send_master_setup_data_message(list(self._peer_queue_manager_dict.values()))
            
            # Log.
            self.logger.log_info(">>> CDR >>> Deleted queues: {0}".format(queue_name_list))
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
        
    def _disconnect_peer_queue_manager(self, peer_queue_manager, notify_pqm_message = None):
        """
        Sets the given PQM to disconnected immediately, then shuts down all socket connections.
        Disconnects request socket if necessary.
        Enqueues an action on the incoming message thread to shut down the dealer socket for the PQM.
        Enqueues a notification message along this QMs router socket to the PQMs dealer socket if notification message type is specified.
        Logs disconnect.
        """
            
        try:
            
            # Denote the PQM as disconnected.
            peer_queue_manager.set_connected_flag(False, self._get_current_time())
            
            # Send an action message to disconnect the dealer socket.
            message = system_messages.SystemPeerConnectionUpdateMessage(False, peer_queue_manager.get_id_string(), peer_queue_manager.get_dealer_connection_string())
            self._pqm_incoming_action_queue.put(message)
                    
            # If we should notify the PQM, we have to send a message out.
            if notify_pqm_message != None:
                self._send_peer_message(notify_pqm_message)

            # Handle updating the pecking order.
            # If the PQM was the master, we must determine the new master.
            # Each QM in the network will remove this PQM; the QM which has become master, will forcibly update the others.
            # Note that in the test to see if this QM is the new master in this clause, we have to check shared memory since we have no yet synchronized after the removal.
            send_master_update_message = False
            if self._is_master(peer_queue_manager.get_id_string()) == True:
                self._shared_memory_manager.remove_qm_from_pecking_order_list(peer_queue_manager.get_id_string())
                if self._shared_memory_manager.is_master(self._id_string) == True:
                    
                    # Slaves do not track the current available ordered queue owner list.
                    # Create the list now.
                    self._current_accepting_data_owner_id_list = self._which_qms_can_accept_data()
                    
                    # Add in the disconnected PQM.  
                    # We need to do this so our removal call will find a new owner for any queues owned by the disconnected PQM.
                    self._current_accepting_data_owner_id_list.append(peer_queue_manager.get_id_string())
                    
                    # Remove the PQM as an owner, track we need to send an update to slaves.
                    self._remove_qm_from_accepting_data_list(peer_queue_manager.get_id_string(), False)
                    send_master_update_message = True
                    
            # If the PQM was not the master but this QM is, it is this QMs responsibility to ensure the other QMs are forcibly updated.
            elif self._is_master(self._id_string) == True:
                self._shared_memory_manager.remove_qm_from_pecking_order_list(peer_queue_manager.get_id_string())
                self._remove_qm_from_accepting_data_list(peer_queue_manager.get_id_string(), False)
                send_master_update_message = True
            
            # Add this PQM to our shared memory to deplete list.
            self._add_monitored_remote_shared_memory(peer_queue_manager.get_id_string())
                                    
            # Synchronize control data; we have to do this for even the non-master QMs (which will be forcibly updated) to get an immediate update on the removed PQM ASAP.
            self._synchronize_control_data(True)
            
            # Update the peers if our flag tells us to.
            if send_master_update_message == True:
                self._send_master_control_data_message(list(self._peer_queue_manager_dict.values()))
            
            # Log connections.
            self._log_pqms()
            
        except:
            
            raise ExceptionFormatter.get_full_exception()    
    
    
    def _discover_peer_queue_managers(self, peer_connection_tuple_list):
        """
        Will attempt to discover peer managers from the given peer connection list.
        Will request the current pecking order via a command from each peer in the connection list.
        Returns a list and a master flag.
        If no connections could be made to the peers in the list or no peers were given in the list, an empty list of peer connection tuples and a True master flag is returned.
        If a connection was established to one of the peers, the list of peers and a master flag is returned (typically False, as this peer just joined the network, it won't be master).
        """
        
        try:
        
            # If we were not given any peers to use for discovery, return the empty list back with the master flag set to true.
            if len(peer_connection_tuple_list) == 0:
                return list(), True
                    
            # We need a ZMQ context separate from the context we will use in the rest of the application (ZMQ requirement).
            zmq_context = zmq.Context(1)
            
            # Initialize our return value.
            pecking_order_list = None
                    
            # Issue the command to each IP/Port combination we were given.
            # Note: It's fine to try our own IP/Port combination in this loop; we haven't created the command request listener yet, so there will be no conflicts.
            for remote_ip, remote_port in peer_connection_tuple_list:
                
                # The remote port is actually the base port for the peer; include the command request offset.
                remote_port += self.config.manager_command_request_port_offset
                
                # Get the connection string and create the socket connection. 
                connection_string = ZmqUtilities.get_socket_connection_string(remote_ip, remote_port)
                remote_request_socket = zmq_context.socket(zmq.REQ)
                remote_request_socket.setsockopt(zmq.LINGER, 0)
                remote_request_socket.connect(connection_string)
                 
                # Setup polling.
                remote_poller = zmq.Poller()
                remote_poller.register(remote_request_socket, zmq.POLLIN)
                 
                # Create the request message.
                message = command_messages.CommandGetPeckingOrderRequestMessage()
                    
                # Send and wait for a reply.
                message.send(remote_request_socket)
                
                # Poll on receiving with our time out.
                if remote_poller.poll(self.config.manager_discovery_poll_time_out_interval):
                    
                    # Get the reply data.
                    raw_message = SocketWrapper.pull_message(remote_request_socket)
                    message = command_messages.CommandGetPeckingOrderReplyMessage.create_from_received(raw_message)
                    pecking_order_list = message.pecking_order_list
                        
                    # DEBUG
                    self.logger.log_info("Pecking order list received from {0}; List: {1}.".format(connection_string, pecking_order_list))
                    
                # Clean the socket connections up.
                remote_request_socket.setsockopt(zmq.LINGER, 0)
                remote_request_socket.close()
                remote_poller.unregister(remote_request_socket)
                
                # If we have a valid pecking order list, break from our loop.
                if pecking_order_list != None:
                    break        
    
            # Report.
            if pecking_order_list != None:
                
                # Form the tuple list.
                peer_connection_tuple_list = list()
                for pqm_id_string in pecking_order_list:
                    peer_split = pqm_id_string.split(":")
                    peer_connection_tuple_list.append((str(peer_split[0]), int(peer_split[1])))
                    
                # Get the master flag.
                if len(pecking_order_list) == 0 or pecking_order_list[0] != self._id_string:
                    master_flag = False
                else:
                    master_flag = True
                    
                # Return out.
                return peer_connection_tuple_list, master_flag
            
            else:
                        
                # Notify and return out with no peers and this QM receiving the master flag.
                self.logger.log_info("No pecking order list data could be retrieved from any peers.  Assuming master.")
                return list(), True

        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _dump_thread_name_dict(self):
        """
        Will dump the current thread name dictionary to shared memory.
        """
        
        try:
        
            self._shared_memory_manager.set_thread_name_dict(self._thread_process_id_dict)

        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _get_connected_pqm_id_string_list(self):
        """
        Will return a list of QM ID strings for all PQMs currently connected to this QM.
        """
        
        try:
        
            connected_pqm_id_string_list = list()
            for pqm in list(self._peer_queue_manager_dict.values()):
                if pqm.get_connected_flag() == True:
                    connected_pqm_id_string_list.append(pqm.get_id_string())
                    
            return connected_pqm_id_string_list

        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _get_disconnected_pqm_id_string_list(self):
        """
        Will return a list of QM ID strings for all PQMs currently disconnected from this QM.
        """
        
        try:
        
            disconnected_pqm_id_string_list = list()
            for pqm in list(self._peer_queue_manager_dict.values()):
                if pqm.get_connected_flag() == False:
                    disconnected_pqm_id_string_list.append(pqm.get_id_string())
                    
            return disconnected_pqm_id_string_list

        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _get_connection_details_from_id_string(self, id_string):
        """
        Returns the IP Address and Port (two items) found within the given ID String.
        """
        
        split_data = id_string.split(":")
        return split_data[0], int(split_data[1])
    
    
    def _get_current_time(self):
        """
        Returns the current local time as an integer.
        """
        
        return int(time.time())

    
    def _get_id_string_from_connection_details(self, ip_address, port):
        """
        Returns the ID String for the QM with the given IP address and port.
        """
        
        return "{0}:{1}".format(ip_address, port)
        
        
    def _get_master_id_string(self):
        """
        Returns the current master id string according to the current pecking order list in local memory.
        Note that if we have no data in local memory (during initialization, this can occur), we check shared memory.
        """
        
        # If we have no data in our pecking order list, check shared memory.
        if self._current_pecking_order_list == None or len(self._current_pecking_order_list) == 0:
            return None #self._shared_memory_manager.get_current_master_id_string()
        return self._current_pecking_order_list[0]
        
        
    def _get_ordered_queue_names(self, queue_wrapper_list):
        """
        Returns a list of all ordered queue names within the system.
        """
        
        # Go through the wrapper list
        ordered_queue_name_list = list()
        for queue_wrapper in queue_wrapper_list:
            if queue_wrapper.is_ordered() == True:
                ordered_queue_name_list.append(queue_wrapper.name)
                
        # Return.
        return ordered_queue_name_list
        
        
    def _get_peer_queue_manager(self, pqm_id_string):
        """
        Returns the PQM object for the given PQM ID String.
        Returns None if no such object exists.
        """
        
        return self._peer_queue_manager_dict.get(pqm_id_string, None)
        
        
    def _get_available_owners_shared_memory_free_space_dictionary(self):
        """
        Returns a dictionary of total free space in the shared memory for all QMs which are currently accepting data.
        Shared memories which do not have enough free space to be used for ownership will not be returned.
        Shared memories which did not respond to the size query will not be returned, even if their QM is in either of the sets described above.
        """
        
        try:
            
            # We need the shared memory size for all available owners.
            qm_shared_memory_size_dict, _ = self._query_remote_shared_memory(self._current_accepting_data_owner_id_list, self._shared_memory_manager.get_memory_usage)
                                    
            # Build the free space dictionary for connected shared memories.
            qm_free_space_dict = dict()
            for qm_id_string, shared_memory_size in list(qm_shared_memory_size_dict.items()):
                
                # Get the max size for the QM.
                if qm_id_string == self._id_string:
                    max_shared_memory_size = self.config.manager_shared_memory_max_size
                    stop_threshold = self.config.manager_shared_memory_ordered_data_reject_threshold
                else:
                    pqm = self._peer_queue_manager_dict.get(qm_id_string, None)
                    if pqm == None:
                        continue#max_shared_memory_size = 0                    
                    max_shared_memory_size = pqm.get_shared_memory_max_size()
                    stop_threshold = pqm.get_ordered_queue_ownership_stop_threshold()
                    
                # If there is not enough free space, do not include this QM.
                if shared_memory_size > stop_threshold:
                    continue
                    
                # Put the entry in our dictionary.
                qm_free_space_dict[qm_id_string] = max_shared_memory_size - shared_memory_size
            
            return qm_free_space_dict
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
    
    def _handle_client_declare_exchanges_request(self, message):
        """
        Handles a client request.
        The master QM forwards the message to the update method.
        A slave QM forwards the message to the master QM.
        The worker thread which forwarded this request will lock until it sees all the desired exchanges in shared memory or the lock times out.
        """
        
        try:
            
            # If we are the master, update from the request and send the latest setup data to peers.
            master_qm_id_string = self._get_master_id_string()
            if master_qm_id_string == self._id_string:
                self._update_from_client_declare_exchanges_request(message)
                self._send_master_setup_data_message(list(self._peer_queue_manager_dict.values()))
                
            # If we are not the master, create a peer request from the message and forward to the master.
            else:
                peer_queue_manager = self._peer_queue_manager_dict[master_qm_id_string]
                request_message = peer_messages.PeerClientDeclareExchangesRequestMessage(peer_queue_manager.get_dealer_id_tag(), message.client_exchange_list)
                self._send_peer_message(request_message)   
                
            # Log.
            self.logger.log_info("Handled client declare exchanges request.") 
                
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        

    def _handle_client_declare_queues_request(self, message):
        """
        Handles a client request.
        The master QM forwards the message to the update method.
        A slave QM forwards the message to the master QM.
        The worker thread which forwarded this request will lock until it sees all the desired exchanges in shared memory or the lock times out.
        """
        
        try:
            
            # If we are the master, update from the request and send the latest setup data to peers.
            master_qm_id_string = self._get_master_id_string()
            if master_qm_id_string == self._id_string:
                self._update_from_client_declare_queues_request(message)
                self._send_master_setup_data_message(list(self._peer_queue_manager_dict.values()))
                
            # If we are not the master, create a peer request from the message and forward to the master.
            else:
                peer_queue_manager = self._peer_queue_manager_dict[master_qm_id_string]
                request_message = peer_messages.PeerClientDeclareQueuesRequestMessage(peer_queue_manager.get_dealer_id_tag(), message.client_queue_list)
                self._send_peer_message(request_message)   
                
            # Log.
            self.logger.log_info("Handled client declare queues request.")  
                         
            # Return success.
            return True
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        

    def _handle_client_delete_queues_request(self, message):
        """
        Handles a client request.
        The master QM forwards the message to the update method.
        A slave QM forwards the message to the master QM.
        The worker thread which forwarded this request will lock until it sees all the desired exchanges in shared memory or the lock times out.
        """
        
        try:
            
            # If we are the master, update from the request and send the latest setup data to peers.
            master_qm_id_string = self._get_master_id_string()
            if master_qm_id_string == self._id_string:
                self._delete_queues(message.queue_name_list)
                
            # If we are not the master, create a peer request from the message and forward to the master.
            else:
                peer_queue_manager = self._peer_queue_manager_dict[master_qm_id_string]
                request_message = peer_messages.PeerClientDeleteQueuesRequestMessage(peer_queue_manager.get_dealer_id_tag(), message.queue_name_list)
                self._send_peer_message(request_message)   
                
            # Log.
            self.logger.log_info("Handled client delete exchanges request.")
                            
            # Return success.
            return True
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _handle_client_lock_queues_request(self, message):
        """
        Handles a client request.
        The master QM forwards the message to the update method.
        A slave QM forwards the message to the master QM.
        The worker thread which forwarded this request will lock until it sees all the desired exchanges in shared memory or the lock times out.
        """
        
        try:
            
            # Use the current queue lock data in shared memory and filter on those which should actually receive a request.
            # Note that a request should only be done if a lock doesn't currently exist or if we are forcing an unlock.
            request_client_queue_lock_list = list()
            for client_queue_lock in message.client_queue_lock_list:                
                owner_id_string = self._current_ordered_queues_lock_owner_dict.get(client_queue_lock.name, "")
                if owner_id_string == "" or client_queue_lock.force_unlock_flag == True:
                    request_client_queue_lock_list.append(client_queue_lock)
            
            # Only proceed if we have items.
            if len(request_client_queue_lock_list) > 0:
                
                # Update the message.
                message.client_queue_lock_list = request_client_queue_lock_list
                
                # If we are the master, update from the request and send the latest setup data to peers.
                master_qm_id_string = self._get_master_id_string()
                if master_qm_id_string == self._id_string:
                    message.owner_id_string = self._id_string
                    self._update_from_client_lock_queues_request(message)
                    self._send_master_control_data_message(list(self._peer_queue_manager_dict.values()))
                    
                # If we are not the master, create a peer request from the message and forward to the master.
                else:
                    peer_queue_manager = self._peer_queue_manager_dict[master_qm_id_string]
                    request_message = peer_messages.PeerClientLockQueuesRequestMessage(peer_queue_manager.get_dealer_id_tag(), message.client_queue_lock_list, "")
                    self._send_peer_message(request_message)  
                
            # Log.
            self.logger.log_info("Handled client lock queues request.")
                                
            # Return success.
            return True

        except:
            
            raise ExceptionFormatter.get_full_exception()
                
        
    def _handle_client_unlock_queues_request(self, message):
        """
        Handles a client request.
        The master QM forwards the message to the update method.
        A slave QM forwards the message to the master QM.
        The worker thread which forwarded this request will lock until it sees all the desired exchanges in shared memory or the lock times out.
        """
        
        try:
            
            # Use the current queue lock data in shared memory and filter on those which should actually receive a request.
            # Note that a request should only be done if a lock exists and is being requested by the owner.
            # Note that force unlocks have no bearing here.
            request_unlock_queue_name_list = list()
            for queue_name in message.queue_name_list:           
                owner_id_string = self._current_ordered_queues_lock_owner_dict.get(queue_name, "")
                if owner_id_string == self._id_string:
                    request_unlock_queue_name_list.append(queue_name)
            
            # Only proceed if we have items.
            if len(request_unlock_queue_name_list) > 0:
                
                # Update the message.
                message.queue_name_list = request_unlock_queue_name_list
                
                # If we are the master, update from the request and send the latest setup data to peers.
                master_qm_id_string = self._get_master_id_string()
                if master_qm_id_string == self._id_string:
                    message.owner_id_string = self._id_string
                    self._update_from_client_unlock_queues_request(message)
                    self._send_master_control_data_message(list(self._peer_queue_manager_dict.values()))
                    
                # If we are not the master, create a peer request from the message and forward to the master.
                else:
                    peer_queue_manager = self._peer_queue_manager_dict[master_qm_id_string]
                    request_message = peer_messages.PeerClientUnlockQueuesRequestMessage(peer_queue_manager.get_dealer_id_tag(), message.queue_name_list)
                    self._send_peer_message(request_message)  
                
            # Log.
            self.logger.log_info("Handled client unlock queues request.")
                                
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _handle_command_add_workers_request(self, message):
        """
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            # Notify.
            notification_string = "Command received: Add workers. Count: {0}.".format(message.count)
            if message.notification != "":
                notification_string += "  Note: {0}.".format(message.notification)
            self.logger.log_info(notification_string)
            
            # Set up our current setup and control data.
            current_setup_data_message = system_messages.SystemUpdateDataWorkerSetupDataMessage(self._current_exchange_wrapper_dict, self._current_data_queue_wrapper_dict)
            current_control_data_message = system_messages.SystemUpdateDataWorkerControlDataMessage(self._current_pecking_order_list, 
                                                                                                    self._current_ordered_queues_lock_owner_dict, 
                                                                                                    self._current_ordered_queue_owners_dict,
                                                                                                    self._current_push_rejection_queue_name_set,
                                                                                                    self._current_routing_key_rejection_list,
                                                                                                    self._current_accepting_data_owner_id_list,
                                                                                                    self._current_frozen_push_queue_list,
                                                                                                    self._current_frozen_pull_queue_list)
            
            # Create and start <count> new workers.
            for _ in range(message.count):
                worker = self._data_worker_manager.create_and_add_worker(self.redis_connection_string, current_setup_data_message, current_control_data_message)
                worker.process.start()
                
            # Ensure shared memory connections are known by the new workers.
            self._synchronize_worker_remote_shared_memory_connections()
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()    
        
        
    def _handle_command_delete_queues_request(self, message):
        """
        The work for this message is done by the data worker which forwarded it to the main thread.
        The main thread's responsibility is only to log the request.
        """
        
        try:
            
            # Notify.
            notification_string = "Command received: Delete queues request.  Queue names: {0}.".format(message.queue_name_list)
            if message.notification != "":
                notification_string += "  Note: {0}.".format(message.notification)
            self.logger.log_info(notification_string)
            
            # If we are the master, update from the request and send the latest setup data to peers.
            master_qm_id_string = self._get_master_id_string()
            if master_qm_id_string == self._id_string:
                self._delete_queues(message.queue_name_list)
                
            # If we are not the master, create a peer request from the message and forward to the master.
            else:
                peer_queue_manager = self._peer_queue_manager_dict[master_qm_id_string]
                request_message = peer_messages.PeerForwardedCommandMessage(peer_queue_manager.get_dealer_id_tag(), message.__dict__)
                self._send_peer_message(request_message)                
            
            # Return success.
            return True
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
         
    def _handle_command_get_statistics_request(self, message):
        """
        The work for this message is done by the data worker which forwarded it to the main thread.
        The main thread's responsibility is only to log the request.
        """
        
        try:
            
            # Notify.
            notification_string = "Command received: Get queue statistics request."
            if message.notification != "":
                notification_string += "  Note: {0}.".format(message.notification)
            self.logger.log_info(notification_string)
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception() 
        
        
    def _handle_command_freeze_queue_request(self, message):
        """
        Handles a freeze queue command request.
        The master QM forwards the message to the update method.
        A slave QM forwards the message to the master QM.
        """
        
        try:
            
            # Notify.
            notification_string = "Command received: Freeze queue request."
            if message.notification != "":
                notification_string += "  Note: {0}.".format(message.notification)
            self.logger.log_info(notification_string)
            
            # If we are the master, update from the request and send the latest setup data to peers.
            master_qm_id_string = self._get_master_id_string()
            if master_qm_id_string == self._id_string:
                self._update_from_command_freeze_queue_request(message)
                
            # If we are not the master, create a peer request from the message and forward to the master.
            else:
                peer_queue_manager = self._peer_queue_manager_dict[master_qm_id_string]
                request_message = peer_messages.PeerForwardedCommandMessage(peer_queue_manager.get_dealer_id_tag(), message.__dict__)
                self._send_peer_message(request_message)                
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception() 
            
        
    def _handle_command_get_pecking_order_request(self, message):
        """
        The work for this message is done by the data worker which forwarded it to the main thread.
        The main thread's responsibility is only to log the request.
        """
        
        try:
            
            # Notify.
            notification_string = "Command received: Get pecking order request."
            if message.notification != "":
                notification_string += "  Note: {0}.".format(message.notification)
            self.logger.log_info(notification_string)
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception() 
        
         
    def _handle_command_get_queue_size_request(self, message):
        """
        The work for this message is done by the data worker which forwarded it to the main thread.
        The main thread's responsibility is only to log the request.
        """
        
        try:
            
            # Notify.
            notification_string = "Command received: Get queue size request.  Queue name: {0}.  Replied with: {1}.".format(message.queue_name, message.queue_size)
            if message.notification != "":
                notification_string += "  Note: {0}.".format(message.notification)
            self.logger.log_info(notification_string)
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()  
        
        
    def _handle_command_get_setup_data_request(self, message):
        """
        The work for this message is done by the data worker which forwarded it to the main thread.
        The main thread's responsibility is only to log the request.
        """
        
        try:
            
            # Notify.
            notification_string = "Command received: Get setup data request."
            if message.notification != "":
                notification_string += "  Note: {0}.".format(message.notification)
            self.logger.log_info(notification_string)
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
            
        
    def _handle_command_list_queue_request(self, message):
        """
        The work for this message is done by the data worker which forwarded it to the main thread.
        The main thread's responsibility is only to log the request.
        Note this is only logged if a custom note came in with the request.
        """
        
        try:
            
            # Notify only if we have a notification.
            if message.notification != "":
                notification_string = "Command received: List queues request."
                if message.notification != "":
                    notification_string += "  Note: {0}.".format(message.notification)
                self.logger.log_info(notification_string)
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()  
    
        
    def _handle_command_purge_queues_request(self, message):
        """
        Purges the queue in the given message of all data.
        """
        
        try:
            
            # Notify.
            notification_string = "Command received: Purge queues request.  Queues: {0}.  All flag: {1}.".format(message.queue_name_list, message.from_all_servers_flag)
            if message.notification != "":
                notification_string += "  Note: {0}.".format(message.notification)
            self.logger.log_info(notification_string)
            
            # Purge.
            for queue_name in message.queue_name_list:
                self._shared_memory_manager.purge_queue(queue_name)
            
            # If we are purging all, go through all PQMs and purge.
            if message.from_all_servers_flag == True:
                
                # Form the list of connected PQMs.
                connected_pqm_id_list = list()
                for pqm_id_string, pqm in list(self._peer_queue_manager_dict.items()):
                    if pqm.get_connected_flag() == True:                        
                        connected_pqm_id_list.append(pqm_id_string)

                # Run the purge command against all connected PQMs.
                # Ignore result data; ignore failed connection data (the PQM itself will fail out and disconnect if the shared memory has a serious issue).
                for queue_name in message.queue_name_list:
                    _, _ = self._query_remote_shared_memory(connected_pqm_id_list, self._shared_memory_manager.purge_queue, queue_name)

                # Run the purge command against all remote shared memories we are trying to deplete.
                # Ignore result data; remove any failed connections from our monitored remote shared memory list.
                for queue_name in message.queue_name_list:
                    _, failed_connection_list = self._query_remote_shared_memory(self._remote_shared_memory_to_deplete_list, self._shared_memory_manager.purge_queue, queue_name)
                    if len(failed_connection_list) > 0:
                        self._remove_monitored_remote_shared_memories(failed_connection_list)

            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()  
                
        
    def _handle_command_remove_workers_request(self, message):
        """
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            # Notify.
            notification_string = "Command received: Remove workers. Count: {0}.".format(message.count)
            if message.notification != "":
                notification_string += "  Note: {0}.".format(message.notification)
            self.logger.log_info(notification_string)
            
            # Stop and remove <count> workers.
            worker_process_list = self._data_worker_manager.stop_and_remove_workers(message.count)
            for worker_process in worker_process_list:
                worker_process.join()
                
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()  
                
                
    def _handle_command_shut_down_request(self, message):
        """
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            # Notify.
            notification_string = "Command received: Shut down."
            if message.notification != "":
                notification_string += "  Note: {0}.".format(message.notification)
            self.logger.log_info(notification_string)

            # Respond to the shut down call.  
            self.shut_down()
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()     
        
        
    def _handle_command_unlock_queue_request(self, message):
        """
        Unlocks the queue specified in the request message.
        Note that this is done by creating a mock client unlock message and following the workflow.
        """
        
        try:
            
            # Get the current owner data for the queue name desired.
            owner_id_string = self._current_ordered_queues_lock_owner_dict.get(message.queue_name, "")
            
            # If there is a lock on the queue, create a mock client message and forward to our handler; return the success flag returned by the handler.
            success_flag = True
            if owner_id_string != "":
                mock_client_message = client_messages.ClientUnlockQueuesRequestMessage([message.queue_name,], "")
                if self._unlock_queues_from_client_message(mock_client_message) == False:
                    success_flag = False
            
            # Notify.
            notification_string = "Command received: Unlock queue request.  Queue name: {0}.".format(message.queue_name)
            if owner_id_string != "":
                notification_string += "  Queue not locked; no action taken."
            if message.notification != "":
                notification_string += "  Note: {0}.".format(message.notification)
            self.logger.log_info(notification_string)
                    
            # Return success flag.
            return success_flag
        
        except:
            
            raise ExceptionFormatter.get_full_exception()  
                        
                         
    def _handle_heart_beats(self, current_time_stamp):
        """
        Handles heart beat processing.
        Goes through all connected peer queue managers and checks two things.
            1) If the PQM has sent a heart beat message to us recently.
            2) If we have sent a heart beat message to the PQM recently.
            3) If the PQM is the master PQM, ensure it has given us control/setup data when we have requested it.
        If the PQM hasn't sent a heart beat within our warning interval, will send a warning to the PQM.
        If the PQM hasn't sent a heart beat within our assumed-dead interval, will mark the PQM as disconnected locally and update its local memory.
            Note: The master QM will inevitably do the disconnection as well and update us with the latest control data.
        """
        
        try:

            for peer_queue_manager in list(self._peer_queue_manager_dict.values()):
                
                # Don't handle heart beat logic with disconnected PQMs.
                if peer_queue_manager.get_connected_flag() == False:
                    continue
                
                # First, check the last time we received a heart beat from the PQM against our assume dead and warning intervals.
                # If either fail, handle.
                interval_time_stamp = current_time_stamp - peer_queue_manager.get_last_heart_beat_received()
                if interval_time_stamp > self.config.heart_beat_assume_dead_interval:
                    self.logger.log_error("Disconnecting PQM due to no heart beat.")
                    heart_beat_message = peer_messages.PeerHeartBeatFailureMessage(peer_queue_manager.get_dealer_id_tag(), True)
                    self._disconnect_peer_queue_manager(peer_queue_manager, heart_beat_message)
                    continue
                
                elif peer_queue_manager.get_heart_beat_warning_sent_flag() == False and interval_time_stamp > self.config.heart_beat_send_warning_interval:
                    self.logger.log_info("Last heart beat from peer ({0}) has exceeded warning threshold.  Last received: {1}.  Sending warning.".format(
                        peer_queue_manager.get_id_string(), peer_queue_manager.get_last_heart_beat_received()))
                    heart_beat_message = peer_messages.PeerHeartBeatFailureMessage(peer_queue_manager.get_dealer_id_tag(), False)
                    self._send_peer_message(heart_beat_message)
                    peer_queue_manager.set_heart_beat_warning_sent_flag(True)
                    
                # Second, send a heart beat message to the PQM from this QM so the PQM knows we are still alive.
                interval_time_stamp = current_time_stamp - peer_queue_manager.get_last_heart_beat_sent()
                if interval_time_stamp > self.config.heart_beat_send_interval:
                    heart_beat_message = peer_messages.PeerHeartBeatMessage(peer_queue_manager.get_dealer_id_tag(), str(current_time_stamp), self._queue_size_snapshot_dict)
                    self._send_peer_message(heart_beat_message)
                    peer_queue_manager.set_last_heart_beat_sent(current_time_stamp)
                    
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
    
    def _handle_loop(self):
        """
        The main handler loop.
        Calls each sub-handler to ensure all responsibilities of the QM are handled.
        If an error occurs in any sub-handler, returns False.  Otherwise, returns True.
        """
            
        try:
            
            # Time stamp.
            current_time_stamp = self._get_current_time()
        
            # Initialize the successful operation flag.
            success_flag = True
            
            # Handle synchronization.
            if self._handle_synchronization(current_time_stamp) == False:
                success_flag = False
                            
            # Handle received messages from PQMs.
            if self._handle_received_message_queue() == False:
                success_flag = False
                        
            # Handle notification messages: messages put into the notification queue by the threads within our system.
            if self._handle_notifications() == False:
                success_flag = False
                
            # Handle heart beats.
            if self._handle_heart_beats(current_time_stamp) == False:
                success_flag = False
                                
            # Handle monitoring.
            if self._handle_monitoring(current_time_stamp) == False:
                success_flag = False
                
            # Return the success flag.
            return success_flag
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _handle_monitoring(self, current_time_stamp):
        """
        Handles monitoring functionality.
        Checks shared memory to ensure it has not grown too large during processing.
        If shared memory has grown too large, the routing key rejection list (from the config file) will be sent to the data workers.
        The data workers will reject data pushed to those routing keys.
        Once shared memory falls below the maximum size * the revert threshold, the rejection list will be reset and sent to the data workers.
        """
        
        # Surround in a try/except block which will raise an unexpected exception out with full information.
        try:
            
            # Check if we should check our exhausted 
            if current_time_stamp > self._last_ordered_queues_exhausted_owners_check_time + self.config.manager_check_ordered_queues_exhausted_owners_interval:
                self._last_ordered_queues_exhausted_owners_check_time = current_time_stamp
                              
                # If we are the master QM, 
                master_qm_id_string = self._get_master_id_string()
                if master_qm_id_string == self._id_string:
                    
                    # Run the update check.
                    self._check_ordered_queues_exhausted_owners()
                    
                # If we are a slave QM and we have data, send our current exhausted queue information to the master.
                elif len(self._current_exhausted_queue_owner_dict) > 0: 
                    
                    # Convert our dictionary tracker from sets to lists so we can form the message.
                    ordered_queues_exhausted_owners_dict = dict()
                    for queue_name, exhausted_owners_set in list(self._current_exhausted_queue_owner_dict.items()):
                        ordered_queues_exhausted_owners_dict[queue_name] = list(exhausted_owners_set)
                        
                    # Create and send the message to the master.
                    peer_queue_manager = self._peer_queue_manager_dict[master_qm_id_string]
                    request_message = peer_messages.PeerOrderedQueuesExhaustedOwnersMessage(peer_queue_manager.get_dealer_id_tag(), ordered_queues_exhausted_owners_dict)
                    self._send_peer_message(request_message)   
                    
            # Check if we should synchronize with queue sizes.
            if current_time_stamp > self._last_shared_memory_monitoring_time + self.config.manager_monitor_shared_memory_interval:
                self._last_shared_memory_monitoring_time = current_time_stamp
                                
                # Get the current shared memory usage.
                self._current_shared_memory_bytes_used = self._shared_memory_manager.get_memory_usage()
                
                """                
                # If we currently have no rejection list, test if we should set one.
                if len(self._current_routing_key_rejection_list) == 0:
                    if self._current_shared_memory_bytes_used > self.config.manager_shared_memory_max_size:
                        self._current_routing_key_rejection_list = self.config.manager_shared_memory_overflow_routing_key_rejection_list
                        self._send_current_control_data_to_workers()
                        self._log_push_rejection_queues()
                        
                # If we currently have a rejection list, test if we should reset it.
                elif self._current_shared_memory_bytes_used < self.config.manager_shared_memory_max_size * self.config.manager_shared_memory_revert_threshold:
                    self._current_routing_key_rejection_list = list()
                    self._send_current_control_data_to_workers()
                    self._log_push_rejection_queues()
                    
                # If we have a rejection list and shouldn't reset it, log a message about the current rejection list.  
                # This is generally a bad thing; spamming the log file is warranted.
                else:
                    self._log_push_rejection_queues()
                """
                    
                # If we are the master QM, check all QMs to see if their ability to own ordered queues has changed.
                master_qm_id_string = self._get_master_id_string()
                if master_qm_id_string == self._id_string:
                    self._update_current_accepting_data_owner_id_list(False, True)
                                   
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
    
    def _handle_notifications(self):
        
        # Surround in a try/except block which will raise an unexpected exception out with full information.
        try:
            
            # Initialize the successful operation flag.
            success_flag = True
            
            # Enter a loop which will only break when we receive no more messages from the queue we are handling.
            while True:
                
                try:
                    
                    # Get a new message; initialize it to not handled.
                    message = self._child_process_action_out_queue.get(False)
                    message_handled = False
        
                    # Forward to the correct handler method based on the message type.
                    if message.get_primary_type() == message_types.PEER:                        
                        
                        if message.get_type() == message_types.PEER_ONLINE_HANDSHAKE_REQUEST:
                            message_handled = self._handle_peer_online_handshake_request(message)

                    elif message.get_primary_type() == message_types.SYSTEM:

                        if message.get_type() == message_types.SYSTEM_THREAD_STATE:
                            message_handled = self._handle_system_thread_state(message)
                            
                        elif message.get_type() == message_types.SYSTEM_SOCKET_STATE:
                            message_handled = self._handle_system_socket_state(message)
                            
                        elif message.get_type() == message_types.SYSTEM_NOTIFICATION_MESSAGE:
                            message_handled = self._handle_system_notification_message(message)
                            
                        elif message.get_type() == message_types.SYSTEM_ERROR_MESSAGE:
                            message_handled = self._handle_system_error_message(message)
                            
                        elif message.get_type() == message_types.SYSTEM_DATA_WORKER_STATUS_REPORT:
                            message_handled = self._handle_system_data_worker_status_report(message)
                            
                        elif message.get_type() == message_types.SYSTEM_ORDERED_QUEUE_OWNERS_EXHAUSTED:
                            message_handled = self._handle_system_ordered_queue_owners_exhausted(message)

                    elif message.get_primary_type() == message_types.CLIENT:         
                                       
                        if message.get_type() == message_types.CLIENT_DECLARE_EXCHANGES_REQUEST:                            
                            message_handled = self._handle_client_declare_exchanges_request(message)
                                       
                        elif message.get_type() == message_types.CLIENT_DECLARE_QUEUES_REQUEST:                            
                            message_handled = self._handle_client_declare_queues_request(message)
                                       
                        elif message.get_type() == message_types.CLIENT_LOCK_QUEUES_REQUEST:                            
                            message_handled = self._handle_client_lock_queues_request(message)
                                       
                        elif message.get_type() == message_types.CLIENT_UNLOCK_QUEUES_REQUEST:                            
                            message_handled = self._handle_client_unlock_queues_request(message)
                                       
                        elif message.get_type() == message_types.CLIENT_DELETE_QUEUES_REQUEST:                            
                            message_handled = self._handle_client_delete_queues_request(message)
                                                    
                    elif message.get_primary_type() == message_types.COMMAND:
                         
                        if message.get_type() == message_types.COMMAND_SHUT_DOWN_REQUEST:
                            message_handled = self._handle_command_shut_down_request(message)
                         
                        elif message.get_type() == message_types.COMMAND_GET_QUEUE_SIZE_REQUEST:
                            message_handled = self._handle_command_get_queue_size_request(message)
                         
                        elif message.get_type() == message_types.COMMAND_PURGE_QUEUES_REQUEST:
                            message_handled = self._handle_command_purge_queues_request(message)
                         
                        elif message.get_type() == message_types.COMMAND_DELETE_QUEUES_REQUEST:
                            message_handled = self._handle_command_delete_queues_request(message)

                        elif message.get_type() == message_types.COMMAND_UNLOCK_QUEUE_REQUEST:
                            message_handled = self._handle_command_unlock_queue_request(message)
                         
                        elif message.get_type() == message_types.COMMAND_ADD_WORKERS_REQUEST:
                            message_handled = self._handle_command_add_workers_request(message)
                         
                        elif message.get_type() == message_types.COMMAND_REMOVE_WORKERS_REQUEST:
                            message_handled = self._handle_command_remove_workers_request(message)
                         
                        elif message.get_type() == message_types.COMMAND_LIST_QUEUES_REQUEST:
                            message_handled = self._handle_command_list_queue_request(message)
                         
                        elif message.get_type() == message_types.COMMAND_GET_PECKING_ORDER_REQUEST:
                            message_handled = self._handle_command_get_pecking_order_request(message)
                         
                        elif message.get_type() == message_types.COMMAND_GET_SETUP_DATA_REQUEST:
                            message_handled = self._handle_command_get_setup_data_request(message)
                         
                        elif message.get_type() == message_types.COMMAND_FREEZE_QUEUE_REQUEST:
                            message_handled = self._handle_command_freeze_queue_request(message)
                         
                        elif message.get_type() == message_types.COMMAND_GET_STATISTICS_REQUEST:
                            message_handled = self._handle_command_get_statistics_request(message)
                                                     
                    # If the message hasn't been handled, notify.
                    if message_handled == False:
                        self.logger.log_error("Thread notification handler found a message it could not process: {0}".format(message))
            
                except EmptyQueueException:
                    
                    # Break from the loop; there are no more messages to handle.
                    break
                    
                except IOError:
                    
                    self._log_exception(note = "IO Error while handling message: {0}".format(str(message)))
                    success_flag = False
                
                except:
                    
                    raise ExceptionFormatter.get_full_exception()
            
            # Return success flag.
            return success_flag
            
        except:
            
            raise ExceptionFormatter.get_full_exception()

    
    def _handle_peer_client_declare_exchanges_request(self, message):
        """
        Slave QMs must forward client messages to the master QM; this method handles a client message.
        If this is the master, it will handle the message and update slaves.
        If this is a slave, it will log an error - slaves should never end up handling this message type.
        """
        
        try:
            
            # Ensure master.
            if self._is_master(self._id_string) == True:
                
                # Log.
                self.logger.log_info("Master received client exchange declaration request from peer: {0}".format(message.sender_id_string))
                
                # Handle the call.
                self._update_from_client_declare_exchanges_request(message)
                self._send_master_setup_data_message(list(self._peer_queue_manager_dict.values()))
            
            # If not master.
            else:
                
                # Log.
                self.logger.log_error("Slave received client exchange declaration request from peer: {0}".format(message.sender_id_string))
                
        except:
            
            raise ExceptionFormatter.get_full_exception()
        

    def _handle_peer_client_declare_queues_request(self, message):
        """
        Slave QMs must forward client messages to the master QM; this method handles a client message.
        If this is the master, it will handle the message and update slaves.
        If this is a slave, it will log an error - slaves should never end up handling this message type.
        """
        
        try:
            
            # Ensure master.
            if self._is_master(self._id_string) == True:
                
                # Log.
                self.logger.log_info("Master received client queue declaration request from peer: {0}".format(message.sender_id_string))
                
                # Handle the call.
                self._update_from_client_declare_queues_request(message)
                self._send_master_setup_data_message(list(self._peer_queue_manager_dict.values()))
            
            # If not master.
            else:
                
                # Log.
                self.logger.log_error("Slave received client queue declaration request from peer: {0}".format(message.sender_id_string))
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        

    def _handle_peer_client_delete_queues_request(self, message):
        """
        Slave QMs must forward client messages to the master QM; this method handles a client message.
        If this is the master, it will handle the message and update slaves.
        If this is a slave, it will log an error - slaves should never end up handling this message type.
        """
        
        try:
            
            # Ensure master.
            if self._is_master(self._id_string) == True:
                
                # Log.
                self.logger.log_info("Master received client queue deletion request from peer: {0}".format(message.sender_id_string))
                
                # Handle the call.
                self._delete_queues(message.queue_name_list)
            
            # If not master.
            else:
                
                # Log.
                self.logger.log_error("Slave received client queue deletion request from peer: {0}".format(message.sender_id_string))
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        

    def _handle_peer_client_lock_queues_request(self, message):
        """
        Slave QMs must forward client messages to the master QM; this method handles a client message.
        If this is the master, it will handle the message and update slaves.
        If this is a slave, it will log an error - slaves should never end up handling this message type.
        """
        
        try:
            
            # Ensure master.
            if self._is_master(self._id_string) == True:
                
                # Log.
                self.logger.log_info("Master received client queue lock request from peer: {0}".format(message.sender_id_string))
                
                # Handle the call.
                message.owner_id_string = message.sender_id_string
                self._update_from_client_lock_queues_request(message)
                self._send_master_control_data_message(list(self._peer_queue_manager_dict.values()))
            
            # If not master.
            else:
                
                # Log.
                self.logger.log_error("Slave received client queue lock request from peer: {0}".format(message.sender_id_string))
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        

    def _handle_peer_client_unlock_queues_request(self, message):
        """
        Slave QMs must forward client messages to the master QM; this method handles a client message.
        If this is the master, it will handle the message and update slaves.
        If this is a slave, it will log an error - slaves should never end up handling this message type.
        """
        
        try:
            
            # Ensure master.
            if self._is_master(self._id_string) == True:
                
                # Log.
                self.logger.log_info("Master received client queue unlock request from peer: {0}".format(message.sender_id_string))
                
                # Handle the call.
                message.owner_id_string = message.sender_id_string
                self._update_from_client_unlock_queues_request(message)
                self._send_master_control_data_message(list(self._peer_queue_manager_dict.values()))
            
            # If not master.
            else:
                
                # Log.
                self.logger.log_error("Slave received client queue unlock request from peer: {0}".format(message.sender_id_string))
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        

    def _handle_peer_forwarded_command_message(self, message):
        """
        Handles a forwarded command message from a peer.
        Translates the command message and places in the action queue for the main thread, to be handled like normal command message.
        Returns False on failure.
        """
        
        try:
            
            # Recreate the command message.
            command_message = None
            if message.command_message_as_dict["_type"] == message_types.COMMAND_FREEZE_QUEUE_REQUEST:
                command_message = command_messages.CommandFreezeQueueRequestMessage("")
                command_message.__dict__.update(message.command_message_as_dict)
                
            elif message.command_message_as_dict["_type"] == message_types.COMMAND_DELETE_QUEUES_REQUEST:
                command_message = command_messages.CommandDeleteQueuesRequestMessage("")
                command_message.__dict__.update(message.command_message_as_dict)

            # If we didn't get a valid message, error out.
            if command_message == None:
                self.logger.log_error("Master received client queue forwarded request from peer: {0}".format(message.sender_id_string))
                return False
            
            # Notify.
            self.logger.log_info("Received forwarded command message from peer: {0}".format(message.sender_id_string))
            
            # Forward to our normal handler.
            self._child_process_action_out_queue.put(command_message, False)
            
            # Return success
            return True
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _handle_peer_heart_beat_message(self, message, peer_queue_manager_id_string):
        """
        Handles a peer heart beat message.
        Heart beat messages contain queue size snapshots; update local QM memory and all data workers with the latest snapshot data.
        """
        
        try:
                    
            # Update the PQM.
            peer_queue_manager = self._peer_queue_manager_dict[peer_queue_manager_id_string]
            peer_queue_manager.set_last_heart_beat_received(self._get_current_time())
            peer_queue_manager.set_last_queue_size_snapshot_dict(message.sender_queue_size_snapshot_dict)
            
            # DEBUG
            #self.logger.log_info("Received heart beat from peer {0}; Peer sent message at {1} remote time; Processed at {2} local time.".format(
            #    peer_queue_manager_id_string, message.sender_time_stamp, peer_queue_manager.get_last_heart_beat_received()))
            
            # Denote the message has been handled.
            return True 
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _handle_peer_heart_beat_failure_message(self, message, peer_queue_manager_id_string):
        """
        Handles a peer heart beat failure message.
        Heart beat messages contain queue size snapshots; update local QM memory and all data workers with the latest snapshot data.
        """
        
        try:
                    
            # Update the PQM.
            #peer_queue_manager = self._peer_queue_manager_dict[peer_queue_manager_id_string]
            #peer_queue_manager.set_last_heart_beat_received(self._get_current_time())
            #peer_queue_manager.set_last_queue_size_snapshot_dict(message.sender_queue_size_snapshot_dict)
            
            self.logger.log_error("Received heart beat failure from peer {0}.".format(peer_queue_manager_id_string))
            # DEBUG
            #self.logger.log_info("Received heart beat from peer {0}; Peer sent message at {1} remote time; Processed at {2} local time.".format(
            #    peer_queue_manager_id_string, message.sender_time_stamp, peer_queue_manager.get_last_heart_beat_received()))
            
            # Denote the message has been handled.
            return True 
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        

    def _handle_peer_master_control_data(self, message, synchronize = True):
        """
        Handles a control data message from the master QM.
        """
        
        try:
            
            # Log.
            self.logger.log_info("Received master control data from peer {0}.".format(
                message.sender_id_string))

            # Update shared memory with the pecking order list.
            self._shared_memory_manager.set_pecking_order_list(message.pecking_order_list)
            
            # Update shared memory with the queue lock information
            self._shared_memory_manager.set_queue_lock_dict(message.queue_lock_owner_dict)
            
            # Update shared memory with the ordered queue owners dictionary.
            self._shared_memory_manager.set_ordered_queue_owners_dict(message.ordered_queue_owners_dict)
            
            # Set the current push rejection queue name list.
            self._current_push_rejection_queue_name_set = message.push_rejection_queue_name_set
            
            # Set the current accepting data QM list.
            self._current_accepting_data_owner_id_list = message.accepting_data_owner_id_list
            
            # Set the current frozen queue information.
            self._current_frozen_push_queue_list = message.frozen_push_queue_list
            self._current_frozen_pull_queue_list = message.frozen_pull_queue_list
            
            # If we should synchronize.
            if synchronize == True:
                
                # Synchronize control data.
                self._synchronize_control_data(True)
                
                # Clear out our current ordered queues exhausted owners tracker.
                # We can do this because the workers will be getting new copies of ordered queue owners.
                # Any previously exhausted data will inherently be replaced.
                self._current_exhausted_queue_owner_dict.clear()
                    
                # Log.
                self._log_push_rejection_queues()
                self._log_frozen_queues()

            # Denote the message has been handled.
            return True 
        
        except:
            
            raise ExceptionFormatter.get_full_exception()   
        
        
    def _handle_peer_master_setup_data(self, message, synchronize = True):
        """
        Handles a setup data message from the master QM.
        """
        
        try:
            
            # Log.
            self.logger.log_info("Received master setup data from peer {0}.".format(message.sender_id_string))
            
            # Clear out old setup data.
            self._shared_memory_manager.update_from_master_setup_data(message.exchange_wrapper_list, message.queue_wrapper_list)
            
            # Synchronize queues.
            if synchronize == True:
                self._synchronize_setup_data(True)

            # Denote the message has been handled.
            return True 
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _handle_peer_offline_message(self, message):
        """
        Handles a peer off-line message.
        """
        
        try:
            
            # Log.
            self.logger.log_info("Received off-line message from peer {0}; Disconnecting.".format(
                message.sender_id_string))
                    
            # Get the peer queue manager and run the disconnection code on it; do not send a message to the PQM.
            peer_queue_manager = self._peer_queue_manager_dict[message.sender_id_string]
            self._disconnect_peer_queue_manager(peer_queue_manager)
            
            # Denote the message has been handled.
            return True 
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
    
    def _handle_peer_online_handshake_request(self, message):
        """
        When a peer makes an online handshake request, the message is received and replied to by one of our worker threads.
        A copy of the message is sent to the main thread so memory can be updated.
        This method handles that message.
        """
        
        try:
            
            # The online notification message's message data contains the connection data to the PQM which connected to us.
            if message.settings_dict["id"] not in list(self._peer_queue_manager_dict.keys()):
                
                # If we don't have a record of this PQM yet, create it and add it to memory.
                peer_ip_address, peer_request_port = self._get_connection_details_from_id_string(message.settings_dict["id"])
                self._add_pqm(peer_ip_address, peer_request_port)
                
            # Get the PQM and set its extra properties from the message.
            peer_queue_manager = self._peer_queue_manager_dict[message.settings_dict["id"]]
            peer_queue_manager.set_start_up_time(message.settings_dict["start_time"])
            peer_queue_manager.set_dealer_id_tag(message.sender_dealer_id_tag)
            peer_queue_manager.set_shared_memory_connection_string(message.settings_dict["sm_connection"])
            peer_queue_manager.set_shared_memory_max_size(message.settings_dict["sm_max"])
            peer_queue_manager.set_ordered_queue_ownership_stop_threshold(message.settings_dict["oq_stop"])
            peer_queue_manager.set_ordered_queue_ownership_start_threshold(message.settings_dict["oq_start"])
    
            # Handle connecting the PQM.
            # Note that we do not push an online notification to the PQM - it has done that to us.
            self._connect_peer_queue_manager(peer_queue_manager, message.receiver_dealer_id_tag, False)
            
            # If we are the master, the data worker who relayed this message to us is waiting for the main thread to put reply data into shared memory.
            # Do this now.
            if self._is_master(self._id_string):
                
                # Get the data messages, put them into a dictionary, dump, and send to shared memory.
                control_data_message = self._create_peer_master_control_data_message(None, self._shared_memory_manager)
                setup_data_message = self._create_peer_master_setup_data_message(None, self._shared_memory_manager)
                data_dict = dict()
                data_dict["cdm"] = control_data_message.dump()
                data_dict["sdm"] = setup_data_message.dump()
                self._shared_memory_manager.set_connection_reply_data(message.sending_thread_name, message.settings_dict["id"], bson.dumps(data_dict))
                         
            # DEBUG
            self.logger.log_info("Initialized connection with PQM {0}.".format(peer_queue_manager.get_id_string()))
                        
            # Denote the message has been handled.
            return True 
        
        except:
            
            raise ExceptionFormatter.get_full_exception()                  
        
        
    def _handle_peer_ordered_queues_exhausted_owners_message(self, message):
        """
        PeerOrderedQueuesExhaustedOwnersMessage
        Slave QMs send these messages to the master QM on a regular interval.
        Incorporate the exhausted owners for the ordered queues given in the message into the master QMs tracker.
        Slave QMs should never receive this call.
        """
        
        try:
            
            # Ensure master.
            if self._is_master(self._id_string) == True:
                
                # Log.
                self.logger.log_info("Master received peer ordered queues exhausted owners message from peer: {0}".format(message.sender_id_string))
                
                # Incorporate the peer's information.
                for queue_name, exhausted_owner_id_string_list in list(message.ordered_queues_owners_exhausted_dictionary.items()):
                    current_set = self._current_exhausted_queue_owner_dict.get(queue_name, set())
                    self._current_exhausted_queue_owner_dict[queue_name] = current_set.union(set(exhausted_owner_id_string_list))
                       
                # Log.
                self._log_exhausted_queue_owners()
                
            # If not master.
            else:
                
                # Log.
                self.logger.log_error("Slave received peer ordered queues exhausted owners message from peer: {0}".format(message.sender_id_string))
            
        except:
            
            raise ExceptionFormatter.get_full_exception() 
        
        
    def _handle_peer_request_master_data(self, message):
        """
        Sends all master data to the peer who sent the message.
        """
        
        try:
            
            # Get the PQM which requested the data.
            peer_queue_manager = self._peer_queue_manager_dict[message.sender_id_string]
            self._send_master_control_data_message([peer_queue_manager,])
            self._send_master_setup_data_message([peer_queue_manager,])
                            
            # DEBUG
            self.logger.log_info("Sent master data, as requested, by PQM {0}.".format(peer_queue_manager.get_id_string()))
            
            # Denote the message has been handled.
            return True 
        
        except:
            
            raise ExceptionFormatter.get_full_exception()  
            

    def _handle_received_message_queue(self):
        """
        Messages which have been received on dealer sockets are put into the received message queue by the thread handling those sockets.
        These messages are handled in this method.
        Example message types: Peer heart beat, peer data request, peer data to queue, etc
        """
        
        # Initialize success flag.
        success_flag = True
        
        try:
        
            # Get a new message; initialize it to not handled.
            peer_queue_manager_id_string, raw_message = self._pqm_received_message_queue.get(False)
            raw_message_primary_type = BaseMessage.get_raw_message_primary_type(raw_message)
            raw_message_type = BaseMessage.get_raw_message_type(raw_message)
            message_handled = False
    
            if raw_message_primary_type == message_types.PEER:
                
                # Heart beat
                if raw_message_type == message_types.PEER_HEART_BEAT:
                    
                    # Translate the message.
                    heart_beat_message = peer_messages.PeerHeartBeatMessage.create_from_received(raw_message, peer_queue_manager_id_string)
                    message_handled = self._handle_peer_heart_beat_message(heart_beat_message, peer_queue_manager_id_string)
                    
                elif raw_message_type == message_types.PEER_ORDERED_QUEUES_OWNERS_EXHAUSTED:
                    
                    # Translate and forward.
                    request_message = peer_messages.PeerOrderedQueuesExhaustedOwnersMessage.create_from_received(raw_message, peer_queue_manager_id_string)
                    message_handled = self._handle_peer_ordered_queues_exhausted_owners_message(request_message)
                    
                elif raw_message_type == message_types.PEER_CLIENT_DECLARE_EXCHANGES_REQUEST:
                    
                    # Translate and forward.
                    request_message = peer_messages.PeerClientDeclareExchangesRequestMessage.create_from_received(raw_message, peer_queue_manager_id_string)
                    message_handled = self._handle_peer_client_declare_exchanges_request(request_message)
                    
                elif raw_message_type == message_types.PEER_CLIENT_DECLARE_QUEUES_REQUEST:
                    
                    # Translate and forward.
                    request_message = peer_messages.PeerClientDeclareQueuesRequestMessage.create_from_received(raw_message, peer_queue_manager_id_string)
                    message_handled = self._handle_peer_client_declare_queues_request(request_message)
                    
                elif raw_message_type == message_types.PEER_CLIENT_LOCK_QUEUES_REQUEST:
                    
                    # Translate and forward.
                    request_message = peer_messages.PeerClientLockQueuesRequestMessage.create_from_received(raw_message, peer_queue_manager_id_string)
                    message_handled = self._handle_peer_client_lock_queues_request(request_message)
                    
                elif raw_message_type == message_types.PEER_CLIENT_UNLOCK_QUEUES_REQUEST:
                    
                    # Translate and forward.
                    request_message = peer_messages.PeerClientUnlockQueuesRequestMessage.create_from_received(raw_message, peer_queue_manager_id_string)
                    message_handled = self._handle_peer_client_unlock_queues_request(request_message)
                                        
                elif raw_message_type == message_types.PEER_CLIENT_DELETE_QUEUES_REQUEST:
                    
                    # Translate and forward.
                    request_message = peer_messages.PeerClientDeleteQueuesRequestMessage.create_from_received(raw_message, peer_queue_manager_id_string)
                    message_handled = self._handle_peer_client_delete_queues_request(request_message)
                    
                elif raw_message_type == message_types.PEER_MASTER_CONTROL_DATA:
                    
                    # Translate and forward.
                    request_message = peer_messages.PeerMasterControlDataMessage.create_from_received(raw_message, peer_queue_manager_id_string)
                    message_handled = self._handle_peer_master_control_data(request_message)
                    
                elif raw_message_type == message_types.PEER_MASTER_SETUP_DATA:
                    
                    # Translate and forward.
                    request_message = peer_messages.PeerMasterSetupDataMessage.create_from_received(raw_message, peer_queue_manager_id_string)
                    message_handled = self._handle_peer_master_setup_data(request_message)
                    
                elif raw_message_type == message_types.PEER_REQUEST_MASTER_DATA:
                    
                    # Translate and forward.
                    request_message = peer_messages.PeerRequestMasterDataMessage.create_from_received(raw_message, peer_queue_manager_id_string)
                    message_handled = self._handle_peer_request_master_data(request_message)
                    
                elif raw_message_type == message_types.PEER_OFFLINE:
                    
                    # Translate and forward.
                    request_message = peer_messages.PeerOfflineMessage.create_from_received(raw_message, peer_queue_manager_id_string)
                    message_handled = self._handle_peer_offline_message(request_message)
                    
                elif raw_message_type == message_types.PEER_HEART_BEAT_FAILURE:
                    
                    # Translate and forward.
                    request_message = peer_messages.PeerHeartBeatFailureMessage.create_from_received(raw_message, peer_queue_manager_id_string)
                    message_handled = self._handle_peer_heart_beat_failure_message(request_message, peer_queue_manager_id_string)
                    
                elif raw_message_type == message_types.PEER_FORWARDED_COMMAND_MESSAGE:
                    
                    # Translate and forward.
                    request_message = peer_messages.PeerForwardedCommandMessage.create_from_received(raw_message, peer_queue_manager_id_string)
                    message_handled = self._handle_peer_forwarded_command_message(request_message)
                            
            # If the message hasn't been handled, notify.
            if message_handled == False:
                self.logger.log_error("I found a message in my received message queue ... {0}; {1}; {2}".format(peer_queue_manager_id_string, raw_message, raw_message_type))
                
        except EmptyQueueException:
            
            pass
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        # Return success flag.
        return success_flag
                
        
    def _handle_synchronization(self, current_time_stamp):
        """
        Handles a synchronization with shared memory.
        """
        
        try:
            
            # Check if we should synchronize with queue sizes.
            if current_time_stamp > self._last_synchronize_queue_sizes_time + self.config.manager_synchronize_queue_sizes_interval:
                self._synchronize_queue_sizes(current_time_stamp)
                
            # Check if we should synchronize data workers with the latest remote queue sizes.
            if current_time_stamp > self._last_synchronize_worker_pqm_access_queue_lists + self.config.manager_synchronize_worker_pqm_queue_access_lists_interval:
                self._synchronize_worker_pqm_access_queue_lists(current_time_stamp)

            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()             
            
        
    def _handle_system_data_worker_status_report(self, message):
        """
        Handles a SystemDataWorkerStatusReport message.
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            # Log.
            self.logger.log_debug("Status report from data worker: {0}".format(message.thread_name))
            if message.queue_data_pushed_status_report != None:
                self.logger.log_debug("Pushed data counts: {0}".format(message.queue_data_pushed_status_report))
            if message.queue_data_popped_status_report != None:
                self.logger.log_debug("Popped data counts: {0}".format(message.queue_data_popped_status_report))
            if message.queue_data_requeued_status_report != None:
                self.logger.log_debug("Requeued data counts: {0}".format(message.queue_data_requeued_status_report))
            if message.synchronization_status_report != None:
                self.logger.log_debug("Synchronizations: {0}".format(["{0} : {1}".format(i[0], str(datetime.datetime.fromtimestamp(i[1]))) for i in message.synchronization_status_report]))
            if message.updated_remote_shared_memory_connections_status_report != None:
                self.logger.log_debug("Updated shared memory connections: {0}".format(message.updated_remote_shared_memory_connections_status_report))
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()   
        
        
    def _handle_system_error_message(self, message):
        """
        Handles a SystemErrorMessage.
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            # Log.
            process_id = self._thread_process_id_dict[message.thread_name]
            self.logger.log_error("{0} - {1}: Thread ERROR: {2}".format(process_id, message.thread_name, message.message))
            
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()                     
        
        
    def _handle_system_notification_message(self, message):
        """
        Handles a SystemNotificationMessage.
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            # Log.
            process_id = self._thread_process_id_dict.get(message.thread_name, "No PID Found")
            self.logger.log_info("{0} - {1}: Thread notification: {2}".format(process_id, message.thread_name, message.message))
            
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()   
        
        
    def _handle_system_ordered_queue_owners_exhausted(self, message):
        """
        Handles a SystemOrderedQueueExhaustedOwnersMessage message.
        Records the exhausted queue owners given in the message in the main tracker, which will be relayed/handled on an interval.
        """
        
        try:
        
            # Record the exhausted queue information in our tracker.
            current_set = self._current_exhausted_queue_owner_dict.get(message.queue_name, set())
            self._current_exhausted_queue_owner_dict[message.queue_name] = current_set.union(set(message.exhausted_owner_id_string_list))
            
            # Log.
            self.logger.log_info("{0}: Handled system ordered queue exhausted request.".format(message.thread_name))    
            
            # Return success.
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
                     
        
    def _handle_system_socket_state(self, message):
        """
        Handles a SystemSocketStateMessage.
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            if message.opened_flag == True:
                self.logger.log_info("{0}: Socket opened: {1}".format(message.thread_name, message.notification_string))
            else:
                self.logger.log_info("{0}: Socket closed: {1}".format(message.thread_name, message.notification_string))

            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()                
        
        
    def _handle_system_thread_state(self, message):
        """
        Handles a SystemThreadState message.
        Returns True/False depending on if the message was handled or not.
        """
        
        try:
            
            if message.started_flag == True:
                self.logger.log_info("{0} - {1}: Thread started.".format(message.process_id, message.thread_name))
                self._thread_process_id_dict[message.thread_name] = message.process_id
            else:
                self.logger.log_info("{0}: Thread stopped".format(message.thread_name))
                
                # Shut down the entire server if a thread is shut down and we are still marked as running.
                if self.is_running == True:
                    
                    # If this is a worker thread.
                    if self._data_worker_manager.check_master_thread_list(message.thread_name):

                        # If the name of the thread which has stopped resolves to a valid worker, 
                        #  it means the worker thread has stopped before the tracker was removed.
                        # Check to see if the worker is in a shutdown state; shut down the entire server it this was unexpected.
                        worker = self._data_worker_manager.get_worker_from_thread_name(message.thread_name)
                        if worker != None:
                            
                            # If the worker wasn't supposed to shut down, we have to shut down our server.
                            if worker.should_shutdown == False:
                                self.logger.log_error("{0}: Thread resolved to a worker which has been shut down unexpectedly; shutting down server.".format(message.thread_name))
                                self.shut_down()
                        
                        # Regardless, we must notify the broker that the worker is stopped.
                        self._data_broker_action_queue.put(message)
                        
                    # Any other thread type should result in a shut down.
                    #else:
                    #    self.shut_down()
        
            # Update the thread name dictionary in shared memory.
            self._dump_thread_name_dict()
            
            return True
        
        except:
            
            raise ExceptionFormatter.get_full_exception()   
        
        
    def _is_master(self, qm_id_string = None):
        """
        Returns True if the given QM ID String matches the current master QM ID String according to local memory.
        If no QM ID string is given, uses this QM's id string.
        """
        
        # Get the corrected QM ID if none was specified.
        if qm_id_string == None:
            qm_id_string = self._id_string
        
        # Return the test result.
        return self._get_master_id_string() == qm_id_string
        
        
    def _log_accepting_data_owners(self):
        
        self.logger.log_info(">>> ADO >>> {0}".format(self._current_accepting_data_owner_id_list))


    def _log_exception(self, as_incident = True, note = "", increment_statsd = True):
        
        # Log the exception.
        self.logger.log_error("::: *** EXCEPTION: {0}".format(ExceptionFormatter.get_message()), as_incident)
        if note != "":
            self.logger.log_error("::: *** EXCEPTION - Note: {0}".format(note))
        
        # Send to the exception bucket.
        if increment_statsd == True:
            Utility.send_stats_count(self.statsd_library, self.config.STATSD_QUEUE_MANAGER_EXCEPTION_COUNT, 1)
        
        
    def _log_exchanges(self):

        self.logger.log_debug(">>> E >>> {0}".format(list(self._current_exchange_wrapper_dict.keys())))
        
        
    def _log_exhausted_queue_owners(self):
        
        self.logger.log_debug(">>> EQO >>> {0}".format(self._current_exhausted_queue_owner_dict))
                
        
    def _log_frozen_queues(self):

        self.logger.log_info(">>> PushFQ >>> {0}".format(self._current_frozen_push_queue_list))
        self.logger.log_info(">>> PullFQ >>> {0}".format(self._current_frozen_pull_queue_list))
        
        
    def _log_pecking_order(self):

        self.logger.log_info(">>> PO >>> {0}".format(self._current_pecking_order_list))
        
        
    def _log_pqms(self):

        self.logger.log_info(">>> PQM >>> {0}".format([str(pqm) for pqm in list(self._peer_queue_manager_dict.values())]))
        
        
    def _log_queues(self):

        self.logger.log_debug(">>> Q >>> {0}".format([str(queue_data_wrapper) for queue_data_wrapper in list(self._current_data_queue_wrapper_dict.values())]))
        
        
    def _log_queue_lock_owners(self):

        self.logger.log_info(">>> QLO >>> {0}".format(self._current_ordered_queues_lock_owner_dict))
        
        
    def _log_push_rejection_queues(self):

        self.logger.log_info(">>> PRQ >>> {0}".format(self._current_push_rejection_queue_name_set))
        
        
    def _log_ordered_queue_owners(self):

        self.logger.log_debug(">>> OQO >>> {0}".format(self._current_ordered_queue_owners_dict))
        
            
    def _query_remote_shared_memory(self, pqm_id_string_list, shared_memory_method, *parameters):
        """
        Sends the query to all remote shared memories attached to the PQM IDs in the given PQM ID List.
        Will query all registered remote shared memories with the given shared memory method and parameters.  
        Returns two items:
            1) A dictionary of responses, keyed off the PQM ID string corresponding to the ID String in the remote shared memory list, valued off the response.
            2) A list of PQM ID strings which failed to connect.
        Note: this QMs ID string can appear in the ID string list; local shared memory will be queried when this is the case.
        """
        
        # Check all remote shared memory we are trying to deplete.
        return_dict = dict()
        connection_failed_list = list()
        for pqm_id_string in pqm_id_string_list:
            
            # If this is our own ID string, query locally and continue (do no connection error handling).
            if pqm_id_string == self._id_string:
                return_dict[pqm_id_string] = shared_memory_method(*parameters)
                continue
            
            # Run the command and store the response.
            try:
                return_dict[pqm_id_string] = self._shared_memory_manager.query_remote_shared_memory(pqm_id_string, shared_memory_method, *parameters)
            except RedisConnectionUnknownException:                  
                connection_failed_list.append(pqm_id_string)
                continue
            except RedisConnectionFailureException:                  
                connection_failed_list.append(pqm_id_string)
                continue
            
        # Return.
        return return_dict, connection_failed_list
        
        
    def _remove_monitored_remote_shared_memories(self, remove_pqm_id_string_list):
        """
        Removes all PQM ID Strings in the given list from the list of remote shared memories which will be monitored until depleted.
        This tracker is used to track shared memory which no longer has a PQM connected to it.
        Note this method will forcibly update all data workers with the latest remote shared memory connections.
        """

        # Remove each from the tracker and from shared memory.
        for pqm_id_string in remove_pqm_id_string_list:
            if pqm_id_string in self._remote_shared_memory_to_deplete_list:
                self._remote_shared_memory_to_deplete_list.remove(pqm_id_string)
            self._shared_memory_manager.remove_pqm_connection(pqm_id_string)
            
        # Synchronize the workers.
        self._synchronize_worker_remote_shared_memory_connections()
        
        
    def _remove_qm_from_accepting_data_list(self, dead_qm_id_string, synchronize_control_data):
        """
        Removes the given QM ID string from the available list of ordered queue owners.
        This should happen when a QM goes off-line, is too full to handle more ordered data, etc.
        For any ordered queue which is currently owned by the given QM ID string, a new owner from the available owner list will be used.
        If no new available owners are available for a queue, no data can be written to that queue; the queue will be added to the push rejection list.
        Master method; does not update peers.
        """
            
        try:
            
            if dead_qm_id_string in self._current_accepting_data_owner_id_list:
                
                # Remove from the available list.
                self._current_accepting_data_owner_id_list.remove(dead_qm_id_string)
                                
                # Get the list of all queues which this QM currently owns.
                owned_queue_name_list = list()
                for queue_name, current_owner_list in list(self._current_ordered_queue_owners_dict.items()):
                    if dead_qm_id_string == current_owner_list[-1]:
                        owned_queue_name_list.append(queue_name)
                        
                # If there are owned queue names.
                if len(owned_queue_name_list) > 0:
                    
                    # Get the free space dictionary for all shared memories for ordered queue owners
                    qm_free_space_dict = self._get_available_owners_shared_memory_free_space_dictionary()
                    
                    # Track all queues we must place on the rejection list.
                    add_to_rejection_list = list()
                    
                    # If there are not items in our sorted list or the last item in the list has no free space, we have no available QMs for these new queues.
                    # Add all the queues to our rejection list.
                    if len(qm_free_space_dict) == 0:
                        add_to_rejection_list.extend(owned_queue_name_list)
                    
                    # If there are items in our sorted list, assign.
                    else:
                        
                        # Track if we changed our ordered queues.
                        ordered_queue_change_made = False
                        
                        # Go through each queue, assign in round-robin order.
                        sorted_free_space_tuples = list()
                        for queue_name in owned_queue_name_list:
                            
                            # Refresh our sorted free space tuples if we have no items left to pop.
                            # Sort our free size dictionary by the QMs with the most space free to the QMs with the most space free (we will pop the last item off as we assign).
                            if len(sorted_free_space_tuples) == 0:
                                sorted_free_space_tuples = sorted(list(qm_free_space_dict.items()), key=lambda x: x[1])
                        
                            # Get the current owner list.
                            current_owner_id_list = self._current_ordered_queue_owners_dict.get(queue_name, list())
                            
                            # Find the first owner which is not in our current owner list.
                            valid_tuple = None
                            for test_tuple in sorted_free_space_tuples:
                                if test_tuple[0] not in current_owner_id_list:
                                    valid_tuple = test_tuple
                                    break
                            
                            # If no QM ID string was found, we could not assign a new owner for this queue and must put it on our rejection list.
                            if valid_tuple == None:
                                add_to_rejection_list.append(queue_name)
                            
                            # If a QM ID string was found, assign it and remove the entry in our tracker.
                            else:
                                self._current_ordered_queue_owners_dict.setdefault(queue_name, list()).append(valid_tuple[0])
                                sorted_free_space_tuples.remove(valid_tuple)          
                                ordered_queue_change_made = True        
                            
                        # If a change to our ordered queues was made, update.
                        if ordered_queue_change_made == True:
                            self._shared_memory_manager.set_ordered_queue_owners_dict(self._current_ordered_queue_owners_dict)
                                            
                    # Clear unused ordered queue owners.
                    _ = self._clear_exhausted_ordered_queue_owners(False)                            
                        
                    # When removing a QM, it's possible the removed QM is the only point of access for the queues in the system.
                    # This happens when there's no available QMs yet this QM doesn't own all of the queues.
                    # Check now.
                    if len(self._current_accepting_data_owner_id_list) == 0:
                        for queue_name in list(self._current_ordered_queue_owners_dict.keys()):
                            if queue_name not in add_to_rejection_list:
                                add_to_rejection_list.append(queue_name)
                        
                    # Update our rejection list if necessary
                    if len(add_to_rejection_list) > 0:
                        self._add_queue_names_to_push_rejection_set(add_to_rejection_list, False)
                    
                    # We had to have made some changes to the system; synchronize if desired.
                    if synchronize_control_data == True:
                        self._synchronize_control_data(True)
                
            # Log.
            self._log_accepting_data_owners()
            self.logger.log_info("Remove QM from accepting data list processed.")
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
    
    def _remove_queues_from_ordered_queue_owner_dict(self, queue_name_list, synchronize_control_data):
        """
        Removes the given queues from the current ordered queue owner dictionary.
        Master method; does not update peers.
        """
            
        try:
            
            # Track if a change was actually made so we only update shared memory / workers if necessary.
            change_made = False
        
            # Go through each queue name given.
            for queue_name in queue_name_list:
                
                # Delete the queue if it is in our owners dictionary; do not balance owners.
                if queue_name in list(self._current_ordered_queue_owners_dict.keys()): 
                    del(self._current_ordered_queue_owners_dict[queue_name])
                    change_made = True
                    
            # If we had a change, update.
            if change_made == True:
                
                # Write the results to shared memory.
                self._shared_memory_manager.set_ordered_queue_owners_dict(self._current_ordered_queue_owners_dict)
                
                # Synchronize.
                if synchronize_control_data == True:
                    self._synchronize_control_data(True)
                               
            # Log.
            self.logger.log_info("Remove queue from ordered queue owners dictionary processed.")
            self._log_ordered_queue_owners()
                
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _remove_queues_from_push_rejection_set(self, queue_name_list, synchronize_control_data):
        """
        Removes the given queue names from the list of queues which currently should not receive data.
        Master method; does not update peers.        
        Returns true/false depending on if a change was actually made or not.
        """

        try:
            
            # Convert the list to a set.
            queue_name_set = set(queue_name_list)
            
            # Determine if a change will be made.
            change_required = len(queue_name_set.intersection(self._current_push_rejection_queue_name_set)) > 0
            if change_required == True:
                
                # Update our push rejection set.
                self._current_push_rejection_queue_name_set = self._current_push_rejection_queue_name_set - queue_name_set
                            
                # Synchronize.
                if synchronize_control_data == True:
                    self._synchronize_control_data(True)
                    
                # Log.
                self._log_push_rejection_queues()
                
            # Return that a change was made.
            return change_required
    
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _send_current_control_data_to_workers(self):
        """
        Synchronizes all data workers with the current control data in the QMs local memory.
        """
                
        # Create the update message and forward to the workers.
        message = system_messages.SystemUpdateDataWorkerControlDataMessage(self._current_pecking_order_list, 
                                                                           self._current_ordered_queues_lock_owner_dict, 
                                                                           self._current_ordered_queue_owners_dict,
                                                                           self._current_push_rejection_queue_name_set,
                                                                           self._current_routing_key_rejection_list,
                                                                           self._current_accepting_data_owner_id_list,
                                                                           self._current_frozen_push_queue_list,
                                                                           self._current_frozen_pull_queue_list)
        self._data_worker_manager.send_message_to_workers(message)    
        
        
    def _send_current_setup_data_to_workers(self):
        """
        Synchronizes all data workers with the current setup data in the QMs local memory.
        """
        
        # Create the update message and forward to the workers.
        message = system_messages.SystemUpdateDataWorkerSetupDataMessage(self._current_exchange_wrapper_dict, self._current_data_queue_wrapper_dict)
        self._data_worker_manager.send_message_to_workers(message) 
        
        
    def _send_message_to_connected_pqms(self, destination_pqm_list, message):
        """
        Sends the given message to all connected PQMs in the given PQM destination list.
        For each connected PQM, the message will be copied and the destination PQM's ID String will be set before sending.
        """
        
        # Go through each PQM, create a message for it, and put on our outgoing message action queue.
        for pqm in destination_pqm_list:
            if pqm.get_connected_flag() == True:
                request_message = copy.copy(message)
                request_message.destination_dealer_id_tag = pqm.get_dealer_id_tag()
                self._send_peer_message(request_message)   
    
    
    def _send_master_control_data_message(self, peer_queue_manager_list):
        """
        Creates a master control data update message and signals the outgoing thread to send it to each connected PQM in the given PQM list.
        """
        
        try:
            
            # Send the message to connected PQMs.
            self._send_message_to_connected_pqms(peer_queue_manager_list, self._create_peer_master_control_data_message(None, self._shared_memory_manager))
                    
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _send_master_setup_data_message(self, peer_queue_manager_list):
        """
        Creates a master setup data update message and signals the outgoing thread to send it to each connected PQM in the given PQM list.
        """
        
        try:
            
            # Send the message to connected PQMs.
            self._send_message_to_connected_pqms(peer_queue_manager_list, self._create_peer_master_setup_data_message(None, self._shared_memory_manager))
                    
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _send_peer_message(self, action_message):
        """
        Places the given action message onto the outgoing peer queue manager message thread's action queue.
        That thread will handle the action.
        """
        
        # Enqueue the message.
        self._pqm_outgoing_action_queue.put(action_message)
    
    
    def _synchronize_control_data(self, synchronize_data_workers):
        """
        Synchronizes the local setup trackers based off the current information in shared memory.
        Updates data workers if the synchronize flag is set to true.
        """

        try:
            
            # Get the latest dictionary from shared memory.
            self._current_pecking_order_list = self._shared_memory_manager.get_pecking_order_list()
            self._current_ordered_queues_lock_owner_dict = self._shared_memory_manager.get_queue_lock_owner_dict()
            self._current_ordered_queue_owners_dict = self._shared_memory_manager.get_ordered_queue_owners_dict()
            
            # Synchronize workers.
            if synchronize_data_workers == True:
                self._send_current_control_data_to_workers()
            
            # Log the current control data.
            self._log_pecking_order()
            self._log_queue_lock_owners()
            self._log_ordered_queue_owners()
        
        except:

            raise ExceptionFormatter.get_full_exception()
        
    
    def _synchronize_queue_sizes(self, current_time_stamp):
        """
        Synchronizes the current size of all queues in local memory based off the current queue information in shared memory.
        Tracks the delta between the last synchronization.
        """

        try:
        
            # Store the previous queue size dictionary.
            previous_queue_size_snapshot_dict = self._queue_size_snapshot_dict
            
            # Overwrite the current queue size dictionary in local memory.
            self._queue_size_snapshot_dict = self._shared_memory_manager.get_queue_sizes(list(self._current_data_queue_wrapper_dict.keys()))
            
            # If we had no previous dictionary data, set to previous to our current so we don't get invalid data.
            if len(list(previous_queue_size_snapshot_dict.keys())) == 0:
                previous_queue_size_snapshot_dict = self._queue_size_snapshot_dict
                
            # Overwrite the current queue size delta dictionary in local memory.
            self._queue_size_snapshot_delta_dict = dict()
            for queue_name in list(self._queue_size_snapshot_dict.keys()):
                self._queue_size_snapshot_delta_dict[queue_name] = self._queue_size_snapshot_dict[queue_name] - previous_queue_size_snapshot_dict.get(queue_name, 0)
                
            # Update last synchronize time.
            self._last_synchronize_queue_sizes_time = current_time_stamp
            
            # Update the command request thread with our latest information.
            self._command_request_action_queue.put(system_messages.SystemUpdateQmQueueSizeDictionaryMessage(self._queue_size_snapshot_dict), False)
            
            # Update shared memory with the most current queue size dictionary.
            self._shared_memory_manager.set_queue_size_snapshot_dict(self._queue_size_snapshot_dict) 
        
        except:

            raise ExceptionFormatter.get_full_exception()

        
    def _synchronize_setup_data(self, synchronize_data_workers):
        """
        Synchronizes the local setup trackers based off the current information in shared memory.
        Updates data workers if the synchronize flag is set to true.
        """

        try:

            # Get the latest dictionary from shared memory.
            self._current_exchange_wrapper_dict = self._shared_memory_manager.get_exchange_wrapper_dict()
            self._current_data_queue_wrapper_dict = self._shared_memory_manager.get_queue_wrapper_dict()
    
            # Synchronize workers.
            if synchronize_data_workers == True:
                self._send_current_setup_data_to_workers()
            
            # Log the current setup data.
            self._log_exchanges()
            self._log_queues()
        
        except:

            raise ExceptionFormatter.get_full_exception()
    
        
    def _synchronize_worker_pqm_access_queue_lists(self, current_time_stamp):
        """
        Synchronizes the data worker threads with the most recent PQM queue size data.  
        The access dictionary sent to workers is keyed off of queue names with the values being lists of PQMs who reported their shared memory has data available.
        """

        try:
        
            # Form the dictionary we will pass to the data workers.
            # Note that we go through our PQM dictionary in a random ordering.
            # We do this so that the data workers get a list of PQMs with available data in their shared memory in a random order.
            # This ensures the PQMs aren't always queried in the same order by the data workers.
            # Note also that we can control which PQMs are accessed first as necessary (useful for depleting the shared memory of a PQM which has gone off-line).
            pqm_queue_access_dictionary = dict()
            pqm_access_order_list = list(self._peer_queue_manager_dict.keys())
            pqm_queue_size_dictionaries = dict()
            random.shuffle(pqm_access_order_list)
            for pqm_id_string in pqm_access_order_list:
                
                # Get the PQM and ensure it is still connected.
                pqm = self._peer_queue_manager_dict[pqm_id_string]
                if pqm.get_connected_flag() == True:
                    
                    # Get the snapshot dictionary.
                    snapshot_dictionary = pqm.get_last_queue_size_snapshot_dict()
                    
                    # Include this PQM's dictionary in our PQM queue size dictionaries.
                    pqm_queue_size_dictionaries[pqm_id_string] = snapshot_dictionary
                    
                    # If we have received a new update in this PQM since our last send.
                    if pqm.get_last_heart_beat_received() > self._last_synchronize_worker_pqm_access_queue_lists:
                                                
                        # Fill the data worker dictionary with any queues for this PQM which have data.
                        # Note: We do not allow ordered queues to have their data put in these dictionaries; the QMs will manage that data.
                        for queue_name, queue_size in list(snapshot_dictionary.items()):
                            if queue_size > 0:
                                queue_wrapper = self._current_data_queue_wrapper_dict.get(queue_name, None)
                                if queue_wrapper == None or queue_wrapper.is_ordered() == False:
                                    pqm_queue_access_dictionary.setdefault(queue_name, list()).append(pqm_id_string)
            
            # Check our remote shared memory depletion list.
            # Track connection failures.
            remove_from_tracking_list = list()
            remote_shared_memory_dict, connection_failure_list = self._query_remote_shared_memory(self._remote_shared_memory_to_deplete_list, 
                                                                                                  self._shared_memory_manager.get_queue_sizes)
            
            if len(remote_shared_memory_dict) > 0:
                print("Found remote data", remote_shared_memory_dict)

            # Add our failures into our removal list.
            if len(connection_failure_list) > 0:
                remove_from_tracking_list.extend(connection_failure_list)
                
            # Check the queue size information of the valid results.
            for pqm_id_string, remote_queue_size_dict in list(remote_shared_memory_dict.items()):
                
                # Go through and place this PQM's remote shared memory at the front of our access list for all queues which have data.
                queue_inserted = False
                for queue_name, queue_size in list(remote_queue_size_dict.items()):
                    if queue_size > 0:
                        pqm_queue_access_dictionary.setdefault(queue_name, list()).insert(0, pqm_id_string)
                        queue_inserted = True
                
                # If no queue was inserted, mark this PQM's shared memory for removal.
                if queue_inserted == False:
                    remove_from_tracking_list.append(pqm_id_string)
                    
                # If a queue was inserted, we have queue data still; add the PQM's shared memory data to our PQM queue size dictionaries.
                else:
                    pqm_queue_size_dictionaries[pqm_id_string] = remote_queue_size_dict 
            
            # Clean up any remote shared memory we have successfully depleted.
            for pqm_id_string in remove_from_tracking_list:
                
                # Remove the PQM from tracking.
                # SMK: Remove the connection string from the QM and all the workers as well.
                self._remote_shared_memory_to_deplete_list.remove(pqm_id_string)
                
                # Log.
                if pqm_id_string in connection_failure_list:
                    self.logger.log_error("PQM's shared memory connection could not be established; will no longer being monitored.  PQM ID: {0}".format(pqm_id_string))    
                else:
                    self.logger.log_info("PQM's shared memory has been depleted and is no longer being monitored.  PQM ID: {0}".format(pqm_id_string))
            
            # Update the workers.
            self._data_worker_manager.update_workers_pqm_queue_access_dictionaries(pqm_queue_access_dictionary)
            
            # If we had failures, remove the failed connections from our monitored remote shared memories.
            if len(connection_failure_list) > 0:
                self._remove_monitored_remote_shared_memories(connection_failure_list)
            
            # Update the command request thread with our latest information.
            self._command_request_action_queue.put(system_messages.SystemUpdatePqmQueueSizeDictionariesMessage(pqm_queue_size_dictionaries), False)
            
            # Update last synchronize time.
            self._last_synchronize_worker_pqm_access_queue_lists = current_time_stamp
        
        except:

            raise ExceptionFormatter.get_full_exception()
        
        
    def _synchronize_worker_remote_shared_memory_connections(self):
        """
        Synchronizes the data workers with current remote shared memory connections.
        Will ensure workers know of connected PQM and monitored-until-depleted shared memory connections.
        """
        
        # Create the tracker.
        shared_memory_connection_string_dict = dict()
        
        # Go through all peers we have seen.
        # If they are still connected, add their connection information.
        # If their shared memory is in our monitored until depleted list, add their connection information. 
        for pqm_id_string, pqm in list(self._peer_queue_manager_dict.items()):
            if pqm.get_connected_flag() == True or pqm_id_string in self._remote_shared_memory_to_deplete_list:
                pqm_shared_memory_connection_string = pqm.get_shared_memory_connection_string()
                if pqm_shared_memory_connection_string != None:
                    shared_memory_connection_string_dict[pqm_id_string] = pqm_shared_memory_connection_string
                        
        # Update data workers.
        self._data_worker_manager.update_workers_shared_memory_connections(shared_memory_connection_string_dict)
        
        
    def _unlock_queues_from_client_message(self, message):
        """
        Handles the forwarding unlocking - based on master or not master status of the QM - of the queues specified in the given client unlock queue message.
        """
        
        try:
        
            # If we are the current master, update memory and forward the request to connected PQMs.
            master_qm_id_string = self._get_master_id_string()
            if master_qm_id_string == self._id_string:
                self._update_from_client_unlock_queues_request(message)
                request_message = peer_messages.PeerClientUnlockQueuesRequestMessage(None, message.queue_name_list)
                self._send_message_to_connected_pqms(list(self._peer_queue_manager_dict.values()), request_message)
                
            # Forward the request to the master QM if not.
            else:                 
                peer_queue_manager = self._peer_queue_manager_dict[master_qm_id_string]
                request_message = peer_messages.PeerClientUnlockQueuesRequestMessage(peer_queue_manager.get_dealer_id_tag(), message.queue_name_list)
                self._send_peer_message(request_message)    
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
    
    def _update_current_accepting_data_owner_id_list(self, force_local_change, synchronize_on_change):
        """        
        Will check each QM in the system to see if they need to alter their allow ordered data ownership status.
        If the local QM's state should be handled regardless of current state, set force_local_change to True.
        Should only be used by the master.
        Sends out a master update message if changes are made, regardless of synchronize boolean.
        """
        
        try:
            
            # To ensure we only send one update to peers, use a tracker.
            change_made = False
            
            # Track our network memory statistics; we will store this in shared memory for external access.
            network_memory_statistics_dict = dict()
                        
            # Check ourselves.
            allowing_ownership_flag = self._id_string in self._current_accepting_data_owner_id_list
            new_allowing_ownership_flag = self._can_qm_accept_data(self._id_string, allowing_ownership_flag, self._current_shared_memory_bytes_used, 
                                                                   self.config.manager_shared_memory_ordered_data_reject_threshold, 
                                                                   self.config.manager_shared_memory_ordered_data_accept_threshold)
            
            # Update our dictionary.
            qm_memory_stats_dict = { "used" : self._current_shared_memory_bytes_used, "max" : self.config.manager_shared_memory_max_size,
                                     "reject" : self.config.manager_shared_memory_ordered_data_reject_threshold, 
                                     "accept" : self.config.manager_shared_memory_ordered_data_accept_threshold }
            network_memory_statistics_dict[self._id_string] = qm_memory_stats_dict
            
            # If we are forcing a local change and actually had no change.
            if force_local_change == True and new_allowing_ownership_flag == None:
                
                # Force a change.
                new_allowing_ownership_flag = allowing_ownership_flag
                
                # Ensure our change code can run.
                # We need to remove ourself from the available list if we want to be added.
                # We need to add ourself from the available list if we want to be removed. 
                if new_allowing_ownership_flag == True:
                    self._current_accepting_data_owner_id_list.remove(self._id_string)
                else:
                    self._current_accepting_data_owner_id_list.append(self._id_string)
            
            # Update if we have a change.
            if new_allowing_ownership_flag == True:
                self._add_qm_to_accepting_data_list(self._id_string, False)    
                change_made = True
            elif new_allowing_ownership_flag == False:        
                self._remove_qm_from_accepting_data_list(self._id_string, False)
                change_made = True
                                    
            # To check our connected peers, we'll need the current usage of their shared memory.
            connected_pqm_id_string_list = self._get_connected_pqm_id_string_list()
            qm_shared_memory_size_dict, _ = self._query_remote_shared_memory(connected_pqm_id_string_list, self._shared_memory_manager.get_memory_usage)
                
            # Check our connected peers.
            for pqm_id_string in list(qm_shared_memory_size_dict.keys()):
                
                # Get the PQM and test.
                pqm = self._peer_queue_manager_dict[pqm_id_string]
                current_pqm_shared_memory_size = qm_shared_memory_size_dict[pqm_id_string]
                allowing_ownership_flag = pqm_id_string in self._current_accepting_data_owner_id_list
                new_allowing_ownership_flag = self._can_qm_accept_data(pqm_id_string, allowing_ownership_flag, 
                                                                       current_pqm_shared_memory_size, 
                                                                       pqm.get_ordered_queue_ownership_stop_threshold(),
                                                                       pqm.get_ordered_queue_ownership_start_threshold())                    
                # Update our dictionary.
                qm_memory_stats_dict = { "used" : current_pqm_shared_memory_size, "max" : pqm.get_shared_memory_max_size(),
                                         "reject" : pqm.get_ordered_queue_ownership_stop_threshold(), "accept" : pqm.get_ordered_queue_ownership_start_threshold() }
                network_memory_statistics_dict[pqm_id_string] = qm_memory_stats_dict
                
                # If we are forcing a local change and actually had no change.
                if pqm.get_ordered_queue_ownership_force_update_flag() == True and new_allowing_ownership_flag == None:
                    
                    # Force a change.
                    new_allowing_ownership_flag = allowing_ownership_flag
                    
                    # Ensure our change code can run.
                    # We need to remove ourself from the available list if we want to be added.
                    # We need to add ourself from the available list if we want to be removed. 
                    if new_allowing_ownership_flag == True:
                        self._current_accepting_data_owner_id_list.remove(pqm_id_string)
                    else:
                        self._current_accepting_data_owner_id_list.append(pqm_id_string)
                    
                # Update if we have a change.
                if new_allowing_ownership_flag == True:
                    self._add_qm_to_accepting_data_list(pqm_id_string, False)    
                    change_made = True
                elif new_allowing_ownership_flag == False:    
                    self._remove_qm_from_accepting_data_list(pqm_id_string, False)
                    change_made = True
                        
                # Reset force flag either way.
                pqm.set_ordered_queue_ownership_force_update_flag(False)  
                
            # Store our current network statistics.
            self._shared_memory_manager.set_network_memory_statistics_dict(network_memory_statistics_dict)
                
            # Update peers if we made a change.
            if change_made == True:

                # Write the results to shared memory.
                self._shared_memory_manager.set_ordered_queue_owners_dict(self._current_ordered_queue_owners_dict)
                
                # Synchronize.
                if synchronize_on_change == True:
                    self._synchronize_control_data(True)
                
                # Send to peers.
                self._send_master_control_data_message(list(self._peer_queue_manager_dict.values()))
    
        except:
            
            raise ExceptionFormatter.get_full_exception()
            
    
    def _update_from_client_declare_exchanges_request(self, message):
        """
        Updates the QM and its shared memory with data found in the client declare exchanges request message.
        """
        
        try:
            
            # Only new exchanges should make it into this method.
            # Convert the client exchanges into exchange wrappers and pass them into shared memory.
            exchange_wrapper_list = [ExchangeWrapper(client_exchange.name, client_exchange.type) for client_exchange in message.client_exchange_list]
            self._shared_memory_manager.update_exchange_wrappers(exchange_wrapper_list)
            
            # Synchronize setup data.
            self._synchronize_setup_data(True)
            
            # Log.
            for exchange_wrapper in exchange_wrapper_list:
                self.logger.log_info(">>> CDR >>> Updated from client exchange declaration request: {0}".format(exchange_wrapper))
                        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
             
    def _update_from_client_declare_queues_request(self, message):
        """
        Updates the QM and its shared memory with data found in the client declare queues request message.
        Master method; slaves should not call this method.
        Does not send update to peers.
        """
        
        try:
            
            # We must attach the queues to their exchanges and record any ordered queues in shared memory.
            # Record all new queues in local memory.
            
            # Attach all queues.
            self._shared_memory_manager.attach_queues_to_exchanges(message.client_queue_list)
        
            # Track newly declared ordered queues.
            declared_ordered_queue_name_list = self._get_ordered_queue_names(message.client_queue_list)
            
            # Determine which queues are actually new.
            current_ordered_queue_name_list = self._get_ordered_queue_names(list(self._current_data_queue_wrapper_dict.values()))
            
            # Convert to sets and capture difference.
            new_ordered_queue_name_list = list(set(declared_ordered_queue_name_list) - set(current_ordered_queue_name_list))
                    
            # If there are ordered queue changes, synchronize and update peers.
            if len(new_ordered_queue_name_list) > 0:
                self._add_queue_names_to_ordered_queue_owner_dict(new_ordered_queue_name_list, False)
                self._synchronize_control_data(True)
                self._send_master_control_data_message(list(self._peer_queue_manager_dict.values()))
                    
            # Synchronize setup data.
            self._synchronize_setup_data(True)
            
            # Log.
            for client_queue in message.client_queue_list:
                self.logger.log_info(">>> CDR >>> Updated from client queue declaration request: {0}".format(client_queue))
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
            
    
    def _update_from_client_lock_queues_request(self, message):
        """
        Updates the QM and its shared memory with data found in the client lock queues request message.
        Master method; slaves should not call this method.
        Does not send update to peers.
        """
        
        try:
            
            # Pull the correct QM ID string we will use based on the source type of this message.
            if message.get_type() == message_types.PEER_CLIENT_LOCK_QUEUES_REQUEST:
                qm_id_string = message.owner_id_string
            else:
                qm_id_string = self._id_string

            # Update the shared memory; track a log string while doing so.
            locked_queue_name_string = ""
            qm_free_space_dict = self._get_available_owners_shared_memory_free_space_dictionary()
            change_made = False
            for client_queue_lock in message.client_queue_lock_list:
                
                # Set the lock owner.
                self._shared_memory_manager.set_queue_lock_owner(client_queue_lock.name, qm_id_string)
                
                # If the lock owner has enough free space and isn't already an owner, set it as the new owner for the ordered queue.
                if qm_free_space_dict.get(qm_id_string, 0) > 0:
                    if qm_id_string not in self._current_ordered_queue_owners_dict.setdefault(client_queue_lock.name, list()):
                        self._current_ordered_queue_owners_dict.setdefault(client_queue_lock.name, list()).append(qm_id_string)
                        change_made = True
                
                # Update the string.
                locked_queue_name_string += "{0} (PID: {1}) ".format(client_queue_lock.name, client_queue_lock.process_id_string)
                
            # If we made a change.
            if change_made == True:
                self._shared_memory_manager.set_ordered_queue_owners_dict(self._current_ordered_queue_owners_dict)
                        
            # Synchronize setup data.
            self._synchronize_control_data(True)
            
            # Log the current locks.
            self.logger.log_info(">>> CDR >>> Received and allowed locks requested for QM {0}: {1}.".format(qm_id_string, locked_queue_name_string))
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
            
    
    def _update_from_client_unlock_queues_request(self, message):
        """
        Updates the QM and its shared memory with data found in the client unlock queues request message.
        Master method; slaves should not call this method.
        Does not send update to peers.
        """
        
        try:
            
            # Pull the correct QM ID string we will use based on the source type of this message.
            if message.get_type() == message_types.PEER_CLIENT_UNLOCK_QUEUES_REQUEST:
                qm_id_string = message.owner_id_string
            else:
                qm_id_string = self._id_string

            # Update shared memory; track a log string.
            unlocked_queue_name_string = ""
            for queue_name in message.queue_name_list:
                self._shared_memory_manager.set_queue_lock_owner(queue_name, "")
                unlocked_queue_name_string += queue_name + " "
                        
            # Synchronize setup data.
            self._synchronize_control_data(True)
            
            # Log the current locks.
            self.logger.log_info(">>> CDR >>> Received and allowed unlocks requested for QM {0}: {1}.".format(qm_id_string, unlocked_queue_name_string))
        
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def _update_from_command_delete_queues_request(self, message):
        """
        Updates the QM and its shared memory with data found the delete queues command request message.
        Master method.
        Will update peers on change; will update workers on change.
        """
        
        # Forward.
        
        
        
        pass
    
        
    def _update_from_command_freeze_queue_request(self, message):
        """
        Updates the QM and its shared memory with data found the freeze queue command request message.
        Master method.
        Will update peers on change; will update workers on change.
        """
        
        try:
            
            # Update our frozen queue tracker based on the request.
            change_made = False
            
            # Handle switching our ability to allow pushing to the specified queue.
            if message.freeze_push == True:
                if message.queue_name not in self._current_frozen_push_queue_list:
                    self._current_frozen_push_queue_list.append(message.queue_name)
                    change_made = True
            else:
                if message.queue_name in self._current_frozen_push_queue_list:
                    self._current_frozen_push_queue_list.remove(message.queue_name)
                    change_made = True
                    
            # Handle switching our ability to allow pulling from the specified queue.
            if message.freeze_pull == True:
                if message.queue_name not in self._current_frozen_pull_queue_list:
                    self._current_frozen_pull_queue_list.append(message.queue_name)
                    change_made = True
            else:
                if message.queue_name in self._current_frozen_pull_queue_list:
                    self._current_frozen_pull_queue_list.remove(message.queue_name)
                    change_made = True
            
            # If a change was made...
            if change_made == True:
                
                # Synchronize control data.
                self._synchronize_control_data(True)
                
                # Log.
                self._log_frozen_queues()
                
                # Update peers.
                self._send_master_control_data_message(list(self._peer_queue_manager_dict.values()))
                
            # If no change was made...
            else:
                
                # Log.
                self.logger.log_info("No changes detected in freeze queue command.")
            
        except:
            
            raise ExceptionFormatter.get_full_exception()        
        
        
    def _which_qms_can_accept_data(self):
        """
        Returns a list of QM ID strings which can currently accept data, based on their current shared memory sizes.
        """
        
        try:
            
            # Create the return list.
            can_accept_data_list = list()
            
            # Check self.
            current_shared_memory_size = self._shared_memory_manager.get_memory_usage()
            new_allowing_ownership_flag = self._can_qm_accept_data(self._id_string, False, 
                                                                                   current_shared_memory_size, 
                                                                                   self.config.manager_shared_memory_ordered_data_reject_threshold,
                                                                                   self.config.manager_shared_memory_ordered_data_accept_threshold)
            
            # We supplied False to the method, stating we are currently not allowing ownership.
            # The method will return True only if we can allow.
            if new_allowing_ownership_flag == True:
                can_accept_data_list.append(self._id_string)
                
            
            # To check our connected peers, we'll need the current usage of their shared memory.
            connected_pqm_id_string_list = self._get_connected_pqm_id_string_list()
            qm_shared_memory_size_dict, _ = self._query_remote_shared_memory(connected_pqm_id_string_list, self._shared_memory_manager.get_memory_usage)
                
            # Check our connected peers.
            for pqm_id_string in list(qm_shared_memory_size_dict.keys()):
                
                # Get the PQM and test.
                pqm = self._peer_queue_manager_dict[pqm_id_string]
                current_pqm_shared_memory_size = qm_shared_memory_size_dict[pqm_id_string]
                new_allowing_ownership_flag = self._can_qm_accept_data(pqm_id_string, False, 
                                                                                       current_pqm_shared_memory_size,
                                                                                       pqm.get_ordered_queue_ownership_stop_threshold(),
                                                                                       pqm.get_ordered_queue_ownership_start_threshold())
                
                # We supplied False to the method, stating we are currently not allowing ownership.
                # The method will return True only if we can allow.
                if new_allowing_ownership_flag == True:
                    can_accept_data_list.append(pqm_id_string)
                    
            # Return.
            return can_accept_data_list

        except:

            raise ExceptionFormatter.get_full_exception()
