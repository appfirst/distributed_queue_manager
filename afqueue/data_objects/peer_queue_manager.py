from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.common.socket_wrapper import SocketWrapper #@UnresolvedImport
from afqueue.common.zmq_utilities import ZmqUtilities #@UnresolvedImport
from afqueue.data_objects.sequenced_queue_data import SequencedQueueData #@UnresolvedImport
from afqueue.messages import peer_messages #@UnresolvedImport
import zmq #@UnresolvedImport
    
    
class PeerQueueManager():
    """
    The Peer QM class facilitates communication between QMs.
    Each PQM represents a peer QM.
    There are three communication channels:
     1) The request socket: used to make requests to the PQM which require a deadlock reply.
     2) The router socket: used to send requests to the PQM asynchronously.
     3) The dealer socket: used to receive requests from the PQM asynchronously.  
    The router socket exists in the main QM.  
    As other QMs start up (PQMs in this QM's context), they will register with the router socket via a dealer socket.
    The dealer socket created for this purpose will set it's identity to the ID string of the main QM.  
    With these connections in place, the main QM can send data to this PQM via the router socket and this PQMs ID string.
    """
    
    def __init__(self, peer_ip_address, peer_request_port, peer_push_port):
        
        # Assign.
        self.ip_address = peer_ip_address
        self.request_port = peer_request_port
        self.push_port = peer_push_port
        
        # Set blank sockets.
        self._request_socket = None
                
        # Heart beat values.
        self._last_heart_beat_sent = 0
        self._last_heart_beat_received = 0
        self._heart_beat_warning_sent_flag = False
        
        # Connected status of the PQM and of the PQMs request socket.
        self._connected_flag = False
        
        # The start up time of the PQM is used to decide race conditions (newer QMs win in any race condition).
        # Each QM sends its start up time to each PQM during handshakes; this PQMs start up time is stored here.
        self._start_up_time = None
        
        # The dealer socket tags allow us to communicate with the PQM.
        # The dealer socket id tag is the tag we must send to our router socket to send messages to this PQM.
        # The QM dealer socket id tag is the tag we gave the PQM to use in its router socket to send this QM data.
        # Note that the dealer id tags change on each connect.  
        # This is because ZeroMQ does not overwrite id tags in router sockets, meaning if we disconnect and reconnect to a router with the same id tag, the 
        # data sent to that tag will go to the first registered (and now disconnected) dealer instead of the second (current) dealer.  
        self._dealer_socket_id_tag = None
        self._qm_dealer_socket_id_tag = None
        
        # The shared memory connection string used by the PQM.
        self._shared_memory_connection_string = None
        self._shared_memory_max_size = 0
        self._ordered_queue_ownership_stop_threshold = 0
        self._ordered_queue_ownership_start_threshold = -1
        
        #
        self._last_queue_size_snapshot_dict = dict()
        # 
        self.last_queue_request_time_dict = dict()
        # 
        self._sequenced_queue_data_list = list()
        
        self._ordered_queue_ownership_force_update_flag = False
        
        
    def set_ordered_queue_ownership_force_update_flag(self, flag):
        self._ordered_queue_ownership_force_update_flag = flag
    
    def get_ordered_queue_ownership_force_update_flag(self):
        return self._ordered_queue_ownership_force_update_flag
                
        
    def allowed_to_request_data(self, queue_name, current_time):
        
        # Add the entry to our dictionary if it does not exist.
        if queue_name not in self.last_queue_request_time_dict:
            self.last_queue_request_time_dict[queue_name] = 0
                                    
        # Test if we can request.
        required_delay = 5
        if current_time - required_delay > self.last_queue_request_time_dict[queue_name]:
            self.last_queue_request_time_dict[queue_name] = current_time
            return True
        
        return False
        
        
    def handshake(self, zmq_context, settings_dict):
        
        try:
            
            # Create and connect a request socket.
            request_socket = zmq_context.socket(zmq.REQ)
            connection_string = self.get_data_request_socket_connection_string()
            request_socket.connect(connection_string)
            
            # Create and send the handshake request.
            # This will let the PQM know we exist.  
            # We send the QM's ID String and the dealer ID tag of this PQM which will be used to receive asynchronous messages from this PQM
            # Note that this operation will be blocked until a reply is received.
            request_message = peer_messages.PeerOnlineHandshakeRequestMessage(settings_dict, self.get_qm_dealer_id_tag())
            request_message.send(request_socket)
            
            # Get the reply.
            raw_reply_message = SocketWrapper.pull_message(request_socket)
            reply_message = peer_messages.PeerOnlineHandshakeReplyMessage.create_from_received(raw_reply_message)
            
            # Check if we failed to handshake.
            if reply_message.ping_back_success_flag == False:
                raise Exception("Handshaking to host received reply with a ping back failure: {0}".format(connection_string))
                           
            # The response has the PQM's start up time; set it.
            self.set_start_up_time(reply_message.settings_dict["start_time"])
            
            # The response has the PQM's Dealer ID Tag (which we will use to send it data); set it.
            self.set_dealer_id_tag(reply_message.sender_dealer_id_tag)
            
            # Set the shared memory connection string for this PQM.
            self.set_shared_memory_connection_string(reply_message.settings_dict["sm_connection"])
            
            # Set the max size of shared memory for this PQM.
            self.set_shared_memory_max_size(reply_message.settings_dict["sm_max"])
            
            # Set ordered queue ownership thresholds for this PQM.
            self.set_ordered_queue_ownership_stop_threshold(reply_message.settings_dict["oq_stop"])
            self.set_ordered_queue_ownership_start_threshold(reply_message.settings_dict["oq_start"])
            
            # Disconnect the socket we used to handshake.        
            request_socket.setsockopt(zmq.LINGER, 0)
            request_socket.close()
            
            # Return the reply message so the caller can further use it.
            return reply_message
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
        
    def get_connected_flag(self):
        """
        Returns the time at which the last heart beat was received from the PQM. 
        """        
        return self._connected_flag
        
        
    def get_data_request_socket_connection_string(self):
        """
        Returns the socket connection string for the data request socket for the PQM.
        """
        return ZmqUtilities.get_socket_connection_string(self.ip_address, self.request_port)
        
        
    def get_dealer_connection_string(self):
        """
        Returns the connection string to give to the QMs DEALER socket when connecting to the PQMs ROUTER socket. 
        """        
        return "tcp://{0}:{1}".format(self.ip_address, self.push_port)
        
        
    def get_dealer_id_tag(self):
        """
        Returns the tag for the PQM's dealer id tag. 
        """        
        return self._dealer_socket_id_tag

        
    def get_heart_beat_warning_sent_flag(self):
        """
        Returns the true if a heart beat warning message has been sent to the PQM since the last heart beat was received. 
        """        
        return self._heart_beat_warning_sent_flag
    
    
    def get_id_string(self):
        """
        PQMs are identified via their IP address and primary request port.
        """
        return "{0}:{1}".format(self.ip_address, self.request_port)
    
    def get_last_heart_beat_received(self):
        """
        Returns the time at which the last heart beat was received from the PQM. 
        """
        return self._last_heart_beat_received
        
    
    def get_last_heart_beat_sent(self):
        """
        Returns the time at which the last heart beat was sent to the PQM. 
        """
        return self._last_heart_beat_sent
        
        
    def get_last_queue_size_snapshot_dict(self):
        """
        Returns the last queue size snapshot dictionary sent from the PQM.
        """        
        return self._last_queue_size_snapshot_dict
        
    
    def get_ordered_queue_ownership_start_threshold(self):
        """
        Gets the shared memory threshold at which this PQM should start allowing ownership of ordered queues (generally after it has stopped - this is a restart).
        """
        return self._ordered_queue_ownership_start_threshold
        
    
    def get_ordered_queue_ownership_stop_threshold(self):
        """
        Gets the shared memory threshold at which this PQM should stop allowing ownership of ordered queues. 
        """
        return self._ordered_queue_ownership_stop_threshold
        
        
    def get_qm_dealer_id_tag(self):
        """
        Returns the tag for the QM's dealer id tag. 
        """        
        return self._qm_dealer_socket_id_tag
        
        
    def get_shared_memory_connection_string(self):
        """
        Returns the connection string the PQM used for its shared memory.
        """        
        return self._shared_memory_connection_string
        
        
    def get_shared_memory_max_size(self):
        """
        Gets the max size of shared memory for the PQM.
        """        
        return self._shared_memory_max_size

    
    def get_start_up_time(self):
        """ 
        Returns the start up time of the PQM.
        """
        return self._start_up_time
        
        
    def pop_expired_queue_data_sequences(self, current_time_stamp, expiration_interval):
        """
        Checks all tracked sets of queue data to see if they have exceeded their received verification interval.
        Uses the given time stamp and expiration interval to compare against the times at which the sets of queue data were sent to determine if they have expired.
        Returns two lists of sequenced queue data sets
         1) Sets of queue data which were verified as being sent by the outgoing thread which have expired
         2) Sets of queue data which were not verified as being sent by the outgoing thread which have expired.
        Note that the data is popped from memory during this process - the returned lists have the data which has expired and must be handled or else data could be lost.
        """
        
        try:
            
            # Form the time we will test against in the loop.
            test_time_stamp = current_time_stamp - expiration_interval
            
            # Go through the data in memory, adding expired elements to our lists as we find them.
            expired_sent_list = list()
            expired_not_sent_list = list()
            for sequenced_queue_data in self._sequenced_queue_data_list:
                
                # If the data has been verified, we only allow it to check against the verified as being sent time stamp.
                if sequenced_queue_data.verified_sent_time_stamp != None:
                    if sequenced_queue_data.verified_sent_time_stamp < test_time_stamp:
                        expired_sent_list.append(sequenced_queue_data)
                
                # If the data has not been verified as being sent, check it against the time it was created.
                elif sequenced_queue_data.sequenced_time_stamp < test_time_stamp:
                    expired_not_sent_list.append(sequenced_queue_data)
                    
            # Remove the expired items from our list; they are about to be returned.
            for sequenced_queue_data in expired_sent_list:
                self._sequenced_queue_data_list.remove(sequenced_queue_data)
                
            for sequenced_queue_data in expired_not_sent_list:
                self._sequenced_queue_data_list.remove(sequenced_queue_data)
            
            # Return.
            return expired_sent_list, expired_not_sent_list
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
    
        
    def register_queue_data_sent(self, queue_name, data_list, sequence_number, sequenced_time_stamp):
        """
        Registers queue data as having been sent to this PQM.
        The PQM will ultimately need to verify it received this data.
        Utilizing a sequence number (generated by the queue from which the data came), the correct set of data can be verified.
        Utilizing time stamps, timely delivery can be ensured.        
        """
        
        # Create the data object.
        sequenced_queue_data = SequencedQueueData(queue_name, data_list, sequence_number, sequenced_time_stamp)
        
        # Record the object in memory.
        self._sequenced_queue_data_list.append(sequenced_queue_data)
    
    
    def set_connected_flag(self, flag, time_stamp):
        """
        Returns the time at which the last heart beat was received from the PQM. 
        """
        
        # Set value.
        self._connected_flag = flag
        
        # If we are connected, set the heart beat time stamps.
        if self._connected_flag == True:
            self._last_heart_beat_received = time_stamp
            self._last_heart_beat_sent = time_stamp
        
        
    def set_dealer_id_tag(self, dealer_id_tag):
        """
        Sets the tag for the PQM's dealer id tag. 
        """        
        self._dealer_socket_id_tag = dealer_id_tag

        
    def set_heart_beat_warning_sent_flag(self, flag):
        """
        Set to true if a heart beat warning message has been sent to the PQM since the last heart beat was received. 
        """        
        self._heart_beat_warning_sent_flag = flag
        
    
    def set_last_heart_beat_received(self, time_stamp):
        """
        Sets the time at which the last heart beat was received from the PQM. 
        Resets the heart beat warning set flag to false.
        """
        self._last_heart_beat_received = time_stamp
        self._heart_beat_warning_sent_flag = False
        
    
    def set_last_heart_beat_sent(self, time_stamp):
        """
        Sets the time at which the last heart beat was sent to the PQM. 
        """
        self._last_heart_beat_sent = time_stamp
        
    
    def set_ordered_queue_ownership_start_threshold(self, threshold):
        """
        Sets the shared memory threshold at which this PQM should start allowing ownership of ordered queues (generally after it has stopped - this is a restart).
        """
        self._ordered_queue_ownership_start_threshold = threshold
        
    
    def set_ordered_queue_ownership_stop_threshold(self, threshold):
        """
        Sets the shared memory threshold at which this PQM should stop allowing ownership of ordered queues. 
        """
        self._ordered_queue_ownership_stop_threshold = threshold
        
        
    def set_qm_dealer_id_tag(self, dealer_id_tag):
        """
        Sets the tag for the QM's dealer id tag.
        This is the tag the QM which contains this PQM object sent the PQM for its dealer ID tag. 
        """        
        self._qm_dealer_socket_id_tag = dealer_id_tag
        
        
    def set_shared_memory_connection_string(self, shared_memory_connection_string):
        """
        Sets the connection string the PQM used for its shared memory.
        """        
        self._shared_memory_connection_string = shared_memory_connection_string
        
        
    def set_shared_memory_max_size(self, shared_memory_max_size):
        """
        Sets the max size of shared memory for the PQM.
        """        
        self._shared_memory_max_size = shared_memory_max_size

        
    def set_last_queue_size_snapshot_dict(self, queue_size_snapshot_dict):
        """
        Sets the last queue size snapshot dictionary sent from the PQM.
        """        
        self._last_queue_size_snapshot_dict = queue_size_snapshot_dict
                
        
    def set_start_up_time(self, start_up_time):
        """ 
        Sets the start up time of the PQM.
        """
        self._start_up_time = start_up_time
        

    def verify_queue_data_sent(self, queue_name, sequence_number, time_stamp):
        """
        Verifies the queue data with the given sequence number as having been sent to the PQM.
        It is possible that verification of the data having been received by the PQM can come in before verification of it having been sent due to the queue nature of the application.
        """
        
        try:
            
            # This method is called when we handle the sent message from the notification queue.  
            # That message is put on the queue by the outgoing message queue.
            # It is possible a received verification could come in before we actually handle that message.
            # It is also impossible for us to get to this method without having the data in our list at some point.
            # Thus,iIf we can't find the sequenced data in our list, we can ignore it. 
            sequenced_queue_data = self._get_queue_data_sent(queue_name, sequence_number)
            if sequenced_queue_data == None:
                return
            
            # Update the verified time stamp.
            sequenced_queue_data.verified_sent_time_stamp = time_stamp
            
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
    
    def verify_queue_data_received(self, queue_name, sequence_number):
        """
        Verifies the set of queue data for the given queue name with the given sequence number as having been received by the PQM.
        This will remove the set of queue data from tracker memory within the PQM.
        """
        
        try:

            # Get the data object from memory.
            sequenced_queue_data = self._get_queue_data_sent(queue_name, sequence_number)
            
            # If we couldn't find the data, it most likely means it was sent so long ago that we marked the send as expired and put the data back in our local queue.
            # This is an issue since we just received verification that the PQM also put the data in its local queue, meaning we have cloned data within the queue system.
            # The best we can do at this point is alert the system.
            if sequenced_queue_data == None:
                raise Exception("Sequenced data could not be found to verify received: {0} - {1}".format(queue_name, sequence_number))
            
            # Remove.
            self._sequenced_queue_data_list.remove(sequenced_queue_data)
                
        except:
            
            raise ExceptionFormatter.get_full_exception()
        
    
    def _get_queue_data_sent(self, queue_name, sequence_number):
        """
        Searches memory for a set of queue data for the given queue with the given sequence number.
        Returns None if no match is found.
        """
        
        # Check each set of sequenced queue data for a match; return if found.
        for sequenced_queue_data in self._sequenced_queue_data_list:
            if sequenced_queue_data.queue_name == queue_name and sequenced_queue_data.sequence_number == sequence_number:
                return sequenced_queue_data

        # Return no data if none was found.
        return None
    
    
    def __str__(self):
        
        connected_status = "C" if self._connected_flag == True else "DC"
        return "{0} ({1})".format(self.get_id_string(), connected_status)

