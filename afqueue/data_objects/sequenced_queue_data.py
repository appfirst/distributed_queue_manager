
class SequencedQueueData(object):
    """
    Container class for sequenced queue data.
    When data from a queue is sent between queue managers, a sequence number is attached to it.
    The sending QM will send the data, along with the sequence number, in a data push message.
    The sending QM will create an object of this class type to track the data it sent out.
    The receiving QM will handle the data and record the sequence number locally.
    The receiving QM will send all new sequence numbers received out in each heart beat.
    The sending QM will receive the sequence numbers and be able to verify data was received. 
    """
    
    def __init__(self, queue_name, data_list, sequence_number, sequenced_time_stamp):
        self.queue_name = queue_name
        self.data_list = data_list
        self.sequence_number = sequence_number
        self.sequenced_time_stamp = sequenced_time_stamp
        self.verified_sent_time_stamp = None
        
        
    def __str__(self):
        
        return str(self.__dict__)