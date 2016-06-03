
###
remote_ip_interface = None



################################
### Synchronization settings ###
################################

# The interval (seconds) between queue synchronizations in the queue manager.
# The smaller this value, the more accurate metrics and faster the sharing of data between queue managers.
# The larger this value, the less load the queue manager puts on its shared memory.
manager_synchronize_queue_sizes_interval = 3

# The interval (seconds) between updates to data workers within the manager with PQM access data.
# The manager's main thread dictates which PQM's queues the data workers can access.
# It will update the data workers with current access data at the specified interval.
manager_synchronize_worker_pqm_queue_access_lists_interval = 5

# Synchronization timeout for when a slave is waiting to get the current setup/control data from the master during handshake.
manager_synchronize_master_data_on_handshake_timeout = 30


##############################
### Shared Memory settings ###
##############################

# The interval (seconds) at which the master QM in the system will check current shared memory usage.
# This check is done to ensure shared memory does not exceed the configured maximum size.
manager_monitor_shared_memory_interval = 3

# The maximum size shared memory is allowed to reach.  
# The master QM will check to see if shared memory usage has exceeded this size on the interval above.
# Note that it is possible shared memory will exceed this size by some amount when data throughput is high since we only check on an interval.
# Because of this, the "stop" threshold for when to stop allowing a particular QM to have more data put into its shared memory should be a conservative number.
# NOTE: If you change this value for a specific environment, be sure to update the thresholds below, as they will be valued off the size
#    in the base configuration file if left alone, instead off the specific file.
manager_shared_memory_max_size = 12 * 1024 * 1024 * 1024 # GB * MB * KB * B

# When monitoring the current shared memory usage across the system, the master QM will determine if any given QM within the system is capable of being an ordered queue "owner."
# Ordered data queues end up having all their data go to one QM's redis until that redis exceeds a threshold.
# The master QM then assigns a new QM's redis to receive all data for that queue - making that QM an "owner", and so on.
# Once a redis has been assigned, it can not be used again for that same queue until all data has been read from that redis' copy of the queue.
# The stop owner threshold is the max amount of memory redis can be using before a change command is sent to the master from a QM owning that redis.
# The master will not allow a QM's redis to be used after the master receives a stop owner call.
# The start owner threshold is the amount of memory redis must then dip back below performing the start owner call after the stop call has been called.
manager_shared_memory_ordered_data_reject_threshold = 0.85 * manager_shared_memory_max_size
manager_shared_memory_ordered_data_accept_threshold = 0.70 * manager_shared_memory_max_size

# Ordered queues are automatically monitored according to the parameters above.
# Extra queues can be assigned with the following settings.
# Set the list of queues which should reject data when all QMs have exceeded their reject thresholds.
manager_shared_memory_overflow_queue_rejection_set = set(["collector.data.be", "log.temp"])

# When we are in overflow rejection mode, we can allow all queues which are not the rejection set to forcibly allow data to be written to them.
# If we stop all queues when we hit our overflow state, it's possible we will never get out of that state - no queues can move data.
# Set this flag to True and all queues (both distributed and ordered) can continue to use data once we are in the overflow state, except the specified queues above.
# Note that this flag does nothing when we are in the state of some of the QMs in the system being full on data; only when the entire system is overflowing.
manager_shared_memory_overflow_allow_all_other_queues_on_rejection = True
#manager_shared_memory_custom_queue_reject_threshold = 0
#manager_shared_memory_custom_queue_accept_threshold = 0

# The interval (seconds) at which QMs will handle queue owners which have been reported as exhausted by their data worker threads.
# Slave QMs send their data to the master QM on this interval; the Master QM will check all reports and update the system with the correct status on this interval.
manager_check_ordered_queues_exhausted_owners_interval = 3

# When determining which QMs have the most free space so we know who should be the next owners for ordered queues, we can group the QMs by free space.
# Set this value to 1 for each QM to be grouped by its exact space; by something else to be grouped in similar space to other QMs.
# Example: Set to 50MB, all QMs within 50MB of each other will be considered as having the same amount of free space during assignment.
manager_shared_memory_system_normalization_size = 50 * 1024 * 1024 # MB * KB * B


########################
### IP/Port settings ###
########################

# Put default QM locations here.
# A QM will go through each QM in this list and ask for the current list of QMs in the network.
# If no QMs are specified or if none respond, the QM will assume it is master.
# Note that the command line parameter "peer" will override this setting - a QM will check with only that peer for the peer list.
manager_default_peer_list = []

# Manager settings settings
manager_default_port = 24000
manager_command_request_port_offset = 3

# Bridge settings
bridge_default_port = 29990
bridge_default_command_port_offset = 1


################################
### Socket Recreation Timers ###
################################

# The interval at which the main thread will check to see if any connections need to be recreated.
bridge_check_worker_recreation_interval = 1

# The period at which we cap our number of recreations.
bridge_worker_recreation_period = 60

# The maximum number of connection recreations we can make over our defined period.
# Use this value to make sure we don't try to make too many connections too quickly.
bridge_worker_recreation_max_per_period = 12


####################
### Thread names ###
####################

# Manager thread names.
manager_command_request_thread_name = "cmd.req"
data_broker_thread_name = "data.b"
data_worker_thread_name = "data.w"
peer_request_thread_name = "pr"
pqm_incoming_message_thread_name = "pqm.in"
pqm_outgoing_message_thread_name = "pqm.out"

# Bridge thread names.
bridge_command_request_thread_name = "cmd.req"
bridge_load_balancer_thread_name = "b.lb"
bridge_worker_thread_name = "b.w"


##########################
### Commander settings ###
##########################

# The number of lines to display when performing a list queues command.
list_queues_max_display_count = 24


###########################
### Heart beat settings ###
###########################

# All times in seconds.
heart_beat_send_interval = 3
heart_beat_send_warning_interval = 15
heart_beat_assume_dead_interval = 30


#################################################
### Polling Timeout / Sleep Interval settings ###
#################################################

# Note: Python's sleep timers work in seconds; ZMQ work in microseconds.

# The default polling time out for all ZMQ polling; if there isn't a specific value below, this value is used.
zmq_poll_timeout = 1000                             # Microseconds

# The Manager's data worker's thread's polling and no activity timing information.
manager_data_worker_poll_interval = 3000            # Microseconds
manager_data_worker_no_activity_sleep = 0.01        # Seconds

# The Manager's command request thread's polling and no activity timing information.
manager_command_request_poll_interval = 3000        # Microseconds
manager_command_request_no_activity_sleep = 0.05    # Seconds

# The Manager's incoming peer queue manager message thread's polling and no activity timing information.
pqm_incoming_message_poll_interval = 1000           # Microseconds
pqm_incoming_message_no_activity_sleep = 0.1        # Seconds

# The Network Bridge's worker's thread's polling and general polling time out information.
bridge_worker_remote_poll_timeout = 2500            # Microseconds
bridge_poll_timeout = 800                           # Microseconds

# The interval at which each PQM will be allowed to respond to a discovery when a QM starts up.
manager_discovery_poll_time_out_interval = 1000     # Microseconds

# The interval at which a QMs data broker thread logs metrics.
manager_data_broker_thread_metric_interval = 15     # Seconds


#######################
### Worker settings ###
#######################

# Network Bridge.

# Initial connections.
# Each network bridge will make the following connections when they start up.
# Supply a list of tuples, with each tuple being a connection information tuple.
# (<connection type>, remote connection IP/Port, number of connections to make)
# Connection type: "zmq" or "pika"
# Remote connection: String; <IP:Port>, example: "queue:5672"
# Count: Integer; example: 5
# Example: 5 ZMQ based connections to a QM running on queue0 at port 24000: [("zmq", "queue0:24000", 5),]
bridge_data_worker_connections = []

# The default connection type for pika connections.
# Can either be: "rabbitmq_singular" or "rabbitmq_cluster"
bridge_data_worker_pika_connection_type = "rabbitmq_cluster"


# Manager.

# Initial data worker thread count.
manager_data_worker_thread_count = 12

# The max number of loops to wait for shared memory to validate changes requested by a worker thread.
# Note: Validation involves querying shared memory for its latest status; the higher the count, the more times and faster we hit shared memory while validating.
manager_data_worker_shared_memory_validation_check_count = 50

# The interval at which data workers send status reports to the main thread.
manager_data_worker_status_report_send_time_interval = 30


####################
### Log Settings ###
####################

# The file name BE processes log to; BE processes will inject their process type into the formatter.
LOG_FILENAME = "/var/log/appfirst/queue.{0}.log"

# The level the BE processes log at.
import logging
LOG_LEVEL = logging.DEBUG

# The log mode.  
#  local1 : sys_log 
#  anything else: local logging
LOG_MODE = "rotate"#"local1"

# The interval at which handler exit messages are logged, in seconds, to the log files.
LOG_EXIT_HANDLER_INTERVAL = 30


#############################
### Process name settings ###
#############################

# Defines the structure daemonized processes will use to rename themselves once running.
PROCESS_NAME_PREFIX = "AF"
PROCESS_NAME_SEPARATOR = "_"
PROCESS_NAME_NETWORK_BRIDGE = "QNB"
PROCESS_NAME_QUEUE_MANAGER = "QM"
PROCESS_NAME_TEST = "T"


######################
### Redis Settings ###
######################

# Redis settings
default_redis_address = "localhost:24009"


#######################
### StatsD Settings ###
#######################

# Set the enabled flag to true if BE processes should be submitting StatsD data to track their operations.
STATSD_ENABLED = True

# Set the base name for the StatsD Bucket names.  Attempt to use the environment specific value first.
try:
    from afqueue.queue_config import STATSD_FORMAT_BASE
except:
    STATSD_FORMAT_BASE = "dev.q"
    
# General bucket name formatters.
STATSD_FORMAT_CALL_COUNT = "count.call"
STATSD_FORMAT_EXCEPTION_COUNT = "count.exception"
STATSD_FORMAT_VALUE_ART = "value.art"

# Bucket names.
STATSD_NETWORK_BRIDGE_WORKER_CALL_COUNT = "{0}.{1}.{2}".format(STATSD_FORMAT_BASE, STATSD_FORMAT_CALL_COUNT, PROCESS_NAME_NETWORK_BRIDGE.lower())
STATSD_NETWORK_BRIDGE_WORKER_AVERAGE_RESPONSE_TIME = "{0}.{1}.{2}".format(STATSD_FORMAT_BASE, STATSD_FORMAT_VALUE_ART, PROCESS_NAME_NETWORK_BRIDGE.lower())
STATSD_NETWORK_BRIDGE_EXCEPTION_COUNT = "{0}.{1}.{2}".format(STATSD_FORMAT_BASE, STATSD_FORMAT_EXCEPTION_COUNT, PROCESS_NAME_NETWORK_BRIDGE.lower())

STATSD_QUEUE_MANAGER_EXCEPTION_COUNT = "{0}.{1}.{2}".format(STATSD_FORMAT_BASE, STATSD_FORMAT_EXCEPTION_COUNT, PROCESS_NAME_QUEUE_MANAGER.lower())

