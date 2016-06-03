# If your using statsd
STATSD_FORMAT_BASE = 'defaut.statsd.changeme'
from afqueue.config_files.queue_config_base import *

# syslog 
LOG_MODE = 'local1'
bridge_data_worker_connections = []

# Defined queue servers that will talk to each other
manager_default_peer_list = ['default0:24000', 'default1:24000']

# Any named queues that will reject data if the queue becomes too full.
manager_shared_memory_overflow_queue_rejection_set = ['default.queue']
