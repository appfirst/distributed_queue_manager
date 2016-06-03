# The prefix for all bucket names the BE processes will use when submitting their operation StatsD data.
# Set this before anything else to ensure this value is used in the generic StatsD bucket names configuration file.
STATSD_FORMAT_BASE = "dev.q.ny"

# Import base configuration and override settings.
from afqueue.config_files.queue_config_base import *

# Utilize sys log.
LOG_MODE = "local1"

# Increase the worker thread count.
manager_data_worker_thread_count = 24

# Set custom default peer list.
manager_default_peer_list = ["spike:24000", "snoopy:24000"]

# Set custom shared memory monitoring.
manager_shared_memory_max_size = int(4 * 1024 * 1024 * 1024) # GB * MB * KB * B
manager_shared_memory_ordered_data_reject_threshold = 0.95 * manager_shared_memory_max_size
manager_shared_memory_ordered_data_accept_threshold = 0.80 * manager_shared_memory_max_size
manager_shared_memory_overflow_queue_rejection_set = ["collector.data.be","collector.logdata.be",]

# Set default bridge connections.
bridge_data_worker_connections = [("pika", "queue0:5672", 15),]

# Set the default bridge data worker's Pika connection type to singular
bridge_data_worker_pika_connection_type = "rabbitmq_singular"

# Set default bridge connections.
bridge_data_worker_connections = [("pika", "queue0:5672", 3),]
