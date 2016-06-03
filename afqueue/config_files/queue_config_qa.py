# The prefix for all bucket names the BE processes will use when submitting their operation StatsD data.
# Set this before anything else to ensure this value is used in the generic StatsD bucket names configuration file.
STATSD_FORMAT_BASE = "dev.q.mn"

# Import base configuration and override settings.
from afqueue.config_files.queue_config_base import *

# Utilize sys log.
LOG_MODE = "local1"

# Disable StatsD for now.
STATSD_ENABLED = False

# Set the default bridge data worker's Pika connection type to singular
bridge_data_worker_pika_connection_type = "rabbitmq_singular"

# Set default bridge connections.
bridge_data_worker_connections = [("pika", "queue0:5672", 3),]