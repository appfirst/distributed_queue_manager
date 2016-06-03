"""
DEFAULT CONFIG FILE

The primary purpose of this file is to get the 
private pod up and running
"""
# The prefix for all bucket names the BE processes will use when submitting their operation StatsD data.
# Set this before anything else to ensure this value is used in the generic StatsD bucket names configuration file.
STATSD_FORMAT_BASE = "dev.q.pod.default"

# Import base configuration and override settings.
from afqueue.config_files.queue_config_base import *

# Utilize sys log.
LOG_MODE = "local1"

# Turn off StatsD.
STATSD_ENABLED = True 

# 
manager_default_peer_list = ["pipeline0:24000", "pipeline1:24000"]

# Set default bridge connections.
bridge_data_worker_connections = []#("pika", "pod-dbank-lab-queue0:5672", 5),]
