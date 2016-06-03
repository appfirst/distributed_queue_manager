#from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
#from afqueue.stats.client import Statsd #@UnresolvedImport
import inspect


class Utility:

    @staticmethod
    def get_stack_trace():
        return inspect.stack()[1:]
    

    @staticmethod
    def send_stats_count(statsd_library, bucket_name, count = 1):
        
        # If StatsD is enabled, send to the appropriate method based on the count.
        if statsd_library != None:
            if count == 1:
                statsd_library.increment(bucket_name)
            else:
                statsd_library.update_stats(bucket_name, count)


    @staticmethod
    def send_stats_time(statsd_library, bucket_name, time):
        
        # If StatsD is enabled, send as a timer.
        if statsd_library != None:
            statsd_library.timing(bucket_name, time)
            

    @staticmethod
    def send_stats_gauge(statsd_library, bucket_name, value):
        
        # If StatsD is enabled, send as a timer.
        if statsd_library != None:
            statsd_library.gauge(bucket_name, value)
                