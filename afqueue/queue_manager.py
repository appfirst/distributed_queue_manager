# Pull in our config.
try:
    from afqueue import queue_config as config
except:
    from afqueue.config_files import queue_config_base as config

# Parse our arguments.
import argparse, sys
parser = argparse.ArgumentParser()
parser.add_argument("--remote_ip", required=False)
parser.add_argument("--remote_ip_interface", required=False)
parser.add_argument("--port", type=int, default=config.manager_default_port, required=False)
parser.add_argument("--redis_address", default=config.default_redis_address, required=False)
parser.add_argument("--peer", required=False)
parser.add_argument("--debug", choices=["on", "off"], required=False)
parser.add_argument("--rename", choices=["on", "off"], required=False)
parsed_arguments = parser.parse_args(sys.argv[1:])


# Setup base process name from parsed arguments.
process_base_name = "{1}{0}{2}".format(config.PROCESS_NAME_SEPARATOR, config.PROCESS_NAME_PREFIX, config.PROCESS_NAME_QUEUE_MANAGER)


# Create the logger before we import any other object which might touch it.
from afqueue.common.logger import Logger
log_file_name = "queue_manager"
log_file_path = config.LOG_FILENAME.format(log_file_name)
logger = Logger(process_base_name, "queue", config.LOG_LEVEL, config.LOG_MODE, log_file_path, config.LOG_EXIT_HANDLER_INTERVAL) 


# Setup our signal trackers.    
import signal
SIGNAL_RECEIVED = None
def sighandler(signum, frame):
    global SIGNAL_RECEIVED
    SIGNAL_RECEIVED = signum
    

# Import the rest of our dependencies.
from afqueue.common.daemon import Daemon
from afqueue.common.network_utilities import NetworkUtilities
from afqueue.common.exception_formatter import ExceptionFormatter
from afqueue.source.queue_manager import QueueManager
import os, time
import zmq #@UnresolvedImport
            
# Wrap the main logic in a daemon wrapper so we can daemonize it.
class DaemonWrapper(Daemon):
    
    
    def __init__(self, parsed_arguments):
        Daemon.__init__(self)
        self.parsed_arguments = parsed_arguments
        self.current_process_name = ""
        
        
    def run(self):
        
        # Log entry and register signal.
        logger.log_info("Process started: {0}".format(self.parsed_arguments))
        
        # Setup StatsD.
        if config.STATSD_ENABLED == True:
            from afqueue.stats.client import Statsd
            from afqueue.stats.afclient import AFTransport
            Statsd.set_transport(AFTransport(logger=logger.get_logger()))
        else:
            Statsd = None
        
        # Setup the signal handler.
        global SIGNAL_RECEIVED
        signal.signal(signal.SIGUSR1, sighandler)
        signal.signal(signal.SIGUSR2, sighandler)
        
        # Get the remote IP we will tell other machines to use to connect to us.
        remote_ip = self.parsed_arguments.remote_ip
        if remote_ip == None:
            remote_ip_interface = self.parsed_arguments.remote_ip_interface
            if remote_ip_interface == None:
                config.remote_ip_interface
            remote_ip = NetworkUtilities.get_interface_ip_address(remote_ip_interface)
            logger.log_info("Found and used IP {0} as remote IP.".format(remote_ip))
        
        # Create the queue manager.
        queue_manager = QueueManager(remote_ip, self.parsed_arguments.port, 
                                     self.parsed_arguments.peer, config, logger, Statsd, self.parsed_arguments.redis_address)        
                
        try:        
            
            # Update the process name before going into our loop.
            self.update_process_name()

            # Enter the queue read and process loop.
            while queue_manager.is_running:
    
                # Run the bridge loop.
                if queue_manager.run_loop() == False:
                    logger.log_error("Error running main loop.  Shutting down.")
                
                # Check if we've received a signal.
                if SIGNAL_RECEIVED != None:
                    
                    # USR1: Shut down.
                    if SIGNAL_RECEIVED == signal.SIGUSR1:
                        break
                    
                    # USR1: Reload config.
                    elif SIGNAL_RECEIVED == signal.SIGUSR2:
                        logger.log_info("::: RELOAD CONFIG ENCOUNTERED ::: Not coded yet")
                        
                    # Reset signal.
                    SIGNAL_RECEIVED = None
                    
                # Sleep.
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            
            # Log.
            logger.log_info("Shutdown keyboard command received.  Shutting down ...")
                        
        except:
            
            # Log the exception.
            logger.log_error("::: *** EXCEPTION: {0}".format(ExceptionFormatter.get_message()), True)
        
        finally:
                
            # Force shut down if it hasn't been ran.
            if queue_manager.shut_down_ran == False:
                queue_manager.shut_down()
        
            # Log entry and register signal.
            logger.log_info("Process stopped")
           
        
    def get_current_process_name(self):
        
        # Get the base process name.
        process_name = process_base_name
            
        # Get the run option.
        process_name += "d" if self.parsed_arguments.debug == "on" else "D"
                
        # Return the process name string.
        return process_name
        
        
    def update_process_name(self):
        
        # If we started up with rename off, return out.
        if self.parsed_arguments.rename == "off":
            return
        
        # Get the current process name.
        current_process_name = self.get_current_process_name()

        # If the process name has changed, store locally and update.
        if self.current_process_name != current_process_name:
            self.current_process_name = current_process_name
            self.set_proc_name(self.current_process_name)
            
        
if __name__ == "__main__":
    
    # Create the daemon wrapper.
    daemon = DaemonWrapper(parsed_arguments)
    
    try:
        
        # Start in debug or daemon mode based on the optional debug flag.
        if parsed_arguments.debug == "on":
            print("Executing start debug command; Stdout being displayed.")
            daemon.start_debug()
            
        else:
            print("Executing start daemon command...")
            daemon.start()

    except SystemExit:
        
        pass

    except Exception:
        
        logger.log_error(ExceptionFormatter.get_message())
        raise ExceptionFormatter.get_full_exception()
        
    sys.exit(0)
    