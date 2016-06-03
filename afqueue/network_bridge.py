# Pull in our config.
try:
    from afqueue import queue_config as config
except:
    from afqueue.config_files import queue_config_base as config

# Parse our arguments.
import argparse, sys
parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, default=config.bridge_default_port, required=False)
parser.add_argument("--debug", choices=["on", "off"], required=False)
parser.add_argument("--rename", choices=["on", "off"], required=False)
parsed_arguments = parser.parse_args(sys.argv[1:])


# Setup base process name from parsed arguments.
process_base_name = "{1}{0}{2}".format(config.PROCESS_NAME_SEPARATOR, config.PROCESS_NAME_PREFIX, config.PROCESS_NAME_NETWORK_BRIDGE)


# Create the logger before we import any other object which might touch it.
from afqueue.common.logger import Logger
log_file_name = "network_bridge"
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
from afqueue.common.exception_formatter import ExceptionFormatter
from afqueue.source.network_bridge import NetworkBridge
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
    
        # Create the bridge.
        network_bridge = NetworkBridge(Statsd, self.parsed_arguments.port, config, logger)
        
        try:
        
            
            # Update the process name before going into our loop.
            self.update_process_name()

            # Enter the queue read and process loop.
            while network_bridge.is_running:
    
                # Run the bridge loop.
                if network_bridge.run_loop() == False:
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
            
            print("Shutdown keyboard command received.  Shutting down ...")
            
        except:
            
            # Log the exception.
            logger.log_error("::: *** EXCEPTION: {0}".format(ExceptionFormatter.get_message()), True)
        
        finally:
                
            # Force shut down if it hasn't been ran.
            if network_bridge.shut_down_ran == False:
                network_bridge.shut_down()
        
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
    
    # Start in debug or daemon mode based on the optional debug flag.
    if parsed_arguments.debug == "on":
        print("Executing start debug command; Stdout being displayed.")
        daemon.start_debug()
        
    else:
        print("Executing start daemon command...")
        daemon.start()
        
    sys.exit(0)
    
