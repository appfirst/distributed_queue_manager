import time, sys, traceback, codecs
import logging.handlers

LOG_LEVEL_DEBUG = 1
LOG_LEVEL_INFO = 2
LOG_LEVEL_WARNING = 3
LOG_LEVEL_ERROR = 4
LOG_LEVEL_CRITICAL = 5

class Logger(object):
    
    def __init__(self, process_tag, logger_base, log_level = logging.INFO, log_mode = "local1", log_file_path = "", minimum_log_interval = 30):
        """
        process tag: the process tag to appear in each log line.
        log level: the minimum log level to actually be logged (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL).
        log mode: what type of logging we are using - SysLog = Local1; Rotating File Handler = anything else. 
        log file path: the full path to the log file; used when we aren't using local1 (i.e., SysLog).
        minimum log interval: the minimum time between handler logs.
        """
            
        # Default the process tag to a generic if none was specified.
        if process_tag == None:
            process_tag = "AF_X"
            
        # Create the loggers.
        self._standard_logger, self._incident_logger = self._create_loggers("{0}.standard".format(logger_base), "{0}.incident".format(logger_base), process_tag, log_level, log_mode, log_file_path)
            
        # Setup the log interval tracking.
        self.minimum_log_interval = minimum_log_interval
        self.last_log_time_dict = {}
        self.object_handled_count_dict = {}
        
        
    def _create_loggers(self, logger_name, incident_logger_name, process_tag, log_level, log_mode, log_file_path):
        
        # Create the logger object.
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        
        # Create the handler and formatter, based on our log mode.
        if log_mode == "local1":
            log_handler = logging.handlers.SysLogHandler(facility="local1", address="/dev/log")
            formatter = logging.Formatter("{0}:".format(process_tag) + 
                "%(process)d|"
                "%(module)s:%(funcName)s:%(lineno)d|"
                "%(levelname)s|"
                "%(message)s",
                "%m.%d-%H:%M:%S")
        else:
            log_handler = logging.handlers.TimedRotatingFileHandler(log_file_path, "midnight", 1, 3)
            formatter = logging.Formatter("%(asctime)s|{0}:".format(process_tag) + 
                "%(process)d|"
                "%(module)s:%(funcName)s:%(lineno)d|"
                "%(levelname)s|"
                "%(message)s",
                "%m.%d-%H:%M:%S")
        
        # Set the formatter and log level for the handler.
        log_handler.setFormatter(formatter)
        log_handler.setLevel(log_level)
        
        # Add the handler to our logger.
        logger.addHandler(log_handler)
            
        # Create the incident logger.
        incident_logger = logging.getLogger(incident_logger_name)
        incident_logger.setLevel(logging.DEBUG)
        
        # Return the logger.
        return logger, incident_logger
    
    
    def get_logger(self, incident_logger = False):
        
        # Return the appropriate logger.
        if incident_logger == False:
            return self._standard_logger
        else:
            return self._incident_loggger
                    
                    
    def log_critical(self, message, log_incident = False):
        
        # Log to standard; Log to incident if desired.
        self._standard_logger.critical(message)
        if log_incident == True and len(self._incident_logger.handlers) > 0:
            self._incident_logger.critical(message)
            
        
    def log_debug(self, message, log_incident = False):
        
        # Log to standard; Log to incident if desired.
        self._standard_logger.debug(message)
        if log_incident == True and len(self._incident_logger.handlers) > 0:
            self._incident_logger.debug(message)
    
    
    def log_error(self, message, log_incident = False):
        
        # Log to standard; Log to incident if desired.
        self._standard_logger.error(message)
        if log_incident == True and len(self._incident_logger.handlers) > 0:
            self._incident_logger.error(message)
    
        
    def log_info(self, message, log_incident = False):
        
        # Log to standard; Log to incident if desired.
        self._standard_logger.info(message)
        if log_incident == True and len(self._incident_logger.handlers) > 0:
            self._incident_logger.info(message)
            
        
    def log_warning(self, message, log_incident = False):
        
        # Log to standard; Log to incident if desired.
        self._standard_logger.warning(message)
        if log_incident == True and len(self._incident_logger.handlers) > 0:
            self._incident_logger.warning(message)

    
    def log_handle_exit(self, handle_method, handled_count, message_formatter, log_method = None):
        
        # If the handler index doesn't exist in our tracker, add it with default values.
        if handle_method not in list(self.last_log_time_dict.keys()):
            self.last_log_time_dict[handle_method] = 0
            self.object_handled_count_dict[handle_method] = 0
        
        # Increment the number of objects we've handled since the last logging of a handle exit.
        self.object_handled_count_dict[handle_method] += handled_count
        
        # Test if we should log the exit message based on timing.
        current_time = time.time()
        log_interval = current_time - self.last_log_time_dict[handle_method]
        if log_interval >= self.minimum_log_interval:
            
            # If we weren't given a log method, default to the info logger.
            if log_method == None:
                log_method = self._standard_logger.info
                
            # Log the message and reset our trackers.
            log_method(message_formatter.format(self.object_handled_count_dict[handle_method]))
            self.last_log_time_dict[handle_method] = current_time
            self.object_handled_count_dict[handle_method] = 0
            
            
    def _log_incident(self, message, method):
        
        
        pass
        
        
    def save_coredump(self, fn):
        tb = sys.exc_info()[2]
        stack = []
        while tb:
            stack.append(tb.tb_frame)
            tb = tb.tb_next
    
        with codecs.open(fn, "w", encoding="utf-8") as fp:
            stack.reverse()
            traceback.print_exc(file=fp)
    
            lines = []
            lines.append("\nLocals by frame")
            for frame in stack:
                lines.append("Frame %s in %s at line %s" % (
                    frame.f_code.co_name,
                    frame.f_code.co_filename,
                    frame.f_lineno))
                for k, v in frame.f_locals.items():
                    lines.append("  {0}={1}".format(k, v))
                lines.append("")
            fp.write("\n".join(lines))
