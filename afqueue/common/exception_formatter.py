import sys
import os


# Static utility methods related to XML parsing.
class ExceptionFormatter(object):
    
    @staticmethod
    def get_message():
        
        # Return full exception data.
        exc_type, exc_obj, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        return "E Type: {0}; E File: {1}; E Line: {2}; E: {3}.".format(exc_type, file_name, exc_tb.tb_lineno, exc_obj)
    
    
    @staticmethod
    def get_full_exception():
        
        # Return full exception data.
        exc_type, exc_obj, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        return exc_type("E Type: {0}; E File: {1}; E Line: {2}; E: {3}.".format(exc_type, file_name, exc_tb.tb_lineno, exc_obj))
