from queue import Empty as EmptyQueueException
from afqueue.common.exception_formatter import ExceptionFormatter #@UnresolvedImport
from afqueue.messages import message_types, system_messages #@UnresolvedImport


def consume_action_queue(thread_name, action_queue, notification_out_queue):
    
    # Enter a loop which will only break when we receive no more messages from the queue we are handling.
    while True:
        
        try:
            
            # Get a new message; initialize it to not handled.
            message = action_queue.get(False)
            message_handled = False

            if message.get_primary_type() == message_types.SYSTEM:                        
                if message.get_type() == message_types.SYSTEM_STOP_THREAD:
                    notification_out_queue.put(system_messages.SystemNotificationMessage(thread_name, "Shutting down per main thread request"))
                    message_handled = True
                    return False
                
            # If the message hasn't been handled, notify.
            if message_handled == False:
                notification_out_queue.put(system_messages.SystemErrorMessage(thread_name, "Thread notification handler found a message it could not process: " + str(message)))

        except EmptyQueueException:
            
            # Break from the loop; there are no more messages to handle.
            break
            
        except IOError as e:
            
            # Notify of the IO error and return True (which will signal a shut down).
            notification_out_queue.put(system_messages.SystemErrorMessage(thread_name, "IO Error ({0}).  Shutting down.".format(e)))
            return False
        
        except:
            
            # Notify the general exception.
            notification_out_queue.put(system_messages.SystemErrorMessage(thread_name, "Exception while checking in-bound notifications: " + ExceptionFormatter.get_message()))
            return False
        
    # Return true - we should keep running.
    return True