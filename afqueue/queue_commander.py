#!/usr/bin/env python
# encoding: utf-8

"""
Executes commands to get status information from AFQM server processes
"""

import sys
import argparse
import zmq
import operator

try:
    from afqueue import queue_config as config
except ImportError:
    from afqueue.config_files import queue_config_base as config
from afqueue.common.socket_wrapper import SocketWrapper #@UnresolvedImport
from afqueue.common.zmq_utilities import ZmqUtilities
from afqueue.messages import command_messages
from afqueue.source.shared_memory_manager import SharedMemoryManager

# Set up our valid commands.
command_choice_list = [
    "shut_down", "add_dwt", "remove_dwt", "get_stats", "list_queues",
    "purge_queues", "purge_dump_queues", "freeze_queue", "delete_queues",
    "unlock_queue", "get_setup_data", "get_pecking_order",
]


class QueueCommander(object):
    pass


def queue_size_direct_query():
    redis_connection_string_list = parsed_arguments.address.split(",")
    full_queue_size_snapshot_dict = {}
    for redis_connection_string in redis_connection_string_list:
        shared_memory_manager = SharedMemoryManager(redis_connection_string)
        queue_size_snapshot_dict = shared_memory_manager.get_queue_sizes()

        for queue_name, queue_size in list(queue_size_snapshot_dict.items()):
            if queue_name not in list(full_queue_size_snapshot_dict.keys()):
                full_queue_size_snapshot_dict[queue_name] = 0
            full_queue_size_snapshot_dict[queue_name] += queue_size

    return full_queue_size_snapshot_dict


def get_queue_size_dict():
    # If the user put in the standard redis port, use a direct query.
    server_connection_list = parsed_arguments.address.split(",")
    port = server_connection_list[0].split(":")[1]
    if int(port) % 10 == 9:
        return queue_size_direct_query()
    else:
        queue_name_list = []
        if parsed_arguments.queues is not None:
            queue_name_list.extend(parsed_arguments.queues.split(","))

        from_all_servers_flag = True if parsed_arguments.option == "all" else False
        message = command_messages.CommandListQueuesRequestMessage(queue_name_list, from_all_servers_flag, parsed_arguments.note)
        reply_message_type = command_messages.CommandListQueuesReplyMessage

        # Run the command.
        full_reply_message = run_command(message, reply_message_type)

        # We will get queue size information separately from each QM we queried.
        if from_all_servers_flag == False:
            full_queue_size_dict = {}
            for reply_message in list(full_reply_message.values()):
                for queue_name, queue_size in list(reply_message.queue_size_dict.items()):
                    if queue_name not in list(full_queue_size_dict.keys()):
                        full_queue_size_dict[queue_name] = 0
                    full_queue_size_dict[queue_name] = full_queue_size_dict[queue_name] + queue_size
            return full_queue_size_dict
        else:
            return full_reply_message[list(full_reply_message.keys())[0]].queue_size_dict


def run_command(message, reply_message_type):
    """
    Actually send command to queue server's ZMQ socket and receive/return response
    """
    # Create the ZMQ context.
    zmq_context = zmq.Context(1)

    # Form return dictionary.
    results_dict = {}

    # Issue the command to each IP/Port combination we were given.
    for address in parsed_arguments.address.split(","):
        # Split the IP and port.
        split_data = address.split(":")
        bridge_ip = split_data[0]
        bridge_port = split_data[1]

        # Get the connection string and create the socket connection.
        connection_string = ZmqUtilities.get_socket_connection_string(bridge_ip, bridge_port)
        remote_request_socket = zmq_context.socket(zmq.REQ)
        remote_request_socket.setsockopt(zmq.LINGER, 0)
        remote_request_socket.setsockopt(zmq.RCVTIMEO, 1000)  # ms
        remote_request_socket.setsockopt(zmq.SNDTIMEO, 1000)  # ms
        remote_request_socket.connect(connection_string)

        try:
            # Send and wait for a reply.
            print(("Sending message: {0}".format(message)))
            message.send(remote_request_socket)

            # Get the message; initialize to not handled.
            print("Waiting for reply...")
            raw_message = SocketWrapper.pull_message(remote_request_socket)
            
            # Path off command.
            reply_message = reply_message_type.create_from_received(raw_message)
            if hasattr(reply_message, 'response_code'):
                if int(reply_message.response_code) >= 500:
                    raise Exception("Error code returned.  Reply: {0}".format(reply_message))

            #
            results_dict[address] = reply_message

        except:
            raise

        finally:
            # Destroy our socket.
            remote_request_socket.setsockopt(zmq.LINGER, 0)
            remote_request_socket.close()

    # Return results.
    return results_dict


def handle_get_setup_data():
    message = command_messages.CommandGetSetupDataRequestMessage(parsed_arguments.note)
    reply_message_type = command_messages.CommandGetSetupDataReplyMessage

    # Run the command.
    results_dict = run_command(message, reply_message_type)
    print("Command successful.")
    for destination_server_name, reply_message in list(results_dict.items()):
        print(("Server: ", destination_server_name))

        for exchange_wrapper in reply_message.exchange_wrapper_list:
            print(exchange_wrapper)

        for queue_wrapper in reply_message.queue_wrapper_list:
            print(queue_wrapper)

        if reply_message.notification != "":
            print((destination_server_name, " - Notification received: ", reply_message.notification))


def handle_delete_queue():
    if parsed_arguments.queues == None:
        raise Exception("No queues specified for delete queues request")
    message = command_messages.CommandDeleteQueuesRequestMessage(parsed_arguments.queues.split(","), parsed_arguments.note)

    # Run the command.
    results_dict = run_command(message, reply_message_type)
    print("Command successful.")
    for destination_server_name, reply_message in list(results_dict.items()):
        if reply_message.notification != "":
            print((destination_server_name, " - Notification received: ", reply_message.notification))


def handle_unlock_queue():
    if parsed_arguments.queues == None:
        raise Exception("No queues specified for delete queues request")
    elif len(parsed_arguments.queues.split(",")) != 1:
        raise Exception("Only one queue may be specified for request")
    message = command_messages.CommandUnlockQueueRequestMessage(parsed_arguments.queues, parsed_arguments.note)

    # Run the command.
    results_dict = run_command(message, reply_message_type)
    print("Command successful.")
    for destination_server_name, reply_message in list(results_dict.items()):
        if reply_message.notification != "":
            print((destination_server_name, " - Notification received: ", reply_message.notification))


def handle_shut_down():
    # Create message.
    message = command_messages.CommandShutDownMessage(parsed_arguments.note)

    # Run the command.
    results_dict = reply_message = run_command(message, reply_message_type)
    print("Command successful.")
    for destination_server_name, reply_message in list(results_dict.items()):
        if reply_message.notification != "":
            print((destination_server_name, " - Notification received: ", reply_message.notification))


def handle_add_worker_threads():
    # Create message.
    message = command_messages.CommandAddWorkersRequestMessage(parsed_arguments.count)

    # Run the command.
    results_dict = run_command(message, reply_message_type)
    print("Command successful.")
    for destination_server_name, reply_message in list(results_dict.items()):
        if reply_message.notification != "":
            print((destination_server_name, " - Notification received: ", reply_message.notification))


def handle_remove_worker_threads():
    # Create message.
    message = command_messages.CommandRemoveWorkersRequestMessage(parsed_arguments.count)

    # Run the command.
    results_dict = run_command(message, reply_message_type)
    print("Command successful.")
    for destination_server_name, reply_message in list(results_dict.items()):
        if reply_message.notification != "":
            print((destination_server_name, " - Notification received: ", reply_message.notification))


def handle_get_pecking_order():
    # Create message.
    message = command_messages.CommandGetPeckingOrderRequestMessage(parsed_arguments.note)
    reply_message_type = command_messages.CommandGetPeckingOrderReplyMessage

    # Run the command.
    results_dict = run_command(message, reply_message_type)

    # Handle response.
    print("Command successful.")
    for destination_server_name, reply_message in list(results_dict.items()):
        print(("Pecking order from: ", destination_server_name))
        print((str(reply_message.pecking_order_list)))
        if reply_message.notification != "":
            print(("Notification received: ", reply_message.notification))


def handle_get_statistics():
    # Create message.
    message = command_messages.CommandGetStatisticsRequestMessage("", parsed_arguments.note)
    reply_message_type = command_messages.CommandGetStatisticsReplyMessage

    # Run the command.
    results_dict = run_command(message, reply_message_type)

    # Handle response.
    print("Command successful.")
    for destination_server_name, reply_message in list(results_dict.items()):
        print(("\nStats from: ", destination_server_name))
        thread_name_list = sorted(reply_message.thread_dict.keys())
        for thread_name in thread_name_list:
            print(("Name: {0}; PID: {1}".format(thread_name, reply_message.thread_dict[thread_name])))
        if reply_message.notification != "":
            print(("Notification received: ", reply_message.notification))
        qm_id_string_list = sorted(reply_message.net_stat_dict.keys())
        print("*** Network memory usage ***")
        for qm_id_string in qm_id_string_list:
            used = reply_message.net_stat_dict[qm_id_string]["used"] / 1024 / 1024
            max = reply_message.net_stat_dict[qm_id_string]["max"] / 1024 / 1024
            reject = reply_message.net_stat_dict[qm_id_string]["reject"] / 1024 / 1024
            accept = reply_message.net_stat_dict[qm_id_string]["accept"] / 1024 / 1024
            print(("QM {0} - Status: {1} / {2} MB; R/A: {3} / {4} MB".format(qm_id_string, used, max, reject, accept)))
        if reply_message.notification != "":
            print(("Notification received: ", reply_message.notification))


def handle_purge_queues():
    if parsed_arguments.queues == None:
        raise Exception("No queues specified for purge queues request")
    if parsed_arguments.option == "all":
        from_all_servers_flag = True
    else:
        from_all_servers_flag = False
    message = command_messages.CommandPurgeQueuesRequestMessage(parsed_arguments.queues.split(","), from_all_servers_flag, parsed_arguments.note)

    # Run the command.
    results_dict = run_command(message, reply_message_type)
    print("Command successful.")
    for destination_server_name, reply_message in list(results_dict.items()):
        if reply_message.notification != "":
            print((destination_server_name, " - Notification received: ", reply_message.notification))


def handle_freeze_queue():
    if parsed_arguments.queues == None:
        raise Exception("No queue specified for request")
    elif len(parsed_arguments.queues.split(",")) != 1:
        raise Exception("Only one queue may be specified for request")
    elif parsed_arguments.freeze_push == None:
        raise Exception("No freeze_push flag set")
    elif parsed_arguments.freeze_pull == None:
        raise Exception("No freeze_pull flag set")
    freeze_push = True if parsed_arguments.freeze_push == "True" else False
    freeze_pull = True if parsed_arguments.freeze_pull == "True" else False
    message = command_messages.CommandFreezeQueueRequestMessage(parsed_arguments.queues, freeze_push, freeze_pull, parsed_arguments.note)

    # Run the command.
    results_dict = run_command(message, reply_message_type)
    print("Command successful.")
    for destination_server_name, reply_message in list(results_dict.items()):
        if reply_message.notification != "":
            print((destination_server_name, " - Notification received: ", reply_message.notification))


def handle_list_queues():
    queue_size_dict = get_queue_size_dict()
    sorted_tuple_list = sorted(iter(queue_size_dict.items()), key=operator.itemgetter(1), reverse=True)

    # Update max count if given.
    max_count = config.list_queues_max_display_count
    if parsed_arguments.count != None:
        max_count = int(parsed_arguments.count)

    # Display all items, capped by the max count.
    display_count = 0
    for queue_name, queue_size in sorted_tuple_list:
        print(("{0} :: {1}".format(queue_name, queue_size)))
        display_count += 1
        if max_count > 0 and display_count > max_count:
            break


def handle_purge_dump_queues():
    # Get the queue size dictionary.
    queue_size_dict = get_queue_size_dict()

    # Get a list of all dump queues.
    queue_name_list = []
    for queue_name, queue_size in list(queue_size_dict.items()):
        if queue_name.startswith("dump."):
            if queue_size > 0:
                queue_name_list.append(str(queue_name))

    #queue_name_list.append("statsx.tenant.0")
    #queue_name_list.append("statsx.tenant.1")
    #try:
    #    queue_name_list.remove("dump.log.data")
    #    queue_name_list.remove("dump.tofrontend")
    #except:
    #    pass

    # Form the command.
    if parsed_arguments.option == "all":
        from_all_servers_flag = True
    else:
        from_all_servers_flag = False
    message = command_messages.CommandPurgeQueuesRequestMessage(queue_name_list, from_all_servers_flag, parsed_arguments.note)

    # Run the command.
    reply_message = run_command(message, reply_message_type)


if __name__ == '__main__':
    # Parse our arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument("--command", choices=command_choice_list, required=True)
    parser.add_argument("--address", required=True, help="IP:Port of socket to receive command.  Multiple must be separated by commas.")
    parser.add_argument("--count", type=int, default=1, help="The iteration count of the command.  Integer.")
    #parser.add_argument("--type", help="The type specifier for the command.  Exact usage dependent on command type.")
    parser.add_argument("--queues", required=False, help="The name of the queues to run the command on.  Multiple must be separated by commas.")
    parser.add_argument("--option", choices=["all"], required=False)
    parser.add_argument("--freeze_push", choices=["True", "False"], required=False)
    parser.add_argument("--freeze_pull", choices=["True", "False"], required=False)
    parser.add_argument("--note", default="", required=False, help="Optional notification to appear in the destination logs when the command is received.")
    parsed_arguments = parser.parse_args(sys.argv[1:])

    # Create the command message before bothering to create our sockets, in case the command setup fails.
    # Set the reply message type we'll receive.
    reply_message_type = command_messages.CommandReplyMessage

    # Shutdown command.
    if parsed_arguments.command == "shut_down":
        handle_shut_down()

    # Remote connect command.
    elif parsed_arguments.command == "add_dwt":
        handle_add_worker_threads()

    # Remote disconnect command.
    elif parsed_arguments.command == "remove_dwt":
        handle_remove_worker_threads()

    # Get commands.
    elif parsed_arguments.command == "get_pecking_order":
        handle_get_pecking_order()
    elif parsed_arguments.command == "get_setup_data":
        handle_get_setup_data()
    elif parsed_arguments.command == "get_stats":
        handle_get_statistics()

    # Get list queues command.
    elif parsed_arguments.command == "list_queues":
        handle_list_queues()

    # Get queue size command.
    elif parsed_arguments.command == "purge_queues":
        handle_purge_queues()

    # Get queue size command.
    elif parsed_arguments.command == "purge_dump_queues":
        handle_purge_dump_queues()

    # Get freeze queue command.
    elif parsed_arguments.command == "freeze_queue":
        handle_freeze_queue()

    # Get queue size command.
    elif parsed_arguments.command == "delete_queues":
        handle_delete_queue()

    # Get queue size command.
    elif parsed_arguments.command == "unlock_queue":
        handle_unlock_queue()

    # Unknown command.
    else:
        raise Exception("Invalid command type specified")
