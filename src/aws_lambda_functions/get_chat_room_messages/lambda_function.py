import logging
import os
from binascii import unhexlify, hexlify
from typing import *
import uuid
from threading import Thread
from queue import Queue
from cassandra.query import SimpleStatement
import databases
import utils

# Configure the logging tool in the AWS Lambda function.
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# Initialize constants with parameters to configure.
CASSANDRA_USERNAME = os.environ["CASSANDRA_USERNAME"]
CASSANDRA_PASSWORD = os.environ["CASSANDRA_PASSWORD"]
CASSANDRA_HOST = os.environ["CASSANDRA_HOST"].split(",")
CASSANDRA_PORT = int(os.environ["CASSANDRA_PORT"])
CASSANDRA_LOCAL_DC = os.environ["CASSANDRA_LOCAL_DC"]
CASSANDRA_KEYSPACE_NAME = os.environ["CASSANDRA_KEYSPACE_NAME"]


# The connection to the database will be created the first time the AWS Lambda function is called.
# Any subsequent call to the function will use the same database connection until the container stops.
CASSANDRA_CONNECTION = None


def run_multithreading_tasks(functions: List[Dict[AnyStr, Union[Callable, Dict[AnyStr, Any]]]]) -> Dict[AnyStr, Any]:
    # Create the empty list to save all parallel threads.
    threads = []

    # Create the queue to store all results of functions.
    queue = Queue()

    # Create the thread for each function.
    for function in functions:
        # Check whether the input arguments have keys in their dictionaries.
        try:
            function_object = function["function_object"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)
        try:
            function_arguments = function["function_arguments"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)

        # Add the instance of the queue to the list of function arguments.
        function_arguments["queue"] = queue

        # Create the thread.
        thread = Thread(target=function_object, kwargs=function_arguments)
        threads.append(thread)

    # Start all parallel threads.
    for thread in threads:
        thread.start()

    # Wait until all parallel threads are finished.
    for thread in threads:
        thread.join()

    # Get the results of all threads.
    results = {}
    while not queue.empty():
        results = {**results, **queue.get()}

    # Return the results of all threads.
    return results


def check_input_arguments(**kwargs) -> None:
    # Make sure that all the necessary arguments for the AWS Lambda function are present.
    try:
        input_arguments = kwargs["event"]["arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        queue = kwargs["queue"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Check the format and values of required arguments in the list of input arguments.
    required_arguments = ["chatRoomId", "fetchSize"]
    for argument_name, argument_value in input_arguments.items():
        if argument_name in required_arguments and argument_value is None:
            raise Exception("The '{0}' argument can't be None/Null/Undefined.".format(argument_name))
        if argument_name.endswith("Id"):
            try:
                uuid.UUID(argument_value)
            except ValueError:
                raise Exception("The '{0}' argument format is not UUID.".format(argument_name))

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "chat_room_id": input_arguments.get("chatRoomId", None),
            "fetch_size": input_arguments.get("fetchSize", None),
            "paging_state": input_arguments.get("pagingState", None)
        }
    })

    # Return nothing.
    return None


def reuse_or_recreate_cassandra_connection(queue: Queue) -> None:
    global CASSANDRA_CONNECTION
    if not CASSANDRA_CONNECTION:
        try:
            CASSANDRA_CONNECTION = databases.create_cassandra_connection(
                CASSANDRA_USERNAME,
                CASSANDRA_PASSWORD,
                CASSANDRA_HOST,
                CASSANDRA_PORT,
                CASSANDRA_LOCAL_DC
            )
        except Exception as error:
            logger.error(error)
            raise Exception("Unable to connect to the Cassandra database.")
    queue.put({"cassandra_connection": CASSANDRA_CONNECTION})
    return None


def get_aggregated_data(**kwargs) -> Dict[AnyStr, Any]:
    # Check if the input dictionary has all the necessary keys.
    try:
        cassandra_connection = kwargs["cassandra_connection"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        cql_arguments = kwargs["cql_arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        fetch_size = cql_arguments["fetch_size"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        paging_state = cql_arguments["paging_state"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare the CQL query that gets the list of messages of the specific chat room.
    cql_query = """
    select
        message_id,
        message_author_id,
        message_channel_id,
        message_content,
        message_created_date_time,
        message_deleted_date_time,
        message_is_delivered,
        message_is_read,
        message_is_sent,
        message_text,
        message_updated_date_time,
        quoted_message_author_id,
        quoted_message_channel_id,
        quoted_message_content,
        quoted_message_id,
        quoted_message_text
    from
        chat_rooms_messages
    where
        chat_room_id = %(chat_room_id)s;
    """
    cql_statement = SimpleStatement(cql_query, fetch_size=fetch_size)

    # Execute the CQL query dynamically, in a convenient and safe way.
    try:
        if paging_state:
            chat_room_messages_data = cassandra_connection.execute(
                cql_statement,
                cql_arguments,
                paging_state=unhexlify(paging_state)
            )
        else:
            chat_room_messages_data = cassandra_connection.execute(cql_statement, cql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Get the next paging state.
    paging_state = chat_room_messages_data.paging_state

    # Alternative form of the ternary operator in Python. Format: (expression_on_false, expression_on_true)[predicate]
    paging_state = (None, paging_state)[paging_state is not None]

    # Define the next paging state that will be sent in the response to the frontend.
    paging_state = hexlify(paging_state).decode() if paging_state else None

    # Create the response structure and return it.
    return {
        "paging_state": paging_state,
        "chat_room_messages_data": chat_room_messages_data
    }


def analyze_and_format_chat_room_messages_data(**kwargs) -> List[Dict[AnyStr, Any]]:
    # Check if the input dictionary has all the necessary keys.
    try:
        chat_room_messages_data = kwargs["chat_room_messages_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the chat room messages data.
    chat_room_messages = []
    for chat_room_message_data in chat_room_messages_data.current_rows:
        chat_room_message, quoted_message = {}, {}
        for key, value in chat_room_message_data.items():
            if key.endswith("_date_time") and value is not None:
                value = value.isoformat()
            elif key.endswith("_id") and value is not None:
                value = str(value)
            if key.startswith("quoted_"):
                quoted_message[utils.camel_case(key.replace("quoted_", ""))] = value
            else:
                chat_room_message[utils.camel_case(key)] = value
        chat_room_message["quotedMessage"] = quoted_message
        chat_room_messages.append(chat_room_message)

    # Return the list of analyzed and formatted data about chat room messages.
    return chat_room_messages


def lambda_handler(event, context):
    """
    :param event: The AWS Lambda function uses this parameter to pass in event data to the handler.
    :param context: The AWS Lambda function uses this parameter to provide runtime information to your handler.
    """
    # Run several initialization functions in parallel.
    results_of_tasks = run_multithreading_tasks([
        {
            "function_object": check_input_arguments,
            "function_arguments": {
                "event": event
            }
        },
        {
            "function_object": reuse_or_recreate_cassandra_connection,
            "function_arguments": {}
        }
    ])

    # Define the input arguments of the AWS Lambda function.
    input_arguments = results_of_tasks["input_arguments"]
    chat_room_id = uuid.UUID(input_arguments["chat_room_id"])
    fetch_size = input_arguments["fetch_size"]
    paging_state = input_arguments["paging_state"]

    # Define the instances of the database connections.
    cassandra_connection = results_of_tasks["cassandra_connection"]

    # This statement must fix ERROR NoHostAvailable: ('Unable to complete the operation against any hosts').
    success = False
    while not success:
        try:
            cassandra_connection.set_keyspace(CASSANDRA_KEYSPACE_NAME)
            success = True
        except Exception as error:
            try:
                cassandra_connection = databases.create_cassandra_connection(
                    CASSANDRA_USERNAME,
                    CASSANDRA_PASSWORD,
                    CASSANDRA_HOST,
                    CASSANDRA_PORT,
                    CASSANDRA_LOCAL_DC
                )
                global CASSANDRA_CONNECTION
                CASSANDRA_CONNECTION = cassandra_connection
            except Exception as error:
                logger.error(error)
                raise Exception(error)

    # Get the aggregated data.
    aggregated_data = get_aggregated_data(
        cassandra_connection=cassandra_connection,
        cql_arguments={
            "chat_room_id": chat_room_id,
            "fetch_size": fetch_size,
            "paging_state": paging_state
        }
    )

    # Define a few necessary variables that will be used in the future.
    paging_state = aggregated_data["paging_state"]
    chat_room_messages_data = aggregated_data["chat_room_messages_data"]

    # Define the variable that stores information about chat room messages.
    chat_room_messages = analyze_and_format_chat_room_messages_data(chat_room_messages_data=chat_room_messages_data)

    # Create the response structure and return it.
    return {
        "pagingState": paging_state,
        "chatRoomMessages": chat_room_messages[::-1]  # Reverse the list.
    }
