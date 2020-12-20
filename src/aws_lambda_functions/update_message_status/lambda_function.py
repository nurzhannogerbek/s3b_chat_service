import logging
import os
from cassandra.cluster import Session
from typing import *
import uuid
from threading import Thread
from queue import Queue
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
        input_arguments = kwargs["event"]["arguments"]["input"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        queue = kwargs["queue"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Check the format and values of required arguments in the list of input arguments.
    required_arguments = ["chatRoomId", "messagesIds", "messageStatus"]
    for argument_name, argument_value in input_arguments.items():
        if argument_name not in required_arguments:
            raise Exception("The '%s' argument doesn't exist.".format(utils.camel_case(argument_name)))
        if argument_value is None:
            raise Exception("The '%s' argument can't be None/Null/Undefined.".format(utils.camel_case(argument_name)))
        if argument_name.endswith("Id"):
            try:
                uuid.UUID(argument_value)
            except ValueError:
                raise Exception("The '%s' argument format is not UUID.".format(utils.camel_case(argument_name)))

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "chat_room_id": input_arguments.get("chatRoomId", None),
            "messages_ids": input_arguments.get("messagesIds", None),
            "message_status": input_arguments.get("messageStatus", None)
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


def set_cassandra_keyspace(cassandra_connection: Session) -> None:
    # This peace of code fix ERROR NoHostAvailable: ("Unable to complete the operation against any hosts").
    successful_operation = False
    while not successful_operation:
        try:
            cassandra_connection.set_keyspace(CASSANDRA_KEYSPACE_NAME)
            successful_operation = True
        except Exception as error:
            try:
                cassandra_connection = databases.create_cassandra_connection(
                    CASSANDRA_USERNAME,
                    CASSANDRA_PASSWORD,
                    CASSANDRA_HOST,
                    CASSANDRA_PORT,
                    CASSANDRA_LOCAL_DC
                )
            except Exception as error:
                logger.error(error)
                raise Exception(error)

    # Return nothing.
    return None


def update_chat_room_messages_statuses(**kwargs) -> None:
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
        column_name = cql_arguments["column_name"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        messages_ids = cql_arguments["messages_ids"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare the CQL query that updates the status of the message.
    cql_statement = """
    update
        chat_rooms_messages
    set
        {0} = true
    where
        chat_room_id = %(chat_room_id)s
    and
        message_id = %(message_id)s
    if exists;
    """.format(column_name.lower())

    # Update the status of each message individually.
    for message_id in messages_ids:
        # Add or update the value of the argument.
        cql_arguments["message_id"] = uuid.UUID(message_id)

        # Execute the CQL query dynamically, in a convenient and safe way.
        try:
            cassandra_connection.execute(cql_statement, cql_arguments)
        except Exception as error:
            logger.error(error)
            raise Exception(error)

    # Return nothing.
    return None


def get_chat_room_messages(**kwargs) -> List[Dict[AnyStr, Any]]:
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
        messages_ids = cql_arguments["messages_ids"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare the CQL query that gets data the last chat room message.
    cql_statement = """
    select
        chat_room_id,
        message_id,
        message_author_id,
        message_channel_id,
        message_content_url,
        message_created_date_time,
        message_deleted_date_time,
        message_is_delivered,
        message_is_read,
        message_is_sent,
        message_text,
        message_type,
        message_updated_date_time,
        quoted_message_author_id,
        quoted_message_channel_id,
        quoted_message_content_url,
        quoted_message_id,
        quoted_message_text,
        quoted_message_type
    from
        chat_rooms_messages
    where
        chat_room_id = %(chat_room_id)s
    and
        message_id = %(message_id)s
    limit 1;
    """

    # Define the empty list to store information about messages.
    chat_room_messages = []

    # Get information of each message individually.
    for message_id in messages_ids:
        # Add or update the value of the argument.
        cql_arguments["message_id"] = uuid.UUID(message_id)

        # Execute the CQL query dynamically, in a convenient and safe way.
        try:
            chat_room_message = cassandra_connection.execute(cql_statement, cql_arguments).one()
        except Exception as error:
            logger.error(error)
            raise Exception(error)

        # Add the message information to the list
        chat_room_messages.append(chat_room_message)

    # Return the list of chat room messages.
    return chat_room_messages


def analyze_and_format_chat_room_messages(**kwargs) -> List[Dict[AnyStr, Any]]:
    # Check if the input dictionary has all the necessary keys.
    try:
        chat_room_messages = kwargs["chat_room_messages"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the last message data.
    messages = []
    for chat_room_message in chat_room_messages:
        message = {}
        quoted_message = {}
        for key, value in chat_room_message.items():
            if key.endswith("_date_time") and value is not None:
                value = value.isoformat()
            elif key.endswith("_id") and value is not None:
                value = str(value)
            if key.startswith("quoted_"):
                quoted_message[utils.camel_case(key.replace("quoted_", ""))] = value
            else:
                message[utils.camel_case(key)] = value
        message["quotedMessage"] = quoted_message
        messages.append(message)

    # Return analyzed and formatted messages.
    return messages


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
    chat_room_id = input_arguments["chat_room_id"]
    messages_ids = input_arguments["messages_ids"]
    # Available values: "message_is_delivered", "message_is_read", "message_is_sent".
    message_status = input_arguments['message_status']

    # Define the instances of the database connections.
    cassandra_connection = results_of_tasks["cassandra_connection"]
    set_cassandra_keyspace(cassandra_connection=cassandra_connection)

    # Update the statuses of different messages.
    update_chat_room_messages_statuses(
        cassandra_connection=cassandra_connection,
        cql_arguments={
            "column_name": message_status,
            "chat_room_id": uuid.UUID(chat_room_id),
            "messages_ids": messages_ids
        }
    )

    # Define the variable that stores information about messages.
    chat_room_messages = get_chat_room_messages(
        cassandra_connection=cassandra_connection,
        cql_arguments={
            "chat_room_id": uuid.UUID(chat_room_id),
            "messages_ids": messages_ids
        }
    )

    # Define variables that stores formatted information about messages.
    chat_room_messages = analyze_and_format_chat_room_messages(chat_room_messages=chat_room_messages)

    # Create the response structure and return it.
    return {
        "chatRoomId": chat_room_id,
        "chatRoomMessages": chat_room_messages
    }
