import logging
import os
from copy import deepcopy
from itertools import groupby
from psycopg2.extras import RealDictCursor
from functools import wraps
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
POSTGRESQL_USERNAME = os.environ["POSTGRESQL_USERNAME"]
POSTGRESQL_PASSWORD = os.environ["POSTGRESQL_PASSWORD"]
POSTGRESQL_HOST = os.environ["POSTGRESQL_HOST"]
POSTGRESQL_PORT = int(os.environ["POSTGRESQL_PORT"])
POSTGRESQL_DB_NAME = os.environ["POSTGRESQL_DB_NAME"]

# The connection to the database will be created the first time the AWS Lambda function is called.
# Any subsequent call to the function will use the same database connection until the container stops.
POSTGRESQL_CONNECTION = None


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
    required_arguments = ["userId"]
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
            "user_id": input_arguments.get("userId", None)
        }
    })

    # Return nothing.
    return None


def reuse_or_recreate_postgresql_connection(queue: Queue) -> None:
    global POSTGRESQL_CONNECTION
    if not POSTGRESQL_CONNECTION:
        try:
            POSTGRESQL_CONNECTION = databases.create_postgresql_connection(
                POSTGRESQL_USERNAME,
                POSTGRESQL_PASSWORD,
                POSTGRESQL_HOST,
                POSTGRESQL_PORT,
                POSTGRESQL_DB_NAME
            )
        except Exception as error:
            logger.error(error)
            raise Exception("Unable to connect to the PostgreSQL database.")
    queue.put({"postgresql_connection": POSTGRESQL_CONNECTION})
    return None


def postgresql_wrapper(function):
    @wraps(function)
    def wrapper(**kwargs):
        try:
            postgresql_connection = kwargs["postgresql_connection"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)
        cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)
        kwargs["cursor"] = cursor
        result = function(**kwargs)
        cursor.close()
        return result
    return wrapper


@postgresql_wrapper
def get_channels_types_and_channels_data(**kwargs) -> List[Dict[AnyStr, Any]]:
    # Check if the input dictionary has all the necessary keys.
    try:
        cursor = kwargs["cursor"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        sql_arguments = kwargs["sql_arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare an SQL query that returns information about the available channels types and channels.
    sql_statement = """
    select
        channels.channel_id::text,
        channels.channel_name::text,
        channels.channel_description::text,
        channels.channel_technical_id::text,
        channel_types.channel_type_id::text,
        channel_types.channel_type_name::text,
        channel_types.channel_type_description::text
    from
        channels
    left join channel_types on
        channels.channel_type_id = channel_types.channel_type_id
    where
        channels.channel_id in (
            select
                channels_organizations_relationship.channel_id
            from
                channels_organizations_relationship
            left join internal_users on
                channels_organizations_relationship.organization_id = internal_users.organization_id
            left join users on
                internal_users.internal_user_id = users.internal_user_id
            where
                users.user_id = %(user_id)s
            and
                users.internal_user_id is not null
        );
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the list of data about channels types and channels.
    return cursor.fetchall()


def analyze_and_format_channels_types_and_channels_data(**kwargs) -> List[Dict[AnyStr, Any]]:
    # Check if the input dictionary has all the necessary keys.
    try:
        channels_types_and_channels_data = kwargs["channels_types_and_channels_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Check the availability of data.
    if channels_types_and_channels_data:
        # Define the empty list.
        channels_types_and_channels = []

        # Pre-sort the data to group it later.
        data = sorted(channels_types_and_channels_data, key=lambda x: x['channel_type_id'])

        # The "groupby" function makes grouping objects in an iterable a snap.
        grouped_data = groupby(deepcopy(data), key=lambda x: x['channel_type_id'])

        # Define a list of keys that will be deleted later.
        keys_to_delete = ["channel_type_id", "channel_type_name", "channel_type_description"]

        # Store channels grouped by types in a dictionary for quick search and matching.
        storage = {}

        # Clean the dictionaries of certain keys and convert the key names in the dictionaries.
        for key, value in grouped_data:
            cleaned_data = []
            for record in list(value):
                cleaned_record = {}
                [record.pop(item) for item in keys_to_delete]
                for record_key, record_value in record.items():
                    cleaned_record[utils.camel_case(record_key)] = record_value
                cleaned_data.append(cleaned_record)
            storage[key] = cleaned_data

        # Format the channels types and channels data.
        channels_types_ids = []
        for entry in channels_types_and_channels_data:
            channel_type_and_channel = {}
            if entry['channel_type_id'] not in channels_types_ids:
                channels_types_ids.append(entry['channel_type_id'])
                for key, value in entry.items():
                    if key in deleted_keys:
                        channel_type_and_channel[utils.camel_case(key)] = value
                    channel_type_and_channel["channels"] = storage[entry["channel_type_id"]]
                channels_types_and_channels.append(channel_type_and_channel)
    else:
        # Define the empty list.
        channels_types_and_channels = []

    # Return the analyzed and formatted data about channels types and channels.
    return channels_types_and_channels


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
            "function_object": reuse_or_recreate_postgresql_connection,
            "function_arguments": {}
        }
    ])

    # Define the input arguments of the AWS Lambda function.
    input_arguments = results_of_tasks["input_arguments"]
    user_id = input_arguments['user_id']

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]

    # Define the variable which stores information about channels types and channels.
    channels_types_and_channels_data = get_channels_types_and_channels_data(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "user_id": user_id
        }
    )

    # Analyze and format the data obtained earlier from the database.
    channels_types_and_channels = analyze_and_format_channels_types_and_channels_data(
        channels_types_and_channels_data=channels_types_and_channels_data
    )

    # Return the list of data about channels types and channels.
    return channels_types_and_channels
