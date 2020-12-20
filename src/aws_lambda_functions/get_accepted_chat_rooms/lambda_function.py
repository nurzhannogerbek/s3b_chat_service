import logging
import os
from psycopg2.extras import RealDictCursor
from cassandra.cluster import Session
from functools import wraps
from typing import *
import uuid
from threading import Thread
from queue import Queue
from datetime import datetime
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
CASSANDRA_USERNAME = os.environ["CASSANDRA_USERNAME"]
CASSANDRA_PASSWORD = os.environ["CASSANDRA_PASSWORD"]
CASSANDRA_HOST = os.environ["CASSANDRA_HOST"].split(",")
CASSANDRA_PORT = int(os.environ["CASSANDRA_PORT"])
CASSANDRA_LOCAL_DC = os.environ["CASSANDRA_LOCAL_DC"]
CASSANDRA_KEYSPACE_NAME = os.environ["CASSANDRA_KEYSPACE_NAME"]

# The connection to the database will be created the first time the AWS Lambda function is called.
# Any subsequent call to the function will use the same database connection until the container stops.
POSTGRESQL_CONNECTION = None
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
    required_arguments = ["operatorId", "channelsIds"]
    for argument_name, argument_value in input_arguments.items():
        if argument_name in required_arguments and argument_value is None:
            raise Exception("The '%s' argument can't be None/Null/Undefined.".format(utils.camel_case(argument_name)))
        if argument_name.endswith("Id"):
            try:
                uuid.UUID(argument_value)
            except ValueError:
                raise Exception("The '%s' argument format is not UUID.".format(utils.camel_case(argument_name)))
        elif argument_name.endswith("DateTime") and argument_value:
            try:
                datetime.fromisoformat(argument_value)
            except ValueError:
                raise Exception("The '%s' argument format is not ISO.".format(utils.camel_case(argument_name)))

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "operator_id": input_arguments.get("operatorId", None),
            "channels_ids": input_arguments.get("channelsIds", None),
            "start_date_time": input_arguments.get("startDateTime", None),
            "end_date_time": input_arguments.get("endDateTime", None)
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
        except Exception as warning:
            logger.warning(warning)
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


def get_accepted_chat_rooms_data(**kwargs) -> List[Dict[AnyStr, Any]]:
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
        channels_ids = cql_arguments["channels_ids"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        start_date_time = cql_arguments["start_date_time"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        end_date_time = cql_arguments["end_date_time"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare the CQL query that gets a list of accepted chat rooms for the specific channel.
    if all(argument is not None for argument in [start_date_time, end_date_time]):
        cql_statement = """
        select
            channel_id,
            chat_room_id,
            client_id,
            last_message_content,
            last_message_date_time,
            unread_messages_number
        from
            accepted_chat_rooms
        where
            operator_id = %(operator_id)s
        and
            channel_id = %(channel_id)s
        and
            chat_room_id > maxTimeuuid(%(start_date_time)s)
        and
            chat_room_id < minTimeuuid(%(end_date_time)s);
        """
    else:
        cql_statement = """
        select
            channel_id,
            chat_room_id,
            client_id,
            last_message_content,
            last_message_date_time,
            unread_messages_number
        from
            accepted_chat_rooms
        where
            operator_id = %(operator_id)s
        and
            channel_id = %(channel_id)s;
        """

    # Define the empty list to store information about accepted chat rooms from all channels.
    accepted_chat_rooms_data = []

    # For each channel, get all accepted chat rooms from the database.
    for channel_id in channels_ids:
        # Add or update the value of the argument.
        cql_arguments["channel_id"] = uuid.UUID(channel_id)

        # Execute the CQL query dynamically, in a convenient and safe way.
        try:
            accepted_chat_rooms = cassandra_connection.execute(cql_statement, cql_arguments)
        except Exception as error:
            logger.error(error)
            raise Exception(error)

        # Add the results to the general list.
        accepted_chat_rooms_data.extend(accepted_chat_rooms)

    # Return the list of accepted chat rooms from all channels.
    return accepted_chat_rooms_data


@postgresql_wrapper
def get_clients_data(**kwargs) -> None:
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
    try:
        queue = kwargs["queue"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare the SQL query that returns the information of clients.
    sql_statement = """
    select
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then 'identified_user'::text
            else 'unidentified_user'::text
        end as user_type,
        users.user_id::text,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_first_name::text
            else null
        end as user_first_name,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_last_name::text
            else null
        end as user_last_name,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_middle_name::text
            else null
        end as user_middle_name,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_primary_email::text
            else null
        end as user_primary_email,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_secondary_email::text
            else null
        end as user_secondary_email,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_primary_phone_number::text
            else null
        end as user_primary_phone_number,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_secondary_phone_number::text
            else null
        end as user_secondary_phone_number,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_profile_photo_url::text
            else null
        end as user_profile_photo_url,
        genders.gender_id::text,
        genders.gender_technical_name::text,
        genders.gender_public_name::text,
        countries.country_id::text,
        countries.country_short_name::text,
        countries.country_official_name::text,
        countries.country_alpha_2_code::text,
        countries.country_alpha_3_code::text,
        countries.country_numeric_code::text,
        countries.country_code_top_level_domain::text,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.metadata::text
            else unidentified_users.metadata::text
        end as metadata,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.telegram_username::text
            else null
        end as telegram_username,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.whatsapp_profile::text
            else null
        end as whatsapp_profile,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.whatsapp_username::text
            else null
        end as whatsapp_username
    from
        users
    left join identified_users on
        users.identified_user_id = identified_users.identified_user_id
    left join unidentified_users on
        users.unidentified_user_id = unidentified_users.unidentified_user_id
    left join genders on
        identified_users.gender_id = genders.gender_id
    left join countries on
        identified_users.country_id = countries.country_id
    where
        users.user_id in %(clients_ids)s;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Put the result of the function in the queue.
    queue.put({"clients_data": cursor.fetchall()})

    # Return nothing.
    return None


@postgresql_wrapper
def get_channels_data(**kwargs) -> None:
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
    try:
        queue = kwargs["queue"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare the SQL query which returns information about the channels.
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
        channels.channel_id in %(channels_ids)s;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Put the result of the function in the queue.
    queue.put({"channels_data": cursor.fetchall()})

    # Return nothing.
    return None


def analyze_and_format_clients_data(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        clients_data = kwargs["clients_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        queue = kwargs["queue"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the clients data.
    clients_storage = {}
    for client_data in clients_data:
        client, gender, country = {}, {}, {}
        for key, value in client_data.items():
            if key.startswith("gender_"):
                gender[utils.camel_case(key)] = value
            elif key.startswith("country_"):
                country[utils.camel_case(key)] = value
            else:
                client[utils.camel_case(key)] = value
        client["gender"] = gender
        client["country"] = country
        clients_storage[client_data["user_id"]] = client

    # Put the result of the function in the queue.
    queue.put({"clients_storage": clients_storage})

    # Return nothing.
    return None


def analyze_and_format_channels_data(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        channels_data = kwargs["channels_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        queue = kwargs["queue"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the channels data.
    channels_storage = {}
    for channel_data in channels_data:
        channel, channel_type = {}, {}
        for key, value in channel_data.items():
            if any([item in key for item in ["channel_type_id", "channel_type_name", "channel_type_description"]]):
                channel_type[utils.camel_case(key)] = value
            else:
                channel[utils.camel_case(key)] = value
        channel["channelType"] = channel_type
        channels_storage[channel_data["channel_id"]] = channel

    # Put the result of the function in the queue.
    queue.put({"channels_storage": channels_storage})

    # Return nothing.
    return None


def analyze_and_format_accepted_chat_rooms_data(**kwargs) -> List[Dict[AnyStr, Any]]:
    # Check if the input dictionary has all the necessary keys.
    try:
        accepted_chat_rooms_data = kwargs["accepted_chat_rooms_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        clients_storage = kwargs["clients_storage"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        channels_storage = kwargs["channels_storage"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the accepted chat rooms data.
    accepted_chat_rooms = []
    for accepted_chat_room_data in accepted_chat_rooms_data:
        accepted_chat_room = {}
        for key, value in accepted_chat_room_data.items():
            if key.endswith("_date_time") and value is not None:
                value = value.isoformat()
            elif key.endswith("_id") and value is not None:
                value = str(value)
            if key.startswith("client_id"):
                accepted_chat_room["client"] = clients_storage[value]
            elif key.startswith("channel_id"):
                accepted_chat_room["channel"] = channels_storage[value]
            else:
                accepted_chat_room[utils.camel_case(key)] = value
        accepted_chat_rooms.append(accepted_chat_room)

    # Return analyzed and formatted data.
    return accepted_chat_rooms


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
        },
        {
            "function_object": reuse_or_recreate_cassandra_connection,
            "function_arguments": {}
        }
    ])

    # Define the input arguments of the AWS Lambda function.
    input_arguments = results_of_tasks["input_arguments"]
    operator_id = uuid.UUID(input_arguments["operator_id"])
    channels_ids = input_arguments["channels_ids"]
    start_date_time = input_arguments["start_date_time"]
    start_date_time = datetime.fromisoformat(start_date_time) if start_date_time else None
    end_date_time = input_arguments["end_date_time"]
    end_date_time = datetime.fromisoformat(end_date_time) if end_date_time else None

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]
    cassandra_connection = results_of_tasks["cassandra_connection"]
    set_cassandra_keyspace(cassandra_connection=cassandra_connection)

    # Get all accepted chat rooms from different channels.
    accepted_chat_rooms_data = get_accepted_chat_rooms_data(
        cassandra_connection=cassandra_connection,
        cql_arguments={
            "operator_id": operator_id,
            "channels_ids": channels_ids,
            "start_date_time": start_date_time,
            "end_date_time": end_date_time,
        }
    )

    # Check for data in the list that we received from the database.
    if accepted_chat_rooms_data:
        # Create the list of IDs for all clients.
        clients_ids = [str(item["client_id"]) for item in accepted_chat_rooms_data]

        # Run several functions in parallel to get all the necessary data from different database tables.
        results_of_tasks = run_multithreading_tasks([
            {
                "function_object": get_clients_data,
                "function_arguments": {
                    "postgresql_connection": postgresql_connection,
                    "sql_arguments": {
                        "clients_ids": tuple(clients_ids)
                    }
                }
            },
            {
                "function_object": get_channels_data,
                "function_arguments": {
                    "postgresql_connection": postgresql_connection,
                    "sql_arguments": {
                        "channels_ids": tuple(channels_ids)
                    }
                }
            }
        ])

        # Define a few necessary variables that will be used in the future.
        clients_data = results_of_tasks["clients_data"]
        channels_data = results_of_tasks["channels_data"]

        # Run several functions in parallel to analyze and format previously received data.
        results_of_tasks = run_multithreading_tasks([
            {
                "function_object": analyze_and_format_clients_data,
                "function_arguments": {
                    "clients_data": clients_data
                }
            },
            {
                "function_object": analyze_and_format_channels_data,
                "function_arguments": {
                    "channels_data": channels_data
                }
            }
        ])

        # Define a few necessary variables that will be used in the future.
        clients_storage = results_of_tasks["clients_storage"]
        channels_storage = results_of_tasks["channels_storage"]

        # Define the variable that stores information about all accepted chat rooms from different channels.
        accepted_chat_rooms = analyze_and_format_accepted_chat_rooms_data(
            accepted_chat_rooms_data=accepted_chat_rooms_data,
            clients_storage=clients_storage,
            channels_storage=channels_storage
        )
    else:
        accepted_chat_rooms = []

    # Return the list of all accepted chat rooms from different channels.
    return accepted_chat_rooms
