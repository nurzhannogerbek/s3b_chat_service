import logging
import os
from psycopg2.extras import RealDictCursor
from cassandra.cluster import Session
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
    required_arguments = ["chatRoomId", "clientId"]
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
            "client_id": input_arguments.get("clientId", None)
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


@postgresql_wrapper
def get_aggregated_data(**kwargs) -> None:
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

    # Prepare the SQL query that returns aggregated data about the specific completed chat room.
    sql_statement = """
    select
        channels.channel_id::text,
        channels.channel_name::text,
        channels.channel_description::text,
        channels.channel_technical_id::text,
        channel_types.channel_type_id::text,
        channel_types.channel_type_name::text,
        channel_types.channel_type_description::text,
        users.user_id::text as operator_id,
        array_agg(distinct channels_organizations_relationship.organization_id)::text[] as organizations_ids
    from
        chat_rooms
    left join channels on
        chat_rooms.channel_id = channels.channel_id
    left join channel_types on
        channels.channel_type_id = channel_types.channel_type_id
    left join chat_rooms_users_relationship on
        chat_rooms.chat_room_id = chat_rooms_users_relationship.chat_room_id
    left join users on
        chat_rooms_users_relationship.user_id = users.user_id
    left join channels_organizations_relationship on
        chat_rooms.channel_id = channels_organizations_relationship.channel_id
    where
        chat_rooms.chat_room_id = %(chat_room_id)s
    and
        chat_rooms.chat_room_status = 'completed'
    and
        users.internal_user_id is not null
    and
        users.identified_user_id is null
    and
        users.unidentified_user_id is null
    group by
        channels.channel_id,
        channel_types.channel_type_id,
        users.user_id,
        chat_rooms_users_relationship.entry_created_date_time
    order by
        chat_rooms_users_relationship.entry_created_date_time desc
    limit 1;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Put the result of the function in the queue.
    queue.put({"aggregated_data": cursor.fetchone()})

    # Return nothing.
    return None


@postgresql_wrapper
def get_client_data(**kwargs) -> None:
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

    # Prepare the SQL query that returns the information of the specific client.
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
        users.user_id = %(client_id)s
    limit 1;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Put the result of the function in the queue.
    queue.put({"client_data": cursor.fetchone()})

    # Return nothing.
    return None


def get_last_message_data(**kwargs) -> Dict[AnyStr, Any]:
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

    # Prepare the CQL query that returns information about the latest message data.
    cql_statement = """
    select
        last_message_content,
        last_message_date_time
    from
        completed_chat_rooms
    where
        operator_id = %(operator_id)s
    and
        channel_id = %(channel_id)s
    and
        chat_room_id = %(chat_room_id)s
    limit 1;
    """

    # Execute the CQL query dynamically, in a convenient and safe way.
    try:
        last_message_data = cassandra_connection.execute(cql_statement, cql_arguments).one()
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the information about last message data of the completed chat room.
    return last_message_data


def create_non_accepted_chat_room(**kwargs) -> None:
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
        organizations_ids = cql_arguments["organizations_ids"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare the CQL query that creates a non accepted chat room.
    cql_statement = """
    insert into non_accepted_chat_rooms (
        organization_id,
        channel_id,
        chat_room_id,
        client_id,
        last_message_content,
        last_message_date_time
    ) values (
        %(organization_id)s,
        %(channel_id)s,
        %(chat_room_id)s,
        %(client_id)s,
        %(last_message_content)s,
        %(last_message_date_time)s
    );
    """

    # For each organization that can serve the chat room, we create an entry in the database.
    for organization_id in organizations_ids:
        # Add or update the value of the argument.
        cql_arguments["organization_id"] = uuid.UUID(organization_id)

        # Execute the CQL query dynamically, in a convenient and safe way.
        try:
            cassandra_connection.execute(cql_statement, cql_arguments)
        except Exception as error:
            logger.error(error)
            raise Exception(error)

    # Return nothing.
    return None


def delete_completed_chat_room(**kwargs) -> None:
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

    # Prepare the CQL query that deletes the completed chat room information.
    cql_statement = """
    delete from
        completed_chat_rooms
    where
        operator_id = %(operator_id)s
    and
        channel_id = %(channel_id)s
    and
        chat_room_id = %(chat_room_id)s;
    """

    # Execute the CQL query dynamically, in a convenient and safe way.
    try:
        cassandra_connection.execute(cql_statement, cql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return nothing.
    return None


@postgresql_wrapper
def update_chat_room_status(**kwargs) -> None:
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

    # Prepare the SQL query that updates the status of the specific chat room.
    sql_statement = """
    update
        chat_rooms
    set
        chat_room_status = 'non_accepted'
    where
        chat_room_id = %(chat_room_id)s
    returning
        chat_room_status;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Put the result of the function in the queue.
    queue.put({"chat_room_status": cursor.fetchone()["chat_room_status"]})

    # Return nothing.
    return None


def analyze_and_format_aggregated_data(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        aggregated_data = kwargs["aggregated_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        queue = kwargs["queue"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the aggregated data.
    channel = {}
    channel_type = {}
    if aggregated_data:
        fields = ["channel_type_id", "channel_type_name", "channel_type_description"]
        for key, value in aggregated_data.items():
            if any([field in key for field in fields]):
                channel_type[utils.camel_case(key)] = value
            else:
                channel[utils.camel_case(key)] = value
            channel["channelType"] = channel_type

    # Put the result of the function in the queue.
    queue.put({"channel": channel})

    # Return nothing.
    return None


def analyze_and_format_client_data(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        client_data = kwargs["client_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        queue = kwargs["queue"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the client data.
    client = {}
    if client_data:
        gender = {}
        country = {}
        for key, value in client_data.items():
            if key.startswith("gender_"):
                gender[utils.camel_case(key)] = value
            elif key.startswith("country_"):
                country[utils.camel_case(key)] = value
            else:
                client[utils.camel_case(key)] = value
        client["gender"] = gender
        client["country"] = country

    # Put the result of the function in the queue.
    queue.put({"client": client})

    # Return nothing.
    return None


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
    chat_room_id = input_arguments["chat_room_id"]
    client_id = input_arguments["client_id"]

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]
    cassandra_connection = results_of_tasks["cassandra_connection"]
    set_cassandra_keyspace(cassandra_connection=cassandra_connection)

    # Run several functions in parallel to get all necessary data.
    results_of_tasks = run_multithreading_tasks([
        {
            "function_object": get_aggregated_data,
            "function_arguments": {
                "postgresql_connection": postgresql_connection,
                "sql_arguments": {
                    "chat_room_id": chat_room_id
                }
            }
        },
        {
            "function_object": get_client_data,
            "function_arguments": {
                "postgresql_connection": postgresql_connection,
                "sql_arguments": {
                    "client_id": client_id
                }
            }
        }
    ])

    # Define the variable that stores information about aggregated data.
    aggregated_data = results_of_tasks["aggregated_data"]

    # Return the message to the client that there is no data for the chat room.
    if not aggregated_data:
        raise Exception("The chat room data was not found in the database.")

    # Define the variable that stores information about client data.
    client_data = results_of_tasks["client_data"]

    # Return the message to the client that there is no data for the client.
    if not client_data:
        raise Exception("The client data was not found in the database.")

    # Define a few necessary variables that will be used in the future.
    operator_id = aggregated_data["operator_id"]
    channel_id = aggregated_data["channel_id"]
    organizations_ids = aggregated_data["organizations_ids"]

    # Define the variable that stores information about last message data of the completed chat room.
    last_message_data = get_last_message_data(
        cassandra_connection=cassandra_connection,
        cql_arguments={
            "operator_id": uuid.UUID(operator_id),
            "channel_id": uuid.UUID(channel_id),
            "chat_room_id": uuid.UUID(chat_room_id)
        }
    )

    # Define a few necessary variables that will be used in the future.
    last_message_content = last_message_data.get("last_message_content", None)
    last_message_date_time = last_message_data.get("last_message_date_time", None)

    # Run several functions in parallel to create/update/delete all necessary data in different databases tables.
    results_of_tasks = run_multithreading_tasks([
        {
            "function_object": create_non_accepted_chat_room,
            "function_arguments": {
                "cassandra_connection": cassandra_connection,
                "cql_arguments": {
                    "organizations_ids": organizations_ids,
                    "channel_id": uuid.UUID(channel_id),
                    "chat_room_id": uuid.UUID(chat_room_id),
                    "client_id": uuid.UUID(client_id),
                    "last_message_content": last_message_content,
                    "last_message_date_time": last_message_date_time
                }
            }
        },
        {
            "function_object": delete_completed_chat_room,
            "function_arguments": {
                "cassandra_connection": cassandra_connection,
                "cql_arguments": {
                    "operator_id": uuid.UUID(operator_id),
                    "channel_id": uuid.UUID(channel_id),
                    "chat_room_id": uuid.UUID(chat_room_id)
                }
            }
        },
        {
            "function_object": update_chat_room_status,
            "function_arguments": {
                "postgresql_connection": postgresql_connection,
                "sql_arguments": {
                    "chat_room_id": chat_room_id
                }
            }
        },
        {
            "function_object": analyze_and_format_aggregated_data,
            "function_arguments": {
                "aggregated_data": aggregated_data
            }
        },
        {
            "function_object": analyze_and_format_client_data,
            "function_arguments": {
                "client_data": client_data
            }
        }
    ])

    # Define the variable that stores information about the status of the chat room.
    chat_room_status = results_of_tasks["chat_room_status"]

    # Define variables that store formatted information about the channel and client.
    channel = results_of_tasks["channel"]
    client = results_of_tasks["client"]

    # Create the response structure and return it.
    return {
        "chatRoomId": chat_room_id,
        "chatRoomStatus": chat_room_status,
        "channel": channel,
        "channelId": channel_id,
        "client": client,
        "organizationsIds": organizations_ids
    }
