import logging
import os
from multiprocessing import Process, Pipe
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection
from cassandra.cluster import Session
from functools import wraps
from typing import *
import re
import uuid
import databases
import utils

# Configure the logging tool in the AWS Lambda function.
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# Initialize constants with parameters to configure.
POSTGRESQL_USERNAME = os.environ["POSTGRESQL_USERNAME"]
POSTGRESQL_PASSWORD = os.environ["POSTGRESQL_PASSWORD"]
POSTGRESQL_HOST = os.environ["POSTGRESQL_HOST"]
POSTGRESQL_PORT = os.environ["POSTGRESQL_PORT"]
POSTGRESQL_DB_NAME = os.environ["POSTGRESQL_DB_NAME"]
CASSANDRA_USERNAME = os.environ["CASSANDRA_USERNAME"]
CASSANDRA_PASSWORD = os.environ["CASSANDRA_PASSWORD"]
CASSANDRA_HOST = os.environ["CASSANDRA_HOST"].split(",")
CASSANDRA_PORT = os.environ["CASSANDRA_PORT"]
CASSANDRA_LOCAL_DC = os.environ["CASSANDRA_LOCAL_DC"]
CASSANDRA_KEYSPACE_NAME = os.environ["CASSANDRA_KEYSPACE_NAME"]


def execute_parallel_processes(functions: List[Dict[AnyStr, Union[Callable, Dict[AnyStr, Any]]]]) -> Dict[AnyStr, Any]:
    # Create an empty list to save all parallel processes.
    processes = []

    # Create an empty list of pipes to keep all connections.
    pipes = []

    # Create a process for each function.
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

        # Create communication pipes.
        parent_pipe, child_pipe = Pipe()
        pipes.append(parent_pipe)

        # Add the child pipe to the function arguments.
        function_arguments["pipe"] = child_pipe

        # Create a process.
        process = Process(target=function_object, kwargs=function_arguments)
        processes.append(process)

    # Start all parallel processes.
    for process in processes:
        process.start()

    # Wait until all parallel processes are finished.
    for process in processes:
        process.join()

    # Get the results of all processes.
    results = {}
    for pipe in pipes:
        results = {**results, **pipe.recv()}

    # Return the results of all processes.
    return results


def check_input_arguments(event: Dict[AnyStr, Any]) -> Dict[AnyStr, Any]:
    # Make sure that all the necessary arguments for the AWS Lambda function are present.
    try:
        input_arguments = event["arguments"]["input"]
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
        if re.search("Id$", argument_name):
            try:
                uuid.UUID(argument_value)
            except ValueError:
                raise Exception("The '%s' argument format is not UUID.".format(utils.camel_case(argument_name)))

    # Create the response structure and return it.
    return {
        "chat_room_id": input_arguments.get("chatRoomId", None),
        "client_id": input_arguments.get("clientId", None)
    }


def create_postgresql_connection() -> connection:
    try:
        postgresql_connection = databases.create_postgresql_connection(
            POSTGRESQL_USERNAME,
            POSTGRESQL_PASSWORD,
            POSTGRESQL_HOST,
            POSTGRESQL_PORT,
            POSTGRESQL_DB_NAME
        )
    except Exception as error:
        logger.error(error)
        raise Exception("Unable to connect to the PostgreSQL database.")
    return postgresql_connection


def create_cassandra_connection() -> Session:
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
        raise Exception("Unable to connect to the Cassandra database.")
    return cassandra_connection


def postgresql_wrapper(function):
    @wraps(function)
    def wrapper(**kwargs):
        postgresql_connection = create_postgresql_connection()
        cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)
        kwargs["cursor"] = cursor
        result = function(**kwargs)
        cursor.close()
        postgresql_connection.close()
        return result
    return wrapper


def cassandra_wrapper(function):
    @wraps(function)
    def wrapper(**kwargs):
        cassandra_connection = create_cassandra_connection()
        kwargs["cassandra_connection"] = cassandra_connection
        result = function(**kwargs)
        cassandra_connection.shutdown()
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
        pipe = kwargs["pipe"]
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

    # Send data to the pipe and then close it.
    pipe.send({"aggregated_data": cursor.fetchone()})
    pipe.close()


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
        pipe = kwargs["pipe"]
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

    # Send data to the pipe and then close it.
    pipe.send({"client_data": cursor.fetchone()})
    pipe.close()


@cassandra_wrapper
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


@cassandra_wrapper
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
    try:
        pipe = kwargs["pipe"]
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

    # Send data to the pipe and then close it.
    pipe.send({})
    pipe.close()


@cassandra_wrapper
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
    try:
        pipe = kwargs["pipe"]
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

    # Send data to the pipe and then close it.
    pipe.send({})
    pipe.close()


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
        pipe = kwargs["pipe"]
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

    # Send data to the pipe and then close it.
    pipe.send({"chat_room_status": cursor.fetchone()["chat_room_status"]})
    pipe.close()


def analyze_and_format_aggregated_data(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        aggregated_data = kwargs["aggregated_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        pipe = kwargs["pipe"]
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

    # Send data to the pipe and then close it.
    pipe.send({"channel": channel})
    pipe.close()


def analyze_and_format_client_data(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        client_data = kwargs["client_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        pipe = kwargs["pipe"]
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

    # Send data to the pipe and then close it.
    pipe.send({"client": client})
    pipe.close()


def lambda_handler(event, context):
    """
    :param event: The AWS Lambda function uses this parameter to pass in event data to the handler.
    :param context: The AWS Lambda function uses this parameter to provide runtime information to your handler.
    """
    # First check and then define the input arguments of the AWS Lambda function.
    input_arguments = check_input_arguments(event=event)
    chat_room_id = input_arguments["chat_room_id"]
    client_id = input_arguments["client_id"]

    # Run several functions in parallel processes to get all the necessary data from different database tables.
    results_of_processes = execute_parallel_processes([
        {
            "function_object": get_aggregated_data,
            "function_arguments": {
                "sql_arguments": {
                    "chat_room_id": chat_room_id
                }
            }
        },
        {
            "function_object": get_client_data,
            "function_arguments": {
                "sql_arguments": {
                    "client_id": client_id
                }
            }
        }
    ])

    # Define the variable that stores information about aggregated data.
    aggregated_data = results_of_processes["aggregated_data"]

    # Return the message to the client that there is no data for the chat room.
    if not aggregated_data:
        raise Exception("The chat room data was not found in the database.")

    # Define the variable that stores information about client data.
    client_data = results_of_processes["client_data"]

    # Return the message to the client that there is no data for the client.
    if not client_data:
        raise Exception("The client data was not found in the database.")

    # Define a few necessary variables that will be used in the future.
    operator_id = aggregated_data["operator_id"]
    channel_id = aggregated_data["channel_id"]
    organizations_ids = aggregated_data["organizations_ids"]

    # Define the variable that stores information about last message data of the completed chat room.
    last_message_data = get_last_message_data(
        cql_arguments={
            "operator_id": uuid.UUID(operator_id),
            "channel_id": uuid.UUID(channel_id),
            "chat_room_id": uuid.UUID(chat_room_id)
        }
    )

    # Define a few necessary variables that will be used in the future.
    last_message_content = last_message_data.get("last_message_content", None)
    last_message_date_time = last_message_data.get("last_message_date_time", None)

    # Run several related functions to update/delete all necessary data in different databases tables.
    results_of_processes = execute_parallel_processes([
        {
            "function_object": create_non_accepted_chat_room,
            "function_arguments": {
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
                "sql_arguments": {
                    "chat_room_id": chat_room_id
                }
            }
        }
    ])

    # Define a variable that stores information about the status of the chat room.
    chat_room_status = results_of_processes["chat_room_status"]

    # Run several functions in parallel processes to analyze and format all necessary data.
    results_of_processes = execute_parallel_processes([
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

    # Define variables that store formatted information about the channel and the client.
    channel = results_of_processes["channel"]
    client = results_of_processes["client"]

    # Create the response structure and return it.
    return {
        "chatRoomId": chat_room_id,
        "chatRoomStatus": chat_room_status,
        "channel": channel,
        "channelId": channel_id,
        "client": client,
        "organizationsIds": organizations_ids
    }
