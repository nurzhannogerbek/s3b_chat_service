import logging
import os
from multiprocessing import Process, Pipe
from psycopg2.extras import RealDictCursor
from functools import wraps
from typing import *
import databases
import utils

# Configure the logging tool in the AWS Lambda function.
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# Initialize global variables with parameters for settings.
CASSANDRA_USERNAME = os.environ["CASSANDRA_USERNAME"]
CASSANDRA_PASSWORD = os.environ["CASSANDRA_PASSWORD"]
CASSANDRA_HOST = os.environ["CASSANDRA_HOST"].split(",")
CASSANDRA_PORT = os.environ["CASSANDRA_PORT"]
CASSANDRA_LOCAL_DC = os.environ["CASSANDRA_LOCAL_DC"]
CASSANDRA_KEYSPACE_NAME = os.environ["CASSANDRA_KEYSPACE_NAME"]
POSTGRESQL_USERNAME = os.environ["POSTGRESQL_USERNAME"]
POSTGRESQL_PASSWORD = os.environ["POSTGRESQL_PASSWORD"]
POSTGRESQL_HOST = os.environ["POSTGRESQL_HOST"]
POSTGRESQL_PORT = os.environ["POSTGRESQL_PORT"]
POSTGRESQL_DB_NAME = os.environ["POSTGRESQL_DB_NAME"]

# The connection to the database will be created the first time the AWS Lambda function is called.
# Any subsequent call to the function will use the same database connection until the container stops.
CASSANDRA_CONNECTION = None
POSTGRESQL_CONNECTION = None


def check_input_arguments(**kwargs) -> None:
    # Check whether the required input arguments of the AWS Lambda function have keys in their dictionaries.
    try:
        pipe = kwargs["pipe"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        event = kwargs["event"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        arguments = event["arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        input = arguments["input"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        chat_room_id = input["chatRoomId"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        client_id = input["clientId"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Send data to the pipe and then close it.
    pipe.send({
        "input_arguments": {
            "chat_room_id": chat_room_id,
            "client_id": client_id
        }
    })
    pipe.close()


def reuse_or_recreate_cassandra_connection(**kwargs) -> None:
    # Check whether the input arguments have keys in their dictionaries.
    try:
        pipe = kwargs["pipe"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Reuse or recreate Cassandra connection.
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

    # Send data to the pipe and then close it.
    pipe.send({
        "cassandra_connection": CASSANDRA_CONNECTION
    })
    pipe.close()


def reuse_or_recreate_postgresql_connection(**kwargs) -> None:
    # Check whether the input arguments have keys in their dictionaries.
    try:
        pipe = kwargs["pipe"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Reuse or recreate PostgreSQL connection.
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

    # Send data to the pipe and then close it.
    pipe.send({
        "postgresql_connection": POSTGRESQL_CONNECTION
    })
    pipe.close()


def execute_parallel_processes(functions: List[Dict[AnyStr, Union[Callable, Dict[AnyStr, Any]]]]) -> Dict[AnyStr, Any]:
    # Create an empty list to keep all parallel processes.
    processes = []

    # Create an empty list of pipes to keep all connections.
    pipes = []

    # Create a process per function.
    for index, function in enumerate(functions):
        # Check whether the input arguments have keys in their dictionaries.
        try:
            target = function["function_object"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)
        try:
            kwargs = function["function_arguments"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)

        # Create a pipe for communication.
        parent_pipe, child_pipe = Pipe()
        pipes.append(parent_pipe)

        # Add the child pipe the dictionary with arguments.
        kwargs["pipe"] = child_pipe

        # Create a process.
        process = Process(target=target, kwargs=kwargs)
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


def psycopg2_cursor(function):
    @wraps(function)
    def wrapper(**kwargs):
        try:
            postgresql_connection = kwargs["postgresql_connection"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)
        cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)
        result = function(cursor=cursor, **kwargs)
        cursor.close()
        return result

    return wrapper


@psycopg2_cursor
def get_aggregated_data(**kwargs) -> None:
    # Check whether the input arguments have keys in their dictionaries.
    try:
        cursor = kwargs["cursor"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        arguments = kwargs["sql_arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        pipe = kwargs["pipe"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare an sql query that returns aggregated data about a completed chat room.
    statement = """
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
        cursor.execute(statement, arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Send data to the pipe and then close it.
    pipe.send({
        "aggregated_data": cursor.fetchone()
    })
    pipe.close()


@psycopg2_cursor
def get_client_data(**kwargs) -> None:
    # Check whether the input arguments have keys in their dictionaries.
    try:
        cursor = kwargs["cursor"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        arguments = kwargs["sql_arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        pipe = kwargs["pipe"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare the SQL query that returns the information of the specific client.
    statement = """
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
        cursor.execute(statement, arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Send data to the pipe and then close it.
    pipe.send({
        "client_data": cursor.fetchone()
    })
    pipe.close()


def get_last_message_data(**kwargs) -> Dict[AnyStr, Any]:
    # Check whether the input arguments have keys in their dictionaries.
    try:
        cassandra_connection = kwargs["cassandra_connection"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        arguments = kwargs["cql_arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare an CQL query that returns information about the latest message data.
    statement = """
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
        last_message_data = cassandra_connection.execute(statement, arguments).one()
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the information about last message data of the completed chat room.
    return {
        "last_message_data": last_message_data
    }


def create_non_accepted_chat_room(**kwargs) -> None:
    # Check whether the input arguments have keys in their dictionaries.
    try:
        cassandra_connection = kwargs["cassandra_connection"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        arguments = kwargs["cql_arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        organizations_ids = arguments["organizations_ids"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        pipe = kwargs["pipe"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # For each organization that can serve the chat room, we create an entry in the database.
    for organization_id in organizations_ids:
        # Prepare an CQL query that creates a non accepted chat room.
        statement = """
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

        # Add or update the value of the argument.
        arguments["organization_id"] = organization_id

        # Execute the CQL query dynamically, in a convenient and safe way.
        try:
            cassandra_connection.execute(statement, arguments)
        except Exception as error:
            logger.error(error)
            raise Exception(error)

    # Send data to the pipe and then close it.
    pipe.send({})
    pipe.close()


@psycopg2_cursor
def update_chat_room_status(**kwargs) -> None:
    # Check whether the input arguments have keys in their dictionaries.
    try:
        cursor = kwargs["cursor"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        arguments = kwargs["sql_arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        pipe = kwargs["pipe"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare an SQL query that updates the status of the chat room.
    statement = """
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
        cursor.execute(statement, arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Send data to the pipe and then close it.
    pipe.send({
        "chat_room_status": cursor.fetchone()["chat_room_status"]
    })
    pipe.close()


def delete_completed_chat_room(**kwargs) -> None:
    # Check whether the input arguments have keys in their dictionaries.
    try:
        cassandra_connection = kwargs["cassandra_connection"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        arguments = kwargs["cql_arguments"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        pipe = kwargs["pipe"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare an CQL query that deletes the completed chat room information.
    statement = """
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
        cassandra_connection.execute(statement, arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Send data to the pipe and then close it.
    pipe.send({})
    pipe.close()


def analyze_and_format_data(**kwargs) -> None:
    # Check whether the input arguments have keys in their dictionaries.
    try:
        aggregated_data = kwargs["aggregated_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
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

    # Format the aggregated data.
    channel = {}
    channel_type = {}
    if not aggregated_data:
        for key, value in aggregated_data.items():
            if any([i in key for i in ["channel_type_id", "channel_type_name", "channel_type_description"]]):
                channel_type[utils.camel_case(key)] = value
            else:
                channel[utils.camel_case(key)] = value
            channel["channelType"] = channel_type

    # Format the client data.
    client = {}
    if not client_data:
        gender = {}
        country = {}
        for key, value in client_data.items():
            if "gender_" in key:
                gender[utils.camel_case(key)] = value
            elif "country_" in key:
                country[utils.camel_case(key)] = value
            else:
                client[utils.camel_case(key)] = value
        client["gender"] = gender
        client["country"] = country

    # Send data to the pipe and then close it.
    pipe.send({
        "channel": channel,
        "client": client
    })
    pipe.close()


def lambda_handler(event, context):
    """
    :param event: The AWS Lambda function uses this parameter to pass in event data to the handler.
    :param context: The AWS Lambda function uses this parameter to provide runtime information to your handler.
    """
    # Run several functions related to initialization processes in parallel.
    results_of_processes = execute_parallel_processes([
        {
            "function_object": check_input_arguments,
            "function_arguments": {
                "event": event
            }
        },
        {
            "function_object": reuse_or_recreate_cassandra_connection,
            "function_arguments": {}
        },
        {
            "function_object": reuse_or_recreate_postgresql_connection,
            "function_arguments": {}
        }
    ])

    # Define the instances of the database connections.
    cassandra_connection = results_of_processes["cassandra_connection"]
    postgresql_connection = results_of_processes["postgresql_connection"]

    # Define the input arguments of the AWS Lambda function.
    input_arguments = results_of_processes["input_arguments"]
    chat_room_id = input_arguments["chat_room_id"]
    client_id = input_arguments["client_id"]

    # Run several functions in parallel to get all necessary data from different databases tables.
    results_of_processes = execute_parallel_processes([
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

    # Define a variable that stores information about aggregated data.
    aggregated_data = results_of_processes["aggregated_data"]

    # Return a message to the client that there is no data for the chat room.
    if aggregated_data:
        raise Exception("The data is not found in the database. Check the chat room id.")

    # Define a few necessary variables that will be used in the future.
    operator_id = aggregated_data["operator_id"]
    channel_id = aggregated_data["channel_id"]
    organizations_ids = aggregated_data["organizations_ids"]

    # Define a variable that stores information about client data.
    client_data = results_of_processes["client_data"]

    # Return a message to the client that there is no data for the client.
    if client_data:
        raise Exception("The data is not found in the database. Check the client id.")

    # This peace of code fix ERROR NoHostAvailable: ("Unable to complete the operation against any hosts").
    success = False
    while not success:
        try:
            cassandra_connection.set_keyspace(CASSANDRA_KEYSPACE_NAME)
            success = True
        except Exception as error:
            logger.warning(error)
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

    # Define a variable that stores information about last message data of the completed chat room.
    last_message_data = get_last_message_data(
        cassandra_connection=cassandra_connection,
        cql_arguments={
            "operator_id": operator_id,
            "channel_id": channel_id,
            "chat_room_id": chat_room_id
        }
    )

    # Define a few necessary variables that will be used in the future.
    last_message_content = last_message_data.get("last_message_content", None)
    last_message_date_time = last_message_data.get("last_message_date_time", None)

    # Run several functions in parallel to update all necessary data in different databases tables.
    results_of_processes = execute_parallel_processes([
        {
            "function_object": create_non_accepted_chat_room,
            "function_arguments": {
                "cassandra_connection": cassandra_connection,
                "cql_arguments": {
                    "organizations_ids": organizations_ids,
                    "channel_id": channel_id,
                    "chat_room_id": chat_room_id,
                    "client_id": client_id,
                    "last_message_content": last_message_content,
                    "last_message_date_time": last_message_date_time
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
            "function_object": delete_completed_chat_room,
            "function_arguments": {
                "cassandra_connection": cassandra_connection,
                "cql_arguments": {
                    "operator_id": operator_id,
                    "channel_id": channel_id,
                    "chat_room_id": chat_room_id
                }
            }
        },
        {
            "function_object": analyze_and_format_data,
            "function_arguments": {
                "aggregated_data": aggregated_data,
                "client_data": client_data
            }
        }
    ])

    # Define a variable that stores information about the status of the chat room.
    chat_room_status = results_of_processes["chat_room_status"]

    # Define variables that store formatted information about the channel and client.
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
