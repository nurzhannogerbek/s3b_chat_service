import logging
import os
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

# Define the global variable.
chat_room = {}


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
    required_arguments = ["chatRoomId"]
    for argument_name, argument_value in input_arguments.items():
        if argument_name in required_arguments and argument_value is None:
            raise Exception("The '{0}' argument can't be None/Null/Undefined.".format(utils.camel_case(argument_name)))
        if argument_name.endswith("Id"):
            try:
                uuid.UUID(argument_value)
            except ValueError:
                raise Exception("The '{0}' argument format is not UUID.".format(utils.camel_case(argument_name)))

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "chat_room_id": input_arguments.get("chatRoomId", None)
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

    # Prepare the SQL request that returns aggregated data.
    sql_statement = """
    select
        chat_rooms.chat_room_id::text,
        chat_rooms.chat_room_status::text,
        channels.channel_id::text,
        channels.channel_name::text,
        channels.channel_description::text,
        channels.channel_technical_id::text,
        channel_types.channel_type_id::text,
        channel_types.channel_type_name::text,
        channel_types.channel_type_description::text
    from
        chat_rooms
    left join channels on
        chat_rooms.channel_id = channels.channel_id
    left join channel_types on
        channels.channel_type_id = channel_types.channel_type_id
    where
        chat_room_id = %(chat_room_id)s
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

    # Prepare the SQL query that returns the information of a client.
    sql_statement = """
    select
        chat_rooms_users_relationship.entry_created_date_time as chat_room_member_since_date_time,
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
        chat_rooms_users_relationship
    left join users on
        chat_rooms_users_relationship.user_id = users.user_id
    left join identified_users on
        users.identified_user_id = identified_users.identified_user_id
    left join unidentified_users on
        users.unidentified_user_id = unidentified_users.unidentified_user_id
    left join genders on
        identified_users.gender_id = genders.gender_id
    left join countries on
        identified_users.country_id = countries.country_id
    where
        chat_rooms_users_relationship.chat_room_id = %(chat_room_id)s
    and
        (
            users.internal_user_id is null and users.identified_user_id is not null
            or
            users.internal_user_id is null and users.unidentified_user_id is not null
        )
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


@postgresql_wrapper
def get_operators_data(**kwargs) -> None:
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

    # Prepare the SQL query that returns the information of operators.
    sql_statement = """
    select
        distinct on (aggregated_data.user_id) user_id,
        aggregated_data.chat_room_member_since_date_time,
        aggregated_data.auth0_user_id,
        aggregated_data.auth0_metadata,
        aggregated_data.user_first_name,
        aggregated_data.user_last_name,
        aggregated_data.user_middle_name,
        aggregated_data.user_primary_email,
        aggregated_data.user_secondary_email,
        aggregated_data.user_primary_phone_number,
        aggregated_data.user_secondary_phone_number,
        aggregated_data.user_profile_photo_url,
        aggregated_data.user_position_name,
        aggregated_data.gender_id,
        aggregated_data.gender_technical_name,
        aggregated_data.gender_public_name,
        aggregated_data.country_id,
        aggregated_data.country_short_name,
        aggregated_data.country_official_name,
        aggregated_data.country_alpha_2_code,
        aggregated_data.country_alpha_3_code,
        aggregated_data.country_numeric_code,
        aggregated_data.country_code_top_level_domain,
        aggregated_data.role_id,
        aggregated_data.role_technical_name,
        aggregated_data.role_public_name,
        aggregated_data.role_description,
        aggregated_data.organization_id,
        aggregated_data.organization_name,
        aggregated_data.organization_description,
        aggregated_data.parent_organization_id,
        aggregated_data.parent_organization_name,
        aggregated_data.parent_organization_description,
        aggregated_data.root_organization_id,
        aggregated_data.root_organization_name,
        aggregated_data.root_organization_description
    from (
        select
            users.user_id::text user_id,
            chat_rooms_users_relationship.entry_created_date_time as chat_room_member_since_date_time,
            internal_users.auth0_user_id::text,
            internal_users.auth0_metadata::text,
            internal_users.internal_user_first_name::text as user_first_name,
            internal_users.internal_user_last_name::text as user_last_name,
            internal_users.internal_user_middle_name::text as user_middle_name,
            internal_users.internal_user_primary_email::text as user_primary_email,
            internal_users.internal_user_secondary_email::text as user_secondary_email,
            internal_users.internal_user_primary_phone_number::text as user_primary_phone_number,
            internal_users.internal_user_secondary_phone_number::text as user_secondary_phone_number,
            internal_users.internal_user_profile_photo_url::text as user_profile_photo_url,
            internal_users.internal_user_position_name::text as user_position_name,
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
            roles.role_id::text,
            roles.role_technical_name::text,
            roles.role_public_name::text,
            roles.role_description::text,
            organizations.organization_id::text,
            organizations.organization_name::text,
            organizations.organization_description::text,
            organizations.parent_organization_id::text,
            organizations.parent_organization_name::text,
            organizations.parent_organization_description::text,
            organizations.root_organization_id::text,
            organizations.root_organization_name::text,
            organizations.root_organization_description::text
        from
            chat_rooms_users_relationship
        left join users on
            chat_rooms_users_relationship.user_id = users.user_id
        left join internal_users on
            users.internal_user_id = internal_users.internal_user_id
        left join genders on
            internal_users.gender_id = genders.gender_id
        left join countries on
            internal_users.country_id = countries.country_id
        left join roles on
            internal_users.role_id = roles.role_id
        left join organizations on
            internal_users.organization_id = organizations.organization_id
        where
            chat_rooms_users_relationship.chat_room_id = %(chat_room_id)s
        and
            users.internal_user_id is not null
        and
            users.identified_user_id is null
        and
            users.unidentified_user_id is null
        order by
            chat_rooms_users_relationship.entry_created_date_time desc
    ) aggregated_data;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Put the result of the function in the queue.
    queue.put({"operators_data": cursor.fetchall()})

    # Return nothing.
    return None


def analyze_and_format_aggregated_data(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        aggregated_data = kwargs["aggregated_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the aggregated data.
    channel, channel_type = {}, {}
    for key, value in aggregated_data.items():
        if any([i in key for i in ["channel_id", "channel_name", "channel_description", "channel_technical_id"]]):
            channel[utils.camel_case(key)] = value
        elif any([i in key for i in ["channel_type_id", "channel_type_name", "channel_type_description"]]):
            channel_type[utils.camel_case(key)] = value
        else:
            chat_room[utils.camel_case(key)] = value
    channel["channelType"] = channel_type
    chat_room["channel"] = channel

    # Return nothing.
    return None


def analyze_and_format_operators_data(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        operators_data = kwargs["operators_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the operators data.
    operators = []
    if operators_data:
        for entry in operators_data:
            operator, gender, country, role, organization = {}, {}, {}, {}, {}
            for key, value in entry.items():
                if key.endswith("_date_time"):
                    value = value.isoformat()
                if key.startswith("gender_"):
                    gender[utils.camel_case(key)] = value
                elif key.startswith("country_"):
                    country[utils.camel_case(key)] = value
                elif key.startswith("role_"):
                    role[utils.camel_case(key)] = value
                elif key.startswith("organization_"):
                    organization[utils.camel_case(key)] = value
                else:
                    operator[utils.camel_case(key)] = value
            operator["gender"] = gender
            operator["country"] = country
            operator["role"] = role
            operator["organization"] = organization
            operators.append(operator)
        chat_room["chatRoomOperators"] = operators

    # Return nothing.
    return None


def analyze_and_format_client_data(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    try:
        client_data = kwargs["client_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the client data.
    client = {}
    if client_data:
        gender, country = {}, {}
        for key, value in client_data.items():
            if key.endswith("_date_time"):
                value = value.isoformat()
            if key.startswith("gender_"):
                gender[utils.camel_case(key)] = value
            elif key.startswith("country_"):
                country[utils.camel_case(key)] = value
            else:
                client[utils.camel_case(key)] = value
        client["gender"] = gender
        client["country"] = country
        chat_room["chatRoomClient"] = client

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
        }
    ])

    # Define the input arguments of the AWS Lambda function.
    input_arguments = results_of_tasks["input_arguments"]
    chat_room_id = input_arguments['chat_room_id']

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]

    # Run several functions in parallel to get all the necessary data from different database tables.
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
                    "chat_room_id": chat_room_id
                }
            }
        },
        {
            "function_object": get_operators_data,
            "function_arguments": {
                "postgresql_connection": postgresql_connection,
                "sql_arguments": {
                    "chat_room_id": chat_room_id
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

    # Define the variable that stores information about operators data.
    operators_data = results_of_tasks["operators_data"]

    # Run several functions in parallel.
    run_multithreading_tasks([
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
        },
        {
            "function_object": analyze_and_format_operators_data,
            "function_arguments": {
                "operators_data": operators_data
            }
        }
    ])

    # Return information about the chat room.
    return chat_room
