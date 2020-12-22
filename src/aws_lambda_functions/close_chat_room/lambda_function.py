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
    required_arguments = ["chatRoomId", "operatorId", "channelId"]
    for argument_name, argument_value in input_arguments.items():
        if argument_name not in required_arguments:
            raise Exception("The '{0}' argument doesn't exist.".format(utils.camel_case(argument_name)))
        if argument_value is None:
            raise Exception("The '{0}' argument can't be None/Null/Undefined.".format(utils.camel_case(argument_name)))
        if argument_name.endswith("Id"):
            try:
                uuid.UUID(argument_value)
            except ValueError:
                raise Exception("The '{0}' argument format is not UUID.".format(utils.camel_case(argument_name)))

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "chat_room_id": input_arguments.get("chatRoomId", None),
            "operator_id": input_arguments.get("operatorId", None),
            "channel_id": input_arguments.get("channelId", None),
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


def get_accepted_chat_room_data(**kwargs) -> Dict[AnyStr, Any]:
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

    # Prepare the CQL query that returns information about the accepted chat room.
    cql_statement = """
    select
        operator_id,
        channel_id,
        chat_room_id,
        client_id,
        last_message_content,
        last_message_date_time
    from
        accepted_chat_rooms
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
        accepted_chat_room_data = cassandra_connection.execute(cql_statement, cql_arguments).one()
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the information about last message data of the completed chat room.
    return accepted_chat_room_data


def create_completed_chat_room(**kwargs) -> None:
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

    # Prepare the CQL query that creates completed chat room.
    cql_statement = """
    insert into completed_chat_rooms (
        operator_id,
        channel_id,
        chat_room_id,
        client_id,
        last_message_content,
        last_message_date_time,
        unread_messages_number
    ) values (
        %(operator_id)s,
        %(channel_id)s,
        %(chat_room_id)s,
        %(client_id)s,
        %(last_message_content)s,
        %(last_message_date_time)s,
        0
    );
    """

    # Execute the CQL query dynamically, in a convenient and safe way.
    try:
        cassandra_connection.execute(cql_statement, cql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return nothing.
    return None


def delete_accepted_chat_room(**kwargs) -> None:
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

    # Prepare the CQL query that deletes accepted chat room.
    cql_statement = """
    delete from
        accepted_chat_rooms
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
        chat_room_status = 'completed'
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


@postgresql_wrapper
def get_operator_data(**kwargs) -> None:
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

    # Prepare the SQL query that returns the information of the specific operator.
    sql_statement = """
    select
        internal_users.auth0_user_id::text,
        internal_users.auth0_metadata::text,
        users.user_id::text,
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
        users
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
        users.user_id = %(operator_id)s
    and
        users.internal_user_id is not null
    and
        users.identified_user_id is null
    and
        users.unidentified_user_id is null
    limit 1;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Put the result of the function in the queue.
    queue.put({"operator_data": cursor.fetchone()})

    # Return nothing.
    return None


def analyze_and_format_operator_data(operator_data: Dict[AnyStr, Any]) -> Dict[AnyStr, Any]:
    # Format the operator data.
    operator = {}
    if operator_data:
        gender, country, role, organization = {}, {}, {}, {}
        for key, value in operator_data.items():
            if key.startswith("gender_"):
                gender[utils.camel_case(key)] = value
            elif key.startswith("country_"):
                country[utils.camel_case(key)] = value
            elif key.startswith("role_"):
                role[utils.camel_case(key)] = value
            elif "organization" in key:
                organization[utils.camel_case(key)] = value
            else:
                operator[utils.camel_case(key)] = value
        operator["gender"] = gender
        operator["country"] = country
        operator["role"] = role
        operator["organization"] = organization

    # Create the response structure and return it.
    return operator


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
    operator_id = input_arguments["operator_id"]
    channel_id = input_arguments["channel_id"]

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]
    cassandra_connection = results_of_tasks["cassandra_connection"]
    set_cassandra_keyspace(cassandra_connection=cassandra_connection)

    # Define the variable that stores information about specific accepted chat room.
    accepted_chat_room_data = get_accepted_chat_room_data(
        cassandra_connection=cassandra_connection,
        cql_arguments={
            "operator_id": uuid.UUID(operator_id),
            "channel_id": uuid.UUID(channel_id),
            "chat_room_id": uuid.UUID(chat_room_id)
        }
    )

    # Run several functions in parallel via multiple threads.
    results_of_tasks = run_multithreading_tasks([
        {
            "function_object": create_completed_chat_room,
            "function_arguments": {
                "cassandra_connection": cassandra_connection,
                "cql_arguments": {
                    "operator_id": accepted_chat_room_data["operator_id"],
                    "channel_id": accepted_chat_room_data["channel_id"],
                    "chat_room_id": accepted_chat_room_data["chat_room_id"],
                    "client_id": accepted_chat_room_data["client_id"],
                    "last_message_content": accepted_chat_room_data["last_message_content"],
                    "last_message_date_time": accepted_chat_room_data["last_message_date_time"]
                }
            }
        },
        {
            "function_object": delete_accepted_chat_room,
            "function_arguments": {
                "cassandra_connection": cassandra_connection,
                "cql_arguments": {
                    "operator_id": accepted_chat_room_data["operator_id"],
                    "channel_id": accepted_chat_room_data["channel_id"],
                    "chat_room_id": accepted_chat_room_data["chat_room_id"]
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
            "function_object": get_operator_data,
            "function_arguments": {
                "postgresql_connection": postgresql_connection,
                "sql_arguments": {
                    "operator_id": operator_id
                }
            }
        }
    ])

    # Define the variable that stores information about the status of the chat room.
    chat_room_status = results_of_tasks["chat_room_status"]

    # Define the variable that stores information about the specific operator.
    operator_data = results_of_tasks["operator_data"]

    # Define the variable that stores analyzed and formatted operator data.
    operator = analyze_and_format_operator_data(operator_data=operator_data)

    # Create the response structure and return it.
    return {
        "chatRoomId": chat_room_id,
        "channelId": channel_id,
        "chatRoomStatus": chat_room_status,
        "operator": operator
    }
