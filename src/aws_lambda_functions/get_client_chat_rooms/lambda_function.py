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

# Define the global variable.
chat_rooms = []


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
    required_arguments = ["channelTechnicalId", "channelTypeName", "clientId", "currentPageNumber", "recordsNumber"]
    for argument_name, argument_value in input_arguments.items():
        if argument_name in required_arguments and argument_value is None:
            raise Exception("The '{0}' argument can't be None/Null/Undefined.".format(argument_name))
        if argument_name == "clientId":
            try:
                uuid.UUID(argument_value)
            except ValueError:
                raise Exception("The '{0}' argument format is not UUID.".format(argument_name))

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "channel_technical_id": input_arguments.get("channelTechnicalId", None),
            "channel_type_name": input_arguments.get("channelTypeName", None),
            "client_id": input_arguments.get("clientId", None),
            "offset": input_arguments.get("currentPageNumber", None),
            "limit": input_arguments.get("recordsNumber", None),
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


@postgresql_wrapper
def get_aggregated_data(**kwargs) -> List:
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

    # Prepare the SQL query that returns the aggregated data.
    sql_statement = """
    select
        chat_rooms_users_relationship.entry_created_date_time as chat_room_member_since_date_time,
        chat_rooms.chat_room_id::text,
        chat_rooms.chat_room_status::text
    from
        chat_rooms_users_relationship
    left join chat_rooms on
        chat_rooms_users_relationship.chat_room_id = chat_rooms.chat_room_id
    left join channels on
        chat_rooms.channel_id = channels.channel_id
    left join channel_types on
        channels.channel_type_id = channel_types.channel_type_id 
    where
        chat_rooms_users_relationship.user_id = %(client_id)s
    and
        channels.channel_technical_id = %(channel_technical_id)s
    and
        channel_types.channel_type_name = %(channel_type_name)s
    order by
        chat_rooms_users_relationship.entry_created_date_time desc
    offset %(offset)s limit %(limit)s;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the aggregated data.
    return cursor.fetchall()


@postgresql_wrapper
def get_last_operators_data(**kwargs) -> None:
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

    # Prepare the SQL query that returns the last operator information of each client's chat rooms.
    sql_statement = """
    select
        distinct on (chat_rooms_users_relationship.chat_room_id) chat_room_id,
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
        chat_rooms_users_relationship.chat_room_id in %(chat_rooms_ids)s
    and
        users.internal_user_id is not null
    and
        users.unidentified_user_id is null
    and
        users.identified_user_id is null
    order by
        chat_rooms_users_relationship.chat_room_id,
        chat_rooms_users_relationship.entry_created_date_time desc;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Define the variable which stores the list of last operators of chat_rooms.
    operators_data = cursor.fetchall()

    # Define the variable that stores information about last operator from different chat rooms.
    last_operators_data = {}

    # Format the operators data.
    if operators_data:
        for record in operators_data:
            operator, gender, country, role, organization = {}, {}, {}, {}, {}
            for key, value in record.items():
                if key.startswith("gender_"):
                    gender[utils.camel_case(key)] = value
                elif key.startswith("country_"):
                    country[utils.camel_case(key)] = value
                elif key.startswith("role_"):
                    role[utils.camel_case(key)] = value
                elif key.startswith("organization_"):
                    organization[utils.camel_case(key)] = value
                elif key == "chat_room_id":
                    continue
                else:
                    operator[utils.camel_case(key)] = value
            operator["gender"] = gender
            operator["country"] = country
            operator["role"] = role
            operator["organization"] = organization
            last_operators_data[record["chat_room_id"]] = operator

    # Put the result of the function in the queue.
    queue.put({"last_operators_data": last_operators_data})

    # Return nothing.
    return None


def get_last_messages_data(**kwargs) -> None:
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
        aggregated_data = cql_arguments["aggregated_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        queue = kwargs["queue"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Define the variable that stores information about last messages from different chat rooms.
    last_messages_data = {}

    # Parse the aggregated data.
    for record in aggregated_data:
        # Change the data type of the argument.
        cql_arguments["chat_room_id"] = uuid.UUID(record["chat_room_id"])

        # Prepare the CQL query that returns the contents of the last message of the particular chat room.
        cql_statement = """
        select
            message_text,
            message_created_date_time
        from
            chat_rooms_messages
        where
            chat_room_id = %(chat_room_id)s
        limit 1;
        """

        # Execute the CQL query dynamically, in a convenient and safe way.
        try:
            last_message_data = cassandra_connection.execute(cql_statement, cql_arguments).one()
        except Exception as error:
            logger.error(error)
            raise Exception(error)

        # Analyze the data about chat room message received from the database.
        last_message_content = {}
        if last_message_data:
            last_message_content["lastMessageContent"] = last_message_data["message_text"]
            last_message_content["lastMessageDateTime"] = last_message_data["message_created_date_time"].isoformat()
            last_messages_data[record["chat_room_id"]] = last_message_content

    # Put the result of the function in the queue.
    queue.put({"last_messages_data": last_messages_data})

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
        last_operators_data = kwargs["last_operators_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        last_messages_data = kwargs["last_messages_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the aggregated data.
    chat_room = {}
    for record in aggregated_data:
        for key, value in record.items():
            if key.endswith("_date_time") and value is not None:
                value = value.isoformat()
            chat_room[utils.camel_case(key)] = value
        chat_room["operator"] = last_operators_data.get(record["chat_room_id"], None)
        if last_messages_data.__contains__(record["chat_room_id"]):
            chat_room = {**chat_room, **last_messages_data[record["chat_room_id"]]}
        chat_rooms.append(chat_room)

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
    channel_technical_id = input_arguments["channel_technical_id"]
    channel_type_name = input_arguments["channel_type_name"]
    client_id = input_arguments["client_id"]
    limit = input_arguments["limit"]
    offset = (input_arguments["offset"] - 1) * limit

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]
    cassandra_connection = results_of_tasks["cassandra_connection"]
    set_cassandra_keyspace(cassandra_connection=cassandra_connection)

    # Define the variable that stores information about aggregated data.
    aggregated_data = get_aggregated_data(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "client_id": client_id,
            "channel_technical_id": channel_technical_id,
            "channel_type_name": channel_type_name,
            "offset": offset,
            "limit": limit
        }
    )

    # Check if there is aggregated data.
    if aggregated_data:
        # Define the list of chat rooms ids.
        chat_rooms_ids = [item["chat_room_id"] for item in aggregated_data]

        # Run several initialization functions in parallel.
        results_of_tasks = run_multithreading_tasks([
            {
                "function_object": get_last_messages_data,
                "function_arguments": {
                    "cassandra_connection": cassandra_connection,
                    "cql_arguments": {
                        "aggregated_data": aggregated_data
                    }
                }
            },
            {
                "function_object": get_last_operators_data,
                "function_arguments": {
                    "postgresql_connection": postgresql_connection,
                    "sql_arguments": {
                        "chat_rooms_ids": tuple(chat_rooms_ids)
                    }
                }
            }
        ])

        # Define a few necessary variables that will be used in the future.
        last_operators_data = results_of_tasks["last_operators_data"]
        last_messages_data = results_of_tasks["last_messages_data"]

        # Analyze and format all aggregated data obtained earlier from databases.
        analyze_and_format_aggregated_data(
            aggregated_data=aggregated_data,
            last_operators_data=last_operators_data,
            last_messages_data=last_messages_data
        )

    # Return the list of client's chat rooms.
    return chat_rooms
