import logging
import os
from cassandra.query import BatchStatement, SimpleStatement
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection
from cassandra.cluster import Session
from functools import wraps
from typing import *
import uuid
import asyncio
from functools import partial
from cassandra.policies import RetryPolicy
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

# The connection to the database will be created the first time the AWS Lambda function is called.
# Any subsequent call to the function will use the same database connection until the container stops.
POSTGRESQL_CONNECTION = None
CASSANDRA_CONNECTION = None


def check_input_arguments(event: Dict[AnyStr, Any]) -> Dict[AnyStr, Any]:
    # Make sure that all the necessary arguments for the AWS Lambda function are present.
    try:
        input_arguments = event["arguments"]["input"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Check the format and values of required arguments in the list of input arguments.
    required_arguments = ["chatRoomId", "operatorId", "clientId"]
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

    # Create the response structure and return it.
    return {
        "chat_room_id": input_arguments.get("chatRoomId", None),
        "operator_id": input_arguments.get("operatorId", None),
        "client_id": input_arguments.get("clientId", None)
    }


def reuse_or_recreate_postgresql_connection() -> connection:
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
    return POSTGRESQL_CONNECTION


def reuse_or_recreate_cassandra_connection() -> Session:
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
    return CASSANDRA_CONNECTION


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
def get_aggregated_data(**kwargs) -> Dict[AnyStr, Any]:
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

    # Prepare the SQL query to get a list of departments that serve a specific channel.
    sql_statement = """
    select
        chat_rooms.channel_id::text,
        array_agg(distinct channels_organizations_relationship.organization_id)::text[] as organizations_ids
    from
        chat_rooms
    left join channels_organizations_relationship on
        chat_rooms.channel_id = channels_organizations_relationship.channel_id
    where
        chat_rooms.chat_room_id = %(chat_room_id)s
    group by
        chat_rooms.channel_id
    limit 1;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Create the response structure and return it.
    return cursor.fetchone()


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
        non_accepted_chat_rooms
    where
        organization_id = %(organization_id)s
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


def fire_and_forget_wrapper(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        if callable(function):
            return loop.run_in_executor(None, partial(function, *args, **kwargs))
        else:
            raise Exception("The '%s' function must be a callable.".format(function.__name__))

    return wrapper


@fire_and_forget_wrapper
def create_accepted_chat_room(**kwargs) -> None:
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

    # Prepare the CQL query that creates an accepted chat room.
    cql_statement = """
    insert into accepted_chat_rooms (
        operator_id,
        channel_id,
        chat_room_id,
        client_id,
        last_message_content,
        last_message_date_time
    ) values (
        %(operator_id)s,
        %(channel_id)s,
        %(chat_room_id)s,
        %(client_id)s,
        %(last_message_content)s,
        %(last_message_date_time)s
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


def delete_non_accepted_chat_room(**kwargs) -> None:
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

    # Prepare the CQL query that deletes a non accepted chat room.
    cql_statement = cassandra_connection.prepare("""
    delete from
        non_accepted_chat_rooms
    where
        organization_id = %(organization_id)s
    and
        channel_id = %(channel_id)s
    and
        chat_room_id = %(chat_room_id)s;
    """)

    # Create the instance of the "BatchStatement" to delete bulk data in Cassandra by one query.
    batch = BatchStatement(retry_policy=RetryPolicy)

    # For each organization that can serve the chat room, we delete an entry in the database.
    for organization_id in organizations_ids:
        cql_arguments["organization_id"] = uuid.UUID(organization_id)
        batch.add(cql_statement, cql_arguments)

    # Execute the CQL query dynamically, in a convenient and safe way.
    try:
        cassandra_connection.execute(batch)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return nothing.
    return None


@postgresql_wrapper
def update_chat_room_status(**kwargs) -> Dict[AnyStr, Any]:
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

    # Prepare the SQL query that updates the status of the specific chat room.
    sql_statement = """
    update
        chat_rooms
    set
        chat_room_status = 'accepted'
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

    # Create the response structure and return it.
    return cursor.fetchone()["chat_room_status"]


@fire_and_forget_wrapper
@postgresql_wrapper
def set_responsible_operator(**kwargs) -> None:
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

    # Prepare the SQL query that records that the operator took the chat room to work.
    sql_statement = """
    insert into chat_rooms_users_relationship (
        chat_room_id,
        user_id
    ) values (
        %(chat_room_id)s,
        %(operator_id)s
    );
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return nothing.
    return None


@postgresql_wrapper
def get_operator_data(**kwargs) -> Dict[AnyStr, Any]:
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

    # Create the response structure and return it.
    return cursor.fetchone()


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
            elif key.startswith("organization_"):
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
    # First check and then define the input arguments of the AWS Lambda function.
    input_arguments = check_input_arguments(event=event)
    chat_room_id = input_arguments["chat_room_id"]
    operator_id = input_arguments["operator_id"]
    client_id = input_arguments["client_id"]

    # Define the instances of the database connections.
    postgresql_connection = reuse_or_recreate_postgresql_connection()
    cassandra_connection = reuse_or_recreate_cassandra_connection()
    set_cassandra_keyspace(cassandra_connection=cassandra_connection)

    # Define the variable that stores information about aggregated data.
    aggregated_data = get_aggregated_data(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "chat_room_id": chat_room_id
        }
    )

    # Return the message to the client that there is no data for the chat room.
    if not aggregated_data:
        raise Exception("The chat room data was not found in the database.")

    # Define a few necessary variables that will be used in the future.
    channel_id = aggregated_data["channel_id"]
    organizations_ids = aggregated_data["organizations_ids"]

    # Define the variable that stores information about last message data of the completed chat room.
    last_message_data = get_last_message_data(
        cassandra_connection=cassandra_connection,
        cql_arguments={
            "organization_id": uuid.UUID(organizations_ids[0]),
            "channel_id": uuid.UUID(channel_id),
            "chat_room_id": uuid.UUID(chat_room_id)
        }
    )

    # Define a few necessary variables that will be used in the future.
    last_message_content = last_message_data.get("last_message_content", None)
    last_message_date_time = last_message_data.get("last_message_date_time", None)

    # Run several related functions to create/delete all necessary data in different databases tables.
    create_accepted_chat_room(
        cassandra_connection=cassandra_connection,
        cql_arguments={
            "operator_id": uuid.UUID(operator_id),
            "channel_id": uuid.UUID(channel_id),
            "chat_room_id": uuid.UUID(chat_room_id),
            "client_id": uuid.UUID(client_id),
            "last_message_content": last_message_content,
            "last_message_date_time": last_message_date_time
        }
    )
    delete_non_accepted_chat_room(
        cassandra_connection=cassandra_connection,
        cql_arguments={
            "organizations_ids": organizations_ids,
            "channel_id": uuid.UUID(channel_id),
            "chat_room_id": uuid.UUID(chat_room_id)
        }
    )
    set_responsible_operator(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "chat_room_id": chat_room_id,
            "operator_id": operator_id
        }
    )

    # Define the variable that stores information about the status of the chat room.
    chat_room_status = update_chat_room_status(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "chat_room_id": chat_room_id
        }
    )

    # Define the variable that stores information about the specific operator.
    operator_data = get_operator_data(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "operator_id": operator_id
        }
    )

    # Analyze and format operator data.
    operator = analyze_and_format_operator_data(operator_data=operator_data)

    # Create the response structure and return it.
    return {
        "chatRoomId": chat_room_id,
        "channelId": channel_id,
        "chatRoomStatus": chat_room_status,
        "operator": operator
    }
