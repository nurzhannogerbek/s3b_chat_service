import databases
import utils
import logging
import sys
import os
from psycopg2.extras import RealDictCursor


"""
Define connections to databases outside of the "lambda_handler" function.
Connections to databases will be created the first time the function is called.
Any subsequent function call will use the same database connections.
"""
cassandra_connection = None
postgresql_connection = None

# Define databases settings parameters.
CASSANDRA_USERNAME = os.environ["CASSANDRA_USERNAME"]
CASSANDRA_PASSWORD = os.environ["CASSANDRA_PASSWORD"]
CASSANDRA_HOST = os.environ["CASSANDRA_HOST"].split(',')
CASSANDRA_PORT = int(os.environ["CASSANDRA_PORT"])
CASSANDRA_LOCAL_DC = os.environ["CASSANDRA_LOCAL_DC"]
CASSANDRA_KEYSPACE_NAME = os.environ["CASSANDRA_KEYSPACE_NAME"]
POSTGRESQL_USERNAME = os.environ["POSTGRESQL_USERNAME"]
POSTGRESQL_PASSWORD = os.environ["POSTGRESQL_PASSWORD"]
POSTGRESQL_HOST = os.environ["POSTGRESQL_HOST"]
POSTGRESQL_PORT = int(os.environ["POSTGRESQL_PORT"])
POSTGRESQL_DB_NAME = os.environ["POSTGRESQL_DB_NAME"]

logger = logging.getLogger(__name__)  # Create the logger with the specified name.
logger.setLevel(logging.WARNING)  # Set the logging level of the logger.


def lambda_handler(event, context):
    """
    :argument event: The AWS Lambda uses this parameter to pass in event data to the handler.
    :argument context: The AWS Lambda uses this parameter to provide runtime information to your handler.
    """
    # Since connections with databases were defined outside of the function, we create global variables.
    global cassandra_connection
    if not cassandra_connection:
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
            sys.exit(1)
    global postgresql_connection
    if not postgresql_connection:
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
            sys.exit(1)

    # Define the values of the data passed to the function.
    chat_room_id = event["arguments"]["input"]["chatRoomId"]
    operator_id = event["arguments"]["input"]["operatorId"]

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that allows to get the list of departments that serve the specific this channel.
    statement = """
    select
        channel_id
    from
        chat_rooms
    where
        chat_room_id = '{0}'
    limit 1;
    """.format(chat_room_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Fetch the next row of a query result set.
    channel_id = cursor.fetchone()["channel_id"]

    # Set the name of the keyspace you will be working with.
    # This statement must fix ERROR NoHostAvailable: ('Unable to complete the operation against any hosts').
    success = False
    while not success:
        try:
            cassandra_connection.set_keyspace(CASSANDRA_KEYSPACE_NAME)
            success = True
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
                sys.exit(1)

    # Prepare the CQL query statement that returns the information of the accepted chat room.
    cassandra_query = '''
    select
        operator_id,
        channel_id,
        chat_room_id,
        client_id,
        last_message_content,
        last_message_date_time,
        unread_messages_number
    from
        accepted_chat_rooms
    where
        operator_id = {0}
    and
        channel_id = {1}
    and
        chat_room_id = {2}
    limit 1;
    '''.format(
        operator_id,
        channel_id,
        chat_room_id
    )

    # Execute a previously prepared CQL query.
    try:
        accepted_chat_room_entry = cassandra_connection.execute(cassandra_query).one()
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    """
    Prepare the CQL query statement that transfers chat room information from one table to another.
    Dates, IP addresses, and strings need to be enclosed in single quotation marks.
    To use a single quotation mark itself in a string literal, escape it using a single quotation mark.
    """
    cassandra_query = """
    insert into completed_chat_rooms (
        operator_id,
        channel_id,
        chat_room_id,
        client_id,
        last_message_content,
        last_message_date_time,
        unread_messages_number
    ) values (
        {0},
        {1},
        {2},
        {3},
        {4},
        {5},
        {6}
    );
    """.format(
        accepted_chat_room_entry["operator_id"],
        accepted_chat_room_entry["channel_id"],
        accepted_chat_room_entry["chat_room_id"],
        accepted_chat_room_entry["client_id"],
        'null' if accepted_chat_room_entry["last_message_content"] is None
        else "'{0}'".format(accepted_chat_room_entry["last_message_content"].replace("'", "''")),
        'null' if accepted_chat_room_entry["last_message_date_time"] is None
        else "'{0}'".format(utils.date_time_formatter(str(accepted_chat_room_entry["last_message_date_time"]))),
        'null' if accepted_chat_room_entry["unread_messages_number"] is None
        else "{0}".format(accepted_chat_room_entry["unread_messages_number"])
    )

    # Execute a previously prepared CQL query.
    try:
        cassandra_connection.execute(cassandra_query)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Prepare the CQL query statement that deletes chat room information from 'accepted_chat_rooms' table.
    cassandra_query = """
    delete from
        accepted_chat_rooms
    where
        operator_id = {0}
    and
        channel_id = {1}
    and
        chat_room_id = {2};
    """.format(
        operator_id,
        channel_id,
        chat_room_id
    )

    # Execute a previously prepared CQL query.
    try:
        cassandra_connection.execute(cassandra_query)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Prepare the SQL query statement that update the status of the specific chat room.
    statement = """
    update
        chat_rooms
    set
        chat_room_status = 'completed'
    where
        chat_room_id = '{0}'
    returning
        chat_room_status;
    """.format(chat_room_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Fetch the next row of a query result set.
    chat_room_status = cursor.fetchone()["chat_room_status"]

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Prepare the SQL request that returns all detailed information about specific internal user.
    statement = """
    select
        internal_users.auth0_user_id,
        internal_users.auth0_metadata::text,
        users.user_id,
        internal_users.internal_user_first_name as user_first_name,
        internal_users.internal_user_last_name as user_last_name,
        internal_users.internal_user_middle_name as user_middle_name,
        internal_users.internal_user_primary_email as user_primary_email,
        internal_users.internal_user_secondary_email as user_secondary_email,
        internal_users.internal_user_primary_phone_number as user_primary_phone_number,
        internal_users.internal_user_secondary_phone_number as user_secondary_phone_number,
        internal_users.internal_user_profile_photo_url as user_profile_photo_url,
        internal_users.internal_user_position_name as user_position_name,
        genders.gender_id,
        genders.gender_technical_name,
        genders.gender_public_name,
        countries.country_id,
        countries.country_short_name,
        countries.country_official_name,
        countries.country_alpha_2_code,
        countries.country_alpha_3_code,
        countries.country_numeric_code,
        countries.country_code_top_level_domain,
        roles.role_id,
        roles.role_technical_name,
        roles.role_public_name,
        roles.role_description,
        organizations.organization_id,
        organizations.organization_name,
        organizations.organization_description,
        organizations.parent_organization_id,
        organizations.parent_organization_name,
        organizations.parent_organization_description,
        organizations.root_organization_id,
        organizations.root_organization_name,
        organizations.root_organization_description
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
        users.user_id = '{0}'
    and
        users.internal_user_id is not null
    limit 1;
    """.format(operator_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Fetch the next row of a query result set.
    internal_user_entry = cursor.fetchone()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Analyze the data about internal user received from the database.
    internal_user = dict()
    if internal_user_entry is not None:
        gender = dict()
        country = dict()
        role = dict()
        organization = dict()
        for key, value in internal_user_entry.items():
            if "_id" in key and value is not None:
                value = str(value)
            if "gender_" in key:
                gender[utils.camel_case(key)] = value
            elif "country_" in key:
                country[utils.camel_case(key)] = value
            elif "role_" in key:
                role[utils.camel_case(key)] = value
            elif "organization_" in key:
                organization[utils.camel_case(key)] = value
            else:
                internal_user[utils.camel_case(key)] = value
        internal_user["gender"] = gender
        internal_user["country"] = country
        internal_user["role"] = role
        internal_user["organization"] = organization

    # Form the response structure.
    response = {
        "chatRoomId": chat_room_id,
        "chatRoomStatus": chat_room_status,
        "operator": internal_user
    }
    return response
