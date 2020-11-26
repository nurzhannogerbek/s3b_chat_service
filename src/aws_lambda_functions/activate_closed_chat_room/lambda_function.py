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
    client_id = event["arguments"]["input"]["clientId"]

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that gives the id of the last operator in the chat room and id of the channel.
    statement = """
    select
        users.user_id as operator_id,
        chat_rooms.channel_id
    from
        chat_rooms_users_relationship
    left join users on
        chat_rooms_users_relationship.user_id = users.user_id
    left join chat_rooms on
        chat_rooms_users_relationship.chat_room_id = chat_rooms.chat_room_id
    where
        chat_rooms_users_relationship.chat_room_id = '{0}'
    and
        users.internal_user_id is not null
    and
        users.identified_user_id is null
    and
        users.unidentified_user_id is null
    order by
        chat_rooms_users_relationship.entry_created_date_time desc
    limit 1;
    """.format(chat_room_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Fetch the next row of a query result set.
    aggregated_entry = cursor.fetchone()
    if aggregated_entry is None:
        sys.exit(1)

    # Define several variables.
    operator_id = aggregated_entry["operator_id"]
    channel_id = aggregated_entry["channel_id"]

    # Prepare the SQL request that allows to get the list of departments that serve the specific this channel.
    statement = """
    select
        channels.channel_id,
        channels.channel_name,
        channels.channel_description,
        channels.channel_technical_id,
        channel_types.channel_type_id,
        channel_types.channel_type_name,
        channel_types.channel_type_description,
        array_agg(distinct organization_id)::varchar[] as organizations_ids
    from
        channels
    left join channel_types on
        channels.channel_type_id = channel_types.channel_type_id
    left join channels_organizations_relationship on
        channels.channel_id = channels_organizations_relationship.channel_id
    where
        channels.channel_id = '{0}'
    group by
        channels.channel_id,
        channel_types.channel_type_id
    limit 1;
    """.format(channel_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Fetch the next row of a query result set.
    aggregated_entry = cursor.fetchone()
    if aggregated_entry is None:
        sys.exit(1)

    # Check the data about channels types and channels received from the database.
    channel = dict()
    channel_type = dict()
    for key, value in aggregated_entry.items():
        if "_id" in key and value is not None:
            value = str(value)
        if any([i in key for i in ["channel_type_id", "channel_type_name", "channel_type_description"]]):
            channel_type[utils.camel_case(key)] = value
        else:
            channel[utils.camel_case(key)] = value
        channel["channelType"] = channel_type

    # Define the variable with the IDs of the organizations.
    organizations_ids = aggregated_entry["organizations_ids"]

    # Prepare the SQL request that returns the information of the specific client.
    statement = """
    select
        users.user_id,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_first_name
            else null
        end as user_first_name,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_last_name
            else null
        end as user_last_name,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_middle_name
            else null
        end as user_middle_name,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_primary_email
            else null
        end as user_primary_email,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_secondary_email
            else null
            end as user_secondary_email,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_primary_phone_number
            else null
        end as user_primary_phone_number,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_secondary_phone_number
            else null
        end as user_secondary_phone_number,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.identified_user_profile_photo_url
            else null
        end as user_profile_photo_url,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then 'identified_user'
            else 'unidentified_user'
        end as user_type,
        case
            when users.identified_user_id is not null and users.unidentified_user_id is null
            then identified_users.metadata::text
            else unidentified_users.metadata::text
        end as metadata,
        genders.gender_id,
        genders.gender_technical_name,
        genders.gender_public_name,
        countries.country_id,
        countries.country_short_name,
        countries.country_official_name,
        countries.country_alpha_2_code,
        countries.country_alpha_3_code,
        countries.country_numeric_code,
        countries.country_code_top_level_domain
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
        users.user_id = '{0}'
    limit 1;
    """.format(client_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Fetch the next row of a query result set.
    client_entry = cursor.fetchone()

    # Analyze the data about client received from the database.
    client = dict()
    if client_entry is not None:
        gender = dict()
        country = dict()
        for key, value in client_entry.items():
            if "_id" in key and value is not None:
                value = str(value)
            if "gender_" in key:
                gender[utils.camel_case(key)] = value
            elif "country_" in key:
                country[utils.camel_case(key)] = value
            else:
                client[utils.camel_case(key)] = value
        client["gender"] = gender
        client["country"] = country

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

    """
    Prepare the CQL query statement that transfers chat room information from one table to another.
    Dates, IP addresses, and strings need to be enclosed in single quotation marks.
    To use a single quotation mark itself in a string literal, escape it using a single quotation mark.
    """
    for organization_id in organizations_ids:
        cassandra_query = """
        insert into non_accepted_chat_rooms (
            organization_id,
            channel_id,
            chat_room_id,
            client_id
        ) values (
            {0},
            {1},
            {2},
            {3}
        );
        """.format(
            organization_id,
            channel_id,
            chat_room_id,
            client_id
        )

        # Execute a previously prepared CQL query.
        try:
            cassandra_connection.execute(cassandra_query)
        except Exception as error:
            logger.error(error)
            sys.exit(1)

    # Prepare the CQL query statement that deletes chat room information from 'completed_chat_rooms' table.
    cassandra_query = """
    delete from
        completed_chat_rooms
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
        chat_room_status = 'non_accepted'
    where
        chat_room_id = '{0}'
    returning
        chat_room_status;
    """.format(
        chat_room_id,
    )

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Fetch the next row of a query result set.
    chat_room_status = cursor.fetchone()["chat_room_status"]

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Form the response structure.
    response = {
        "chatRoomId": chat_room_id,
        "chatRoomStatus": chat_room_status,
        "channel": channel,
        "channelId": channel_id,
        "client": client,
        "organizationsIds": organizations_ids
    }
    return response
