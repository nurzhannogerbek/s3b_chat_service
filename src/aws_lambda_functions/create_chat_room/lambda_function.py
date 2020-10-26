import databases
import utils
import sys
import logging
import uuid
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from psycopg2.extras import RealDictCursor


"""
Define connections to databases outside of the "lambda_handler" function.
Connections to databases will be created the first time the function is called.
Any subsequent function call will use the same database connections.
"""
cassandra_connection = None
postgresql_connection = None

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
            cassandra_connection = databases.create_cassandra_connection()
        except Exception as error:
            logger.error(error)
            sys.exit(1)
    global postgresql_connection
    if not postgresql_connection:
        try:
            postgresql_connection = databases.create_postgresql_connection()
        except Exception as error:
            logger.error(error)
            sys.exit(1)

    # Define the values of the data passed to the function.
    channel_technical_id = event["arguments"]["input"]["channelTechnicalId"]
    channel_type_name = event["arguments"]["input"]["channelTypeName"]
    client_id = event["arguments"]["input"]["clientId"]

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

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
        channels.channel_technical_id = '{0}'
    and
        lower(channel_types.channel_type_name) = lower('{1}')
    group by
        channels.channel_id,
        channel_types.channel_type_id
    limit 1;
    """.format(
        channel_technical_id.replace("'", "''"),
        channel_type_name
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

    # Generate a unique ID for a new non accepted chat room.
    chat_room_id = str(uuid.uuid1())

    # Set the name of the keyspace you will be working with.
    # This statement must fix ERROR NoHostAvailable: ('Unable to complete the operation against any hosts').
    success = False
    while not success:
        try:
            cassandra_connection.set_keyspace(databases.cassandra_keyspace_name)
            success = True
        except Exception as error:
            try:
                cassandra_connection = databases.create_cassandra_connection()
            except Exception as error:
                logger.error(error)
                sys.exit(1)

    # Define the list of departments that can serve the specific channel.
    organizations_ids = aggregated_entry["organizations_ids"]

    # Prepare the CQL query statement that creates a new non accepted chat room in the Cassandra database.
    if len(organizations_ids) != 0:
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
                aggregated_entry["channel_id"],
                chat_room_id,
                client_id
            )
            statement = SimpleStatement(
                cassandra_query,
                consistency_level=ConsistencyLevel.LOCAL_QUORUM
            )

            # Execute a previously prepared CQL query.
            try:
                cassandra_connection.execute(statement)
            except Exception as error:
                logger.error(error)
                sys.exit(1)

    # Prepare the SQL query statement that creates new non accepted chat room.
    statement = """
    insert into chat_rooms (
        chat_room_id,
        channel_id,
        chat_room_status
    ) values (
        '{0}',
        '{1}',
        'non_accepted'
    )  
    returning
        chat_room_status;
    """.format(
        chat_room_id,
        aggregated_entry["channel_id"]
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

    # Prepare the SQL query statement that add client as a member of the specific chat room.
    statement = """
    insert into chat_rooms_users_relationship (
        chat_room_id,
        user_id
    ) values (
        '{0}',
        '{1}'
    );
    """.format(
        chat_room_id,
        client_id
    )

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Return the new non accepted chat room information as the response.
    response = {
        "chatRoomId": chat_room_id,
        "chatRoomStatus": chat_room_status,
        "channel": channel,
        "channelId": channel["channelId"],
        "client": client,
        "organizationsIds": organizations_ids
    }
    return response
