import databases
import utils
import logging
import sys
import os
from psycopg2.extras import RealDictCursor
from cassandra.query import dict_factory


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
    channel_technical_id = event["arguments"]["input"]["channelTechnicalId"]
    channel_type_name = event["arguments"]["input"]["channelTypeName"]
    client_id = event["arguments"]["input"]["clientId"]
    offset = event["arguments"]["currentPageNumber"]
    limit = event["arguments"]["recordsNumber"]
    offset = (offset - 1) * limit

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that gives the N number of chat rooms that the user participated in.
    statement = """
    select
        chat_rooms_users_relationship.entry_created_date_time as chat_room_member_since_date_time,
        chat_rooms.chat_room_id,
        chat_rooms.chat_room_status
    from
        chat_rooms_users_relationship
    left join chat_rooms on
        chat_rooms_users_relationship.chat_room_id = chat_rooms.chat_room_id
    left join channels on
        chat_rooms.channel_id = channels.channel_id
    left join channel_types on
        channels.channel_type_id = channel_types.channel_type_id 
    where
        chat_rooms_users_relationship.user_id = '{0}'
    and
        channels.channel_technical_id = '{1}'
    and
        lower(channel_types.channel_type_name) = lower('{2}')
    order by
        chat_rooms_users_relationship.entry_created_date_time desc
    offset {3} limit {4};
    """.format(
        client_id,
        channel_technical_id,
        channel_type_name,
        offset,
        limit
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
    aggregated_data_entries = cursor.fetchall()

    # Define the empty list.
    chat_rooms = list()

    # Create lists with IDs depending on the type of chat room.
    if aggregated_data_entries is not None:
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

        # Return each row as a dictionary after querying the Cassandra database.
        cassandra_connection.row_factory = dict_factory

        # Define several variables.
        chat_rooms_ids = list()
        chat_rooms_last_messages_storage = dict()

        # Prepare additional data for the chat rooms.
        for entry in aggregated_data_entries:
            # Create the list of chat rooms IDs.
            chat_rooms_ids.append(entry["chat_room_id"])

            # Prepare the CQL query that returns information about the last message of the specific chat room.
            cassandra_query = """
            select
                message_created_date_time,
                message_updated_date_time,
                message_deleted_date_time,
                message_is_sent,
                message_is_delivered,
                message_is_read,
                message_id,
                message_author_id,
                message_channel_id,
                message_type,
                message_text,
                message_content_url,
                quoted_message_id,
                quoted_message_author_id,
                quoted_message_channel_id,
                quoted_message_type,
                quoted_message_text,
                quoted_message_content_url
            from
                chat_rooms_messages
            where
                chat_room_id = {0}
            limit 1;
            """.format(entry["chat_room_id"])

            # Execute a previously prepared SQL query.
            try:
                chat_room_message_entry = cursor.execute(cassandra_query).one()
            except Exception as error:
                logger.error(error)
                sys.exit(1)

            # Analyze the data about chat room message received from the database.
            chat_room_last_message = dict()
            if chat_room_message_entry is not None:
                chat_room_last_message["lastMessageContent"] = chat_room_message_entry["message_text"]
                chat_room_last_message["lastMessageDateTime"] = chat_room_message_entry["message_created_date_time"]
                chat_rooms_last_messages_storage[entry["chat_room_id"]] = chat_room_last_message

        # Define the Determine the last operators of chat rooms.
        statement = """
        select
	        distinct on (chat_rooms_users_relationship.chat_room_id) chat_room_id,
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
            chat_rooms_users_relationship.chat_room_id in ({0})
        and
            users.internal_user_id is not null
        and
            users.unidentified_user_id is null
        and
            users.identified_user_id is null
        order by
            chat_rooms_users_relationship.chat_room_id,
            chat_rooms_users_relationship.entry_created_date_time desc;
        """.format(", ".join("'{0}'".format(item) for item in chat_rooms_ids))

        # Execute a previously prepared SQL query.
        try:
            cursor.execute(statement)
        except Exception as error:
            logger.error(error)
            sys.exit(1)

        # After the successful execution of the query commit your changes to the database.
        postgresql_connection.commit()

        # The data type is the class 'psycopg2.extras.RealDictRow'.
        operators_entries = cursor.fetchall()

        # The cursor will be unusable from this point forward.
        cursor.close()

        # Analyze the data about internal users received from the database.
        operators_storage = dict()
        if operators_entries is not None:
            for entry in operators_entries:
                operator = dict()
                gender = dict()
                country = dict()
                role = dict()
                organization = dict()
                for key, value in entry.items():
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
                    elif key == "chat_room_id":
                        continue
                    else:
                        operator[utils.camel_case(key)] = value
                operator["gender"] = gender
                operator["country"] = country
                operator["role"] = role
                operator["organization"] = organization
                operators_storage[entry["chat_room_id"]] = operator

        # Prepare the final data that will be sent in the response.
        chat_room = dict()
        for entry in aggregated_data_entries:
            for key, value in entry.items():
                if ("_id" in key or "_date_time" in key) and value is not None:
                    value = str(value)
                chat_room[utils.camel_case(key)] = value
            chat_room["operator"] = operators_storage.get(entry["chat_room_id"], None)
            chat_room = {**chat_room, **chat_rooms_last_messages_storage[entry["chat_room_id"]]}
            chat_rooms.append(chat_room)

    # Return the list of aggregated data as the response.
    return chat_rooms
