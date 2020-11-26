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
    operator_id = event["arguments"]['operatorId']
    channels_ids = event["arguments"]['channelsIds']
    try:
        start_date_time = event["arguments"]['startDateTime']
    except KeyError:
        start_date_time = None
    try:
        end_date_time = event["arguments"]['endDateTime']
    except KeyError:
        end_date_time = None

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

    # Initialize an empty list where information about the chat rooms that are accepted for work will be stored.
    accepted_chat_rooms_entries = list()

    # Find out what chat rooms each channel has.
    # The 'IN' operator is not yet supported in the Amazon Keyspaces.
    for channel_id in channels_ids:
        if start_date_time is None and end_date_time is None:
            cassandra_query = '''
            select
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
                channel_id = {1};
            '''.format(
                operator_id,
                channel_id
            )
        else:
            cassandra_query = '''
            select
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
                chat_room_id > maxTimeuuid('{2}')
            and
                chat_room_id < minTimeuuid('{3}');
            '''.format(
                operator_id,
                channel_id,
                start_date_time,
                end_date_time
            )

        # Execute a previously prepared CQL query.
        try:
            entries = cassandra_connection.execute(cassandra_query)
        except Exception as error:
            logger.error(error)
            sys.exit(1)

        # Use the 'extend()' method to add 'entries' at the end of 'accepted_chat_rooms_entries'.
        accepted_chat_rooms_entries.extend(entries)

    # Initialize an empty list.
    accepted_chat_rooms = list()

    # Check for data in the list that we received from the database.
    if len(accepted_chat_rooms_entries) != 0 or accepted_chat_rooms_entries is not None:
        # Create the list of IDs for all clients.
        clients_ids = list()
        for entry in accepted_chat_rooms_entries:
            clients_ids.append(entry["client_id"])

        # With a dictionary cursor, the data is sent in a form of Python dictionaries.
        cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

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
            users.user_id in ({0});
        """.format(", ".join("'{0}'".format(item) for item in clients_ids))

        # Execute a previously prepared SQL query.
        try:
            cursor.execute(statement)
        except Exception as error:
            logger.error(error)
            sys.exit(1)

        # After the successful execution of the query commit your changes to the database.
        postgresql_connection.commit()

        # The data type is the class 'psycopg2.extras.RealDictRow'.
        clients_entries = cursor.fetchall()

        # Analyze the list with data users received from the database.
        clients_storage = dict()
        for entry in clients_entries:
            client = dict()
            gender = dict()
            country = dict()
            for key, value in entry.items():
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
            clients_storage[entry["user_id"]] = client

        # Prepare the SQL request that returns information about channels and channels types.
        statement = """
        select
            channels.channel_id,
            channels.channel_name,
            channels.channel_description,
            channels.channel_technical_id,
            channel_types.channel_type_id,
            channel_types.channel_type_name,
            channel_types.channel_type_description
        from
            channels
        left join channel_types on
            channels.channel_type_id = channel_types.channel_type_id
        where
            channels.channel_id in ({0});
        """.format(", ".join("'{0}'".format(item) for item in channels_ids))

        # Execute a previously prepared SQL query.
        try:
            cursor.execute(statement)
        except Exception as error:
            logger.error(error)
            sys.exit(1)

        # After the successful execution of the query commit your changes to the database.
        postgresql_connection.commit()

        # The data type is the class 'psycopg2.extras.RealDictRow'.
        channels_and_channels_types_entries = cursor.fetchall()

        # The cursor will be unusable from this point forward.
        cursor.close()

        # Analyze the data about channels and channels types received from the database.
        channels_and_channels_types_storage = dict()
        for entry in channels_and_channels_types_entries:
            channel = dict()
            channel_type = dict()
            for key, value in entry.items():
                # Convert the UUID data type to the string.
                if "_id" in key and value is not None:
                    value = str(value)
                if any([i in key for i in ["channel_type_id", "channel_type_name", "channel_type_description"]]):
                    channel_type[utils.camel_case(key)] = value
                else:
                    channel[utils.camel_case(key)] = value
            channel["channelType"] = channel_type
            channels_and_channels_types_storage[entry["channel_id"]] = channel

        # Analyze the list with data about accepted chat rooms received from the database.
        for entry in accepted_chat_rooms_entries:
            accepted_chat_room = dict()
            for key, value in entry.items():
                # Convert the UUID and DateTime data types to the string.
                if ("_id" in key or "_date_time" in key) and value is not None:
                    value = str(value)
                if key == "client_id":
                    accepted_chat_room["client"] = clients_storage[value]
                elif key == "channel_id":
                    accepted_chat_room["channel"] = channels_and_channels_types_storage[value]
                else:
                    accepted_chat_room[utils.camel_case(key)] = value
            accepted_chat_rooms.append(accepted_chat_room)

    # Return the list of accepted chat rooms.
    return accepted_chat_rooms
