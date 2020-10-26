import databases
import utils
import logging
import sys
import itertools
import copy
from psycopg2.extras import RealDictCursor

"""
Define the connection to the database outside of the "lambda_handler" function.
The connection to the database will be created the first time the function is called.
Any subsequent function call will use the same database connection.
"""
postgresql_connection = None

logger = logging.getLogger(__name__)  # Create the logger with the specified name.
logger.setLevel(logging.WARNING)  # Set the logging level of the logger.


def lambda_handler(event, context):
    """
    :argument event: The AWS Lambda uses this parameter to pass in event data to the handler.
    :argument context: The AWS Lambda uses this parameter to provide runtime information to your handler.
    """
    # Since the connection with database were defined outside of the function, we create global variable.
    global postgresql_connection
    if not postgresql_connection:
        try:
            postgresql_connection = databases.create_postgresql_connection()
        except Exception as error:
            logger.error(error)
            sys.exit(1)

    # Define the value of the data passed to the function.
    user_id = event["arguments"]['userId']

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that returns information about channel types and channels.
    statement = """
    select
        channels.channel_id::varchar,
        channels.channel_name,
        channels.channel_description,
        channels.channel_technical_id,
        channel_types.channel_type_id::varchar,
        channel_types.channel_type_name,
        channel_types.channel_type_description
    from
        channels
    left join channel_types on
        channels.channel_type_id = channel_types.channel_type_id
    where
        channels.channel_id in (
            select
                channels_organizations_relationship.channel_id
            from
                channels_organizations_relationship
            left join internal_users on
                channels_organizations_relationship.organization_id = internal_users.organization_id
            left join users on
                internal_users.internal_user_id = users.internal_user_id
            where
                users.user_id = '{0}'
            and
                users.internal_user_id is not null
        );
    """.format(user_id)

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # The data type is the class 'psycopg2.extras.RealDictRow'.
    channels_types_and_channels_entries = cursor.fetchall()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Check the data about channels types and channels received from the database.
    channels_types_and_channels = list()
    if channels_types_and_channels_entries is not None:
        # Pre-sort the data to group it later.
        channels_types_and_channels_entries = sorted(
            channels_types_and_channels_entries,
            key=lambda x: x['channel_type_id']
        )

        # The "groupby" function makes grouping objects in an iterable a snap.
        grouped_records = itertools.groupby(
            copy.deepcopy(channels_types_and_channels_entries),
            key=lambda x: x['channel_type_id']
        )

        # Create the list of keys.
        deleted_keys = ["channel_type_id", "channel_type_name", "channel_type_description"]

        # Store channels grouped by types in a dictionary for quick search and matching.
        storage = dict()

        # Analyze data, clean the dictionaries of certain keys and convert the key names in the dictionaries.
        for key, records in grouped_records:
            cleaned_records = list()
            for record in list(records):
                cleaned_record = dict()
                [record.pop(item) for item in deleted_keys]
                for _key, _value in record.items():
                    cleaned_record[utils.camel_case(_key)] = _value
                cleaned_records.append(cleaned_record)
            storage[key] = cleaned_records

        # Analyze the list with data about channels and channels types received from the database.
        channels_types_ids = list()
        for entry in channels_types_and_channels_entries:
            channel_type_and_channel = dict()
            if entry['channel_type_id'] not in channels_types_ids:
                channels_types_ids.append(entry['channel_type_id'])
                for key, value in entry.items():
                    if key in deleted_keys:
                        channel_type_and_channel[utils.camel_case(key)] = value
                    channel_type_and_channel["channels"] = storage[entry["channel_type_id"]]
                channels_types_and_channels.append(channel_type_and_channel)

    # Return the list of channels types and channels as the response.
    return channels_types_and_channels
