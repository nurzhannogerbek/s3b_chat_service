import databases
import utils
import logging
import sys
import os
import uuid
from cassandra.query import SimpleStatement, dict_factory
from cassandra import ConsistencyLevel
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
    message_author_id = event["arguments"]["input"]["messageAuthorId"]
    message_channel_id = event["arguments"]["input"]["messageChannelId"]
    message_type = event["arguments"]["input"]["messageType"]
    try:
        message_text = event["arguments"]["input"]["messageText"]
    except KeyError:
        message_text = None
    try:
        message_content_url = event["arguments"]["input"]["messageContentUrl"]
    except KeyError:
        message_content_url = None
    try:
        quoted_message_id = event["arguments"]["input"]["quotedMessage"]["messageId"]
    except KeyError:
        quoted_message_id = None
    try:
        quoted_message_author_id = event["arguments"]["input"]["quotedMessage"]["messageAuthorId"]
    except KeyError:
        quoted_message_author_id = None
    try:
        quoted_message_channel_id = event["arguments"]["input"]["quotedMessage"]["messageChannelId"]
    except KeyError:
        quoted_message_channel_id = None
    try:
        quoted_message_type = event["arguments"]["input"]["quotedMessage"]["messageType"]
    except KeyError:
        quoted_message_type = None
    try:
        quoted_message_text = event["arguments"]["input"]["quotedMessage"]["messageText"]
    except KeyError:
        quoted_message_text = None
    try:
        quoted_message_content_url = event["arguments"]["input"]["quotedMessage"]["messageContentUrl"]
    except KeyError:
        quoted_message_content_url = None

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that gives the minimal information about the specific chat room.
    statement = """
    select
        channel_id,
        chat_room_status
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
    minimal_chat_room_information = cursor.fetchone()
    if minimal_chat_room_information is None:
        sys.exit(1)

    # Define several necessary variables.
    channel_id = minimal_chat_room_information["channel_id"]
    chat_room_status = minimal_chat_room_information["chat_room_status"]

    # Prepare the SQL request that gives the id of the last operator in the chat room.
    statement = """
    select
        users.user_id as operator_id
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

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Fetch the next row of a query result set.
    operator_entry = cursor.fetchone()
    operator_id = None
    if operator_entry is not None:
        operator_id = operator_entry["operator_id"]

    # The cursor will be unusable from this point forward.
    cursor.close()

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

    # The data type of the 'message_id' column is 'timeuuid' which is and this format is comparable to uuid v1.
    message_id = uuid.uuid1()

    """
    Prepare the CQL query statement that creates a new message in the specific chat room.
    Dates, IP addresses, and strings need to be enclosed in single quotation marks.
    To use a single quotation mark itself in a string literal, escape it using a single quotation mark.
    """
    cassandra_query = """
    insert into chat_rooms_messages (
        chat_room_id,
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
    ) values (
        {0},
        toTimestamp(now()),
        toTimestamp(now()),
        null,
        true,
        false,
        false,
        {1},
        {2},
        {3},
        {4},
        {5},
        {6},
        {7},
        {8},
        {9},
        {10},
        {11},
        {12}
    );
    """.format(
        chat_room_id,
        message_id,
        message_author_id,
        message_channel_id,
        "'{0}'".format(message_type),
        'null' if message_text is None or len(message_text) == 0
        else "'{0}'".format(message_text.replace("'", "''")),
        'null' if message_content_url is None or len(message_content_url) == 0
        else "'{0}'".format(message_content_url),
        'null' if quoted_message_id is None or len(quoted_message_id) == 0
        else quoted_message_id,
        'null' if quoted_message_author_id is None or len(quoted_message_author_id) == 0
        else quoted_message_author_id,
        'null' if quoted_message_channel_id is None or len(quoted_message_channel_id) == 0
        else quoted_message_channel_id,
        'null' if quoted_message_type is None or len(quoted_message_type) == 0
        else "'{0}'".format(quoted_message_type),
        'null' if quoted_message_text is None or len(quoted_message_text) == 0
        else "'{0}'".format(quoted_message_text.replace("'", "''")),
        'null' if quoted_message_content_url is None or len(quoted_message_content_url) == 0
        else "'{0}'".format(quoted_message_content_url)
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

    # Execute different CQL requests, depending on the status of the chat room.
    table_name = None
    if chat_room_status == "accepted":
        table_name = "accepted_chat_rooms"
    elif chat_room_status == "completed":
        table_name = "accepted_chat_rooms"
    if table_name is not None:
        cassandra_query = """
        update
            {0}
        set
            last_message_content = {1},
            last_message_date_time = toTimestamp(now())
        where
            operator_id = {2}
        and
            channel_id = {3}
        and
            chat_room_id = {4}
        if exists;
        """.format(
            table_name,
            'null' if message_text is None or len(message_text) == 0
            else "'{0}'".format(message_text.replace("'", "''")),
            operator_id,
            channel_id,
            chat_room_id
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

    # Return each row as a dictionary after querying the Cassandra database.
    cassandra_connection.row_factory = dict_factory

    # Prepare the CQL query statement that returns the information of the created message.
    cassandra_query = '''
    select
        chat_room_id,
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
    and
        message_id = {1}
    limit 1;
    '''.format(
        chat_room_id,
        message_id
    )

    # Execute a previously prepared CQL query.
    try:
        chat_room_message_entry = cassandra_connection.execute(cassandra_query).one()
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Analyze the data about chat room message received from the database.
    chat_room_message = dict()
    if chat_room_message_entry is not None:
        quoted_message = dict()
        for key, value in chat_room_message_entry.items():
            if ("_id" in key or "_date_time" in key) and value is not None:
                value = str(value)
            if "quoted_" in key:
                quoted_message[utils.camel_case(key.replace("quoted_", ""))] = value
            else:
                chat_room_message[utils.camel_case(key)] = value
        chat_room_message["quotedMessage"] = quoted_message
        chat_room_message["channelId"] = channel_id

    # Return the object with information about created chat room message.
    return chat_room_message
