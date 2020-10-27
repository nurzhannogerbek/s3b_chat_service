import databases
import utils
import logging
import sys
import os
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement, dict_factory


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
    chat_room_id = event["arguments"]["input"]['chatRoomId']
    message_id = event["arguments"]["input"]['messageId']
    # Available values: "message_is_delivered", "message_is_read".
    message_status = event["arguments"]["input"]['messageStatus']

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

    # Prepare the CQL request that updates the status of the message.
    cassandra_query = """
    update
        chat_rooms_messages
    set
        {0} = true
    where
        chat_room_id = {1}
    and
        message_id = {2}
    if exists;
    """.format(
        message_status,
        chat_room_id,
        message_id
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

    # Prepare the CQL query statement that returns a certain number of recent messages from a particular chat room.
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

    # Return the the full information about chat room message as the response.
    return chat_room_message
