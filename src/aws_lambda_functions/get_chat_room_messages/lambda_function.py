import databases
import utils
import logging
import sys
import binascii
from cassandra.query import SimpleStatement, dict_factory

"""
Define the connection to the database outside of the "lambda_handler" function.
The connection to the database will be created the first time the function is called.
Any subsequent function call will use the same database connection.
"""
cassandra_connection = None

logger = logging.getLogger(__name__)  # Create the logger with the specified name.
logger.setLevel(logging.WARNING)  # Set the logging level of the logger.


def lambda_handler(event, context):
    """
    :argument event: The AWS Lambda uses this parameter to pass in event data to the handler.
    :argument context: The AWS Lambda uses this parameter to provide runtime information to your handler.
    """
    # Since the connection with database were defined outside of the function, we create global variables.
    global cassandra_connection
    if not cassandra_connection:
        try:
            cassandra_connection = databases.create_cassandra_connection()
        except Exception as error:
            logger.error(error)
            sys.exit(1)

    # Define the values of the data passed to the function.
    chat_room_id = event["arguments"]['chatRoomId']
    fetch_size = event["arguments"]['fetchSize']
    try:
        paging_state = event["arguments"]['pagingState']
    except KeyError:
        paging_state = None

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

    # Return each row as a dictionary after querying the Cassandra database.
    cassandra_connection.row_factory = dict_factory

    # Prepare the CQL query statement that returns a certain number of recent messages from a particular chat room.
    cassandra_query = '''
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
        chat_room_id = {0};
    '''.format(
        chat_room_id
    )
    statement = SimpleStatement(
        cassandra_query,
        fetch_size=fetch_size
    )

    try:
        # You can resume the pagination when executing a new query by using the "ResultSet.paging_state".
        if paging_state is not None:
            # The "unhexlify" function returns the binary data represented by the hexadecimal string "hexstr".
            chat_room_messages_entries = cassandra_connection.execute(
                statement,
                paging_state=binascii.unhexlify(paging_state)
            )
        else:
            chat_room_messages_entries = cassandra_connection.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # Get the next paging state.
    paging_state = chat_room_messages_entries.paging_state

    # Alternative form of the ternary operator in Python. Format: (expression_on_false, expression_on_true)[predicate]
    paging_state = (None, paging_state)[paging_state is not None]

    # Define the next paging state that will be sent in the response to the client.
    if paging_state is not None:
        # The "hexlify" function returns the hexadecimal representation of the binary data.
        paging_state = binascii.hexlify(paging_state).decode()
    else:
        paging_state = None

    # Initialize empty list to store information about chat room messages.
    chat_room_messages = list()

    # Analyze the list with data about chat room messages received from the database.
    for entry in chat_room_messages_entries.current_rows:
        chat_room_message = dict()
        quoted_message = dict()
        for key, value in entry.items():
            # Convert the UUID and DateTime data types to the string.
            if ("_id" in key or "_date_time" in key) and value is not None:
                value = str(value)
            if "quoted_" in key:
                quoted_message[utils.camel_case(key.replace("quoted_", ""))] = value
            else:
                chat_room_message[utils.camel_case(key)] = value
        chat_room_message["quotedMessage"] = quoted_message
        chat_room_messages.append(chat_room_message)

    # Return the object with a list of chat room messages and the next paging state.
    response = {
        "pagingState": paging_state,
        "chatRoomMessages": chat_room_messages[::-1]  # Reverse the list.
    }
    return response
