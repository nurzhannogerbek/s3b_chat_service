import logging
import os
from psycopg2.extras import RealDictCursor
from cassandra.cluster import Session
from functools import wraps
from typing import *
import uuid
from threading import Thread
from queue import Queue
import databases
import utils

# Configure the logging tool in the AWS Lambda function.
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# Initialize constants with parameters to configure.
POSTGRESQL_USERNAME = os.environ["POSTGRESQL_USERNAME"]
POSTGRESQL_PASSWORD = os.environ["POSTGRESQL_PASSWORD"]
POSTGRESQL_HOST = os.environ["POSTGRESQL_HOST"]
POSTGRESQL_PORT = int(os.environ["POSTGRESQL_PORT"])
POSTGRESQL_DB_NAME = os.environ["POSTGRESQL_DB_NAME"]
CASSANDRA_USERNAME = os.environ["CASSANDRA_USERNAME"]
CASSANDRA_PASSWORD = os.environ["CASSANDRA_PASSWORD"]
CASSANDRA_HOST = os.environ["CASSANDRA_HOST"].split(",")
CASSANDRA_PORT = int(os.environ["CASSANDRA_PORT"])
CASSANDRA_LOCAL_DC = os.environ["CASSANDRA_LOCAL_DC"]
CASSANDRA_KEYSPACE_NAME = os.environ["CASSANDRA_KEYSPACE_NAME"]

# The connection to the database will be created the first time the AWS Lambda function is called.
# Any subsequent call to the function will use the same database connection until the container stops.
POSTGRESQL_CONNECTION = None
CASSANDRA_CONNECTION = None


def run_multithreading_tasks(functions: List[Dict[AnyStr, Union[Callable, Dict[AnyStr, Any]]]]) -> Dict[AnyStr, Any]:
    # Create the empty list to save all parallel threads.
    threads = []

    # Create the queue to store all results of functions.
    queue = Queue()

    # Create the thread for each function.
    for function in functions:
        # Check whether the input arguments have keys in their dictionaries.
        try:
            function_object = function["function_object"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)
        try:
            function_arguments = function["function_arguments"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)

        # Add the instance of the queue to the list of function arguments.
        function_arguments["queue"] = queue

        # Create the thread.
        thread = Thread(target=function_object, kwargs=function_arguments)
        threads.append(thread)

    # Start all parallel threads.
    for thread in threads:
        thread.start()

    # Wait until all parallel threads are finished.
    for thread in threads:
        thread.join()

    # Get the results of all threads.
    results = {}
    while not queue.empty():
        results = {**results, **queue.get()}

    # Return the results of all threads.
    return results


def check_input_arguments(**kwargs) -> None:
    # Make sure that all the necessary arguments for the AWS Lambda function are present.
    try:
        queue = kwargs["queue"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        input_arguments = kwargs["event"]["arguments"]["input"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Check the format and values of required arguments.
    chat_room_id = input_arguments.get("chatRoomId", None)
    if chat_room_id is not None:
        try:
            uuid.UUID(chat_room_id)
        except ValueError:
            raise Exception("The 'chatRoomId' argument format is not UUID.")
    else:
        raise Exception("The 'chatRoomId' argument can't be None/Null/Undefined.")
    message_author_id = input_arguments.get("messageAuthorId", None)
    if message_author_id is not None:
        try:
            uuid.UUID(message_author_id)
        except ValueError:
            raise Exception("The 'messageAuthorId' argument format is not UUID.")
    else:
        raise Exception("The 'messageAuthorId' argument can't be None/Null/Undefined.")
    message_channel_id = input_arguments.get("messageChannelId", None)
    if message_channel_id is not None:
        try:
            uuid.UUID(message_channel_id)
        except ValueError:
            raise Exception("The 'messageChannelId' argument format is not UUID.")
    else:
        raise Exception("The 'messageChannelId' argument can't be None/Null/Undefined.")
    message_type = input_arguments.get("messageType", None)
    if message_type is None:
        raise Exception("The 'messageType' argument can't be None/Null/Undefined.")
    message_text = input_arguments.get("messageText", None)
    message_content_url = input_arguments.get("messageContentUrl", None)
    try:
        quoted_message_id = input_arguments["quotedMessage"]["messageId"]
    except KeyError:
        quoted_message_id = None
    if quoted_message_id is not None:
        try:
            uuid.UUID(quoted_message_id)
        except ValueError:
            raise Exception("The 'quotedMessageId' argument format is not UUID.")
    try:
        quoted_message_author_id = input_arguments["quotedMessage"]["messageAuthorId"]
    except KeyError:
        quoted_message_author_id = None
    if quoted_message_author_id is not None:
        try:
            uuid.UUID(quoted_message_author_id)
        except ValueError:
            raise Exception("The 'quotedMessageAuthorId' argument format is not UUID.")
    try:
        quoted_message_channel_id = input_arguments["quotedMessage"]["messageChannelId"]
    except KeyError:
        quoted_message_channel_id = None
    if quoted_message_channel_id is not None:
        try:
            uuid.UUID(quoted_message_channel_id)
        except ValueError:
            raise Exception("The 'quotedMessageChannelId' argument format is not UUID.")
    try:
        quoted_message_type = input_arguments["quotedMessage"]["messageType"]
    except KeyError:
        quoted_message_type = None
    try:
        quoted_message_text = input_arguments["quotedMessage"]["messageText"]
    except KeyError:
        quoted_message_text = None
    try:
        quoted_message_content_url = input_arguments["quotedMessage"]["messageContentUrl"]
    except KeyError:
        quoted_message_content_url = None
    local_message_id = input_arguments.get("localMessageId", None)
    increase_unread_messages_number = input_arguments.get("increaseUnreadMessagesNumber", None)
    if increase_unread_messages_number is None:
        raise Exception("The 'increaseUnreadMessagesNumber' argument can't be None/Null/Undefined.")

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "chat_room_id": chat_room_id,
            "message_author_id": message_author_id,
            "message_channel_id": message_channel_id,
            "message_type": message_type,
            "message_text": message_text,
            "message_content_url": message_content_url,
            "quoted_message_id": quoted_message_id,
            "quoted_message_author_id": quoted_message_author_id,
            "quoted_message_channel_id": quoted_message_channel_id,
            "quoted_message_type": quoted_message_type,
            "quoted_message_text": quoted_message_text,
            "quoted_message_content_url": quoted_message_content_url,
            "local_message_id": local_message_id,
            "increase_unread_messages_number": increase_unread_messages_number
        }
    })

    # Return nothing.
    return None


def reuse_or_recreate_postgresql_connection(queue: Queue) -> None:
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
    queue.put({"postgresql_connection": POSTGRESQL_CONNECTION})
    return None


def reuse_or_recreate_cassandra_connection(queue: Queue) -> None:
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
    queue.put({"cassandra_connection": CASSANDRA_CONNECTION})
    return None


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
def get_chat_room_status(**kwargs) -> AnyStr:
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

    # Prepare the SQL query that returns the chat room status.
    sql_statement = """
    select
        chat_rooms.chat_room_status::text
    from
        chat_rooms
    where
        chat_rooms.chat_room_id = %(chat_room_id)s;
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the chat room status.
    return cursor.fetchone()["chat_room_status"]


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
    try:
        chat_room_status = sql_arguments["chat_room_status"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    if chat_room_status == "non_accepted":
        sql_statement = """
        select
            chat_rooms.channel_id::text,
            null as operator_id,
            array_agg(distinct channels_organizations_relationship.organization_id)::text[] as organizations_ids
        from
            chat_rooms
        left join channels_organizations_relationship on
            chat_rooms.channel_id = channels_organizations_relationship.channel_id
        where
            chat_rooms.chat_room_id = %(chat_room_id)s
        and
            chat_rooms.chat_room_status = 'non_accepted'
        group by
            chat_rooms.channel_id
        limit 1;
        """
    elif chat_room_status == "accepted":
        sql_statement = """
        select
            chat_rooms.channel_id::text,
            users.user_id::text as operator_id,
            array_agg(distinct channels_organizations_relationship.organization_id)::text[] as organizations_ids
        from
            chat_rooms
        left join chat_rooms_users_relationship on
            chat_rooms.chat_room_id = chat_rooms_users_relationship.chat_room_id
        left join users on
            chat_rooms_users_relationship.user_id = users.user_id
        left join channels_organizations_relationship on
            chat_rooms.channel_id = channels_organizations_relationship.channel_id
        where
            chat_rooms.chat_room_id = %(chat_room_id)s
        and
            chat_rooms.chat_room_status = 'accepted'
        and
            users.internal_user_id is not null
        and
            users.identified_user_id is null
        and
            users.unidentified_user_id is null
        group by
            chat_rooms.channel_id,
            users.user_id,
            chat_rooms_users_relationship.entry_created_date_time
        order by
            chat_rooms_users_relationship.entry_created_date_time desc
        limit 1;
        """
    else:
        raise Exception("Processing of a chat room with the status '%s' is not possible.".format(chat_room_status))

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the chat room status.
    return cursor.fetchone()


def create_chat_room_message(**kwargs) -> None:
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

    # Prepare the CQL query that creates a chat room message.
    cql_statement = """
    insert into chat_rooms_messages (
        chat_room_id,
        message_id,
        message_author_id,
        message_channel_id,
        message_content_url,
        message_created_date_time,
        message_deleted_date_time,
        message_is_delivered,
        message_is_read,
        message_is_sent,
        message_text,
        message_type,
        message_updated_date_time,
        quoted_message_author_id,
        quoted_message_channel_id,
        quoted_message_content_url,
        quoted_message_id,
        quoted_message_text,
        quoted_message_type
    ) values (
        %(chat_room_id)s,
        %(message_id)s,
        %(message_author_id)s,
        %(message_channel_id)s,
        %(message_content_url)s,
        toTimestamp(now()),
        null,
        false,
        false,
        true,
        %(message_text)s,
        %(message_type)s,
        toTimestamp(now()),
        %(quoted_message_author_id)s,
        %(quoted_message_channel_id)s,
        %(quoted_message_content_url)s,
        %(quoted_message_id)s,
        %(quoted_message_text)s,
        %(quoted_message_type)s
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


def update_last_message_content_data(**kwargs) -> None:
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
        chat_room_status = cql_arguments["chat_room_status"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        increase_unread_messages_number = cql_arguments["increase_unread_messages_number"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        organizations_ids = cql_arguments["organizations_ids"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Check the status value of the chat room.
    if chat_room_status == "non_accepted":
        # Check whether you need to increase the counter of unread messages.
        if increase_unread_messages_number:
            # Prepare a request to get the number of unread messages.
            cql_statement = """
            select
                unread_messages_number
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

            # Add or update the organization id value inside the dictionary of cql arguments.
            cql_arguments["organization_id"] = uuid.UUID(organizations_ids[0])

            # Execute the CQL query dynamically, in a convenient and safe way.
            try:
                unread_messages_number = cassandra_connection.execute(
                    cql_statement,
                    cql_arguments
                ).one()["unread_messages_number"]
            except Exception as error:
                logger.error(error)
                raise Exception(error)

            # Check the data type of the variable.
            if isinstance(unread_messages_number, type(None)):
                unread_messages_number = 0

            # Increase the number of unread messages.
            unread_messages_number += 1

            # Add the value of the number of unread messages to the CQL argument dictionary.
            cql_arguments["unread_messages_number"] = unread_messages_number

            # Prepare a request to update the contents of the last message.
            cql_statement = """
            update
                non_accepted_chat_rooms
            set
                last_message_content = %(last_message_content)s,
                last_message_date_time = toTimestamp(now()),
                unread_messages_number = %(unread_messages_number)s
            where
                organization_id = %(organization_id)s
            and
                channel_id = %(channel_id)s
            and
                chat_room_id = %(chat_room_id)s
            if exists;
            """
        else:
            # Prepare a request to update the contents of the last message.
            cql_statement = """
            update
                non_accepted_chat_rooms
            set
                last_message_content = %(last_message_content)s,
                last_message_date_time = toTimestamp(now())
            where
                organization_id = %(organization_id)s
            and
                channel_id = %(channel_id)s
            and
                chat_room_id = %(chat_room_id)s
            if exists;
            """

        # For each organization that can serve the chat room, we create an entry in the database.
        for organization_id in organizations_ids:
            # Add or update the value of the argument.
            cql_arguments["organization_id"] = uuid.UUID(organization_id)

            # Execute the CQL query dynamically, in a convenient and safe way.
            try:
                cassandra_connection.execute(cql_statement, cql_arguments)
            except Exception as error:
                logger.error(error)
                raise Exception(error)
    elif chat_room_status == "accepted":
        # Check whether you need to increase the counter of unread messages.
        if increase_unread_messages_number:
            # Prepare a request to get the number of unread messages.
            cql_statement = """
            select
                unread_messages_number
            from
                accepted_chat_rooms
            where
                operator_id = %(operator_id)s
            and
                channel_id = %(channel_id)s
            and
                chat_room_id = %(chat_room_id)s
            limit 1;
            """

            # Execute the CQL query dynamically, in a convenient and safe way.
            try:
                unread_messages_number = cassandra_connection.execute(
                    cql_statement,
                    cql_arguments
                ).one()["unread_messages_number"]
            except Exception as error:
                logger.error(error)
                raise Exception(error)

            # Check the data type of the variable.
            if isinstance(unread_messages_number, type(None)):
                unread_messages_number = 0

            # Increase the number of unread messages.
            unread_messages_number += 1

            # Add the value of the number of unread messages to the CQL argument dictionary.
            cql_arguments["unread_messages_number"] = unread_messages_number

            # Prepare a request to update the contents of the last message.
            cql_statement = """
            update
                non_accepted_chat_rooms
            set
                last_message_content = %(last_message_content)s,
                last_message_date_time = toTimestamp(now()),
                unread_messages_number = %(unread_messages_number)s
            where
                operator_id = %(operator_id)s
            and
                channel_id = %(channel_id)s
            and
                chat_room_id = %(chat_room_id)s
            if exists;
            """
        else:
            # Prepare a request to update the contents of the last message.
            cql_statement = """
            update
                non_accepted_chat_rooms
            set
                last_message_content = %(last_message_content)s,
                last_message_date_time = toTimestamp(now())
            where
                operator_id = %(operator_id)s
            and
                channel_id = %(channel_id)s
            and
                chat_room_id = %(chat_room_id)s
            if exists;
            """

        # Execute the CQL query dynamically, in a convenient and safe way.
        try:
            cassandra_connection.execute(cql_statement, cql_arguments)
        except Exception as error:
            logger.error(error)
            raise Exception(error)
    else:
        raise Exception("Processing of a chat room with the status '%s' is not possible.".format(chat_room_status))

    # Return nothing.
    return None


def get_chat_room_message_data(**kwargs) -> Dict[AnyStr, Any]:
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

    # Prepare the CQL query that gets data the last chat room message.
    cql_statement = """
    select
        chat_room_id,
        message_id,
        message_author_id,
        message_channel_id,
        message_content_url,
        message_created_date_time,
        message_deleted_date_time,
        message_is_delivered,
        message_is_read,
        message_is_sent,
        message_text,
        message_type,
        message_updated_date_time,
        quoted_message_author_id,
        quoted_message_channel_id,
        quoted_message_content_url,
        quoted_message_id,
        quoted_message_text,
        quoted_message_type,
    from
        chat_rooms_messages
    where
        chat_room_id = %(chat_room_id)s
    and
        message_id = %(message_id)s
    limit 1;
    """

    # Execute the CQL query dynamically, in a convenient and safe way.
    try:
        chat_room_message_data = cassandra_connection.execute(cql_statement, cql_arguments).one()
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return data of the last chat room message.
    return chat_room_message_data


def analyze_and_format_chat_room_message_data(**kwargs) -> Dict[AnyStr, Any]:
    # Check if the input dictionary has all the necessary keys.
    try:
        chat_room_message_data = kwargs["chat_room_message_data"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the last message data.
    chat_room_message = {}
    quoted_message = {}
    for key, value in chat_room_message_data.items():
        if key.endswith["_date_time"] and value is not None:
            value = value.isoformat()
        elif key.endswith["_id"] and value is not None:
            value = str(value)
        if key.startswith["quoted_"]:
            quoted_message[utils.camel_case(key.replace("quoted_", ""))] = value
        else:
            chat_room_message[utils.camel_case(key)] = value
    chat_room_message["quotedMessage"] = quoted_message

    # Return analyzed and formatted chat room message data.
    return chat_room_message


def lambda_handler(event, context):
    """
    :param event: The AWS Lambda function uses this parameter to pass in event data to the handler.
    :param context: The AWS Lambda function uses this parameter to provide runtime information to your handler.
    """
    # Run several initialization functions in parallel.
    results_of_tasks = run_multithreading_tasks([
        {
            "function_object": check_input_arguments,
            "function_arguments": {
                "event": event
            }
        },
        {
            "function_object": reuse_or_recreate_postgresql_connection,
            "function_arguments": {}
        },
        {
            "function_object": reuse_or_recreate_cassandra_connection,
            "function_arguments": {}
        }
    ])

    # Define the instances of the database connections.
    postgresql_connection = results_of_tasks["postgresql_connection"]
    cassandra_connection = results_of_tasks["cassandra_connection"]
    set_cassandra_keyspace(cassandra_connection=cassandra_connection)

    # Define the input arguments of the AWS Lambda function.
    input_arguments = results_of_tasks["input_arguments"]
    chat_room_id = input_arguments["chat_room_id"]

    # Define the chat room status.
    chat_room_status = get_chat_room_status(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "chat_room_id": chat_room_id
        }
    )

    # Define the variable that stores information about aggregated data.
    aggregated_data = get_aggregated_data(
        postgresql_connection=postgresql_connection,
        sql_arguments={
            "chat_room_id": chat_room_id,
            "chat_room_status": chat_room_status
        }
    )

    # Define a few necessary variables that will be used in the future.
    channel_id = aggregated_data["channel_id"]
    if aggregated_data["operator_id"]:
        operator_id = uuid.UUID(aggregated_data["operator_id"])
    else:
        operator_id = None
    organizations_ids = aggregated_data["organizations_ids"]
    message_id = uuid.uuid1()
    chat_room_id = uuid.UUID(input_arguments["chat_room_id"])
    message_author_id = uuid.UUID(input_arguments["message_author_id"])
    message_channel_id = uuid.UUID(input_arguments["message_channel_id"])
    message_type = input_arguments["message_type"]
    message_text = input_arguments["message_text"]
    message_content_url = input_arguments["message_content_url"]
    if input_arguments["quoted_message_id"]:
        quoted_message_id = uuid.UUID(input_arguments["quoted_message_id"])
    else:
        quoted_message_id = None
    if input_arguments["quoted_message_author_id"]:
        quoted_message_author_id = uuid.UUID(input_arguments["quoted_message_author_id"])
    else:
        quoted_message_author_id = None
    if input_arguments["quoted_message_channel_id"]:
        quoted_message_channel_id = uuid.UUID(input_arguments["quoted_message_channel_id"])
    else:
        quoted_message_channel_id = None
    quoted_message_type = input_arguments["quoted_message_type"]
    quoted_message_text = input_arguments["quoted_message_text"]
    quoted_message_content_url = input_arguments["quoted_message_content_url"]
    local_message_id = input_arguments["local_message_id"]
    increase_unread_messages_number = input_arguments["increase_unread_messages_number"]
    last_message_content = message_text

    # Run several functions in parallel to create/update/delete all necessary data in different databases tables.
    run_multithreading_tasks([
        {
            "function_object": create_chat_room_message,
            "function_arguments": {
                "cassandra_connection": cassandra_connection,
                "cql_arguments": {
                    "chat_room_id": chat_room_id,
                    "message_id": message_id,
                    "message_author_id": message_author_id,
                    "message_channel_id": message_channel_id,
                    "message_content_url": message_content_url,
                    "message_text": message_text,
                    "message_type": message_type,
                    "quoted_message_author_id": quoted_message_author_id,
                    "quoted_message_channel_id": quoted_message_channel_id,
                    "quoted_message_content_url": quoted_message_content_url,
                    "quoted_message_id": quoted_message_id,
                    "quoted_message_text": quoted_message_text,
                    "quoted_message_type": quoted_message_type
                }
            }
        },
        {
            "function_object": update_last_message_content_data,
            "function_arguments": {
                "cassandra_connection": cassandra_connection,
                "cql_arguments": {
                    "chat_room_status": chat_room_status,
                    "increase_unread_messages_number": increase_unread_messages_number,
                    "organizations_ids": organizations_ids,
                    "chat_room_id": chat_room_id,
                    "operator_id": operator_id,
                    "channel_id": uuid.UUID(channel_id),
                    "last_message_content": last_message_content
                }
            }
        }
    ])

    # Define the variable that stores information about the last chat room message data.
    chat_room_message_data = get_chat_room_message_data(
        cassandra_connection=cassandra_connection,
        cql_arguments={
            "chat_room_id": chat_room_id,
            "message_id": message_id
        }
    )

    # Define variables that stores formatted information about the chat room message.
    chat_room_message = analyze_and_format_chat_room_message_data(chat_room_message_data=chat_room_message_data)

    # Return the local message id that was created on the frontend for quick widget operation.
    chat_room_message["localMessageId"] = local_message_id

    # Send the channel id so that the subscription works correctly on the frontend.
    chat_room_message["channelId"] = channel_id

    # Return data of the created chat room message.
    return chat_room_message
