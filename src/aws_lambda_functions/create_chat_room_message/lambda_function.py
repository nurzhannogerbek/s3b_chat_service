import logging
import os
from psycopg2.extras import RealDictCursor
from functools import wraps
from typing import *
import uuid
from threading import Thread
from queue import Queue
import json
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
    message_text = input_arguments.get("messageText", None)
    message_content = input_arguments.get("messageContent", None)
    try:
        quoted_message_id = input_arguments["quotedMessage"]["messageId"]
    except Exception:
        quoted_message_id = None
    if quoted_message_id is not None:
        try:
            uuid.UUID(quoted_message_id)
        except ValueError:
            raise Exception("The 'quotedMessageId' argument format is not UUID.")
    try:
        quoted_message_author_id = input_arguments["quotedMessage"]["messageAuthorId"]
    except Exception:
        quoted_message_author_id = None
    if quoted_message_author_id is not None:
        try:
            uuid.UUID(quoted_message_author_id)
        except ValueError:
            raise Exception("The 'quotedMessageAuthorId' argument format is not UUID.")
    try:
        quoted_message_channel_id = input_arguments["quotedMessage"]["messageChannelId"]
    except Exception:
        quoted_message_channel_id = None
    if quoted_message_channel_id is not None:
        try:
            uuid.UUID(quoted_message_channel_id)
        except ValueError:
            raise Exception("The 'quotedMessageChannelId' argument format is not UUID.")
    try:
        quoted_message_text = input_arguments["quotedMessage"]["messageText"]
    except Exception:
        quoted_message_text = None
    try:
        quoted_message_content = input_arguments["quotedMessage"]["messageContent"]
    except Exception:
        quoted_message_content = None
    local_message_id = input_arguments.get("localMessageId", None)
    is_client = input_arguments.get("isClient", None)

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "chat_room_id": chat_room_id,
            "message_author_id": message_author_id,
            "message_channel_id": message_channel_id,
            "message_text": message_text,
            "message_content": message_content,
            "quoted_message_id": quoted_message_id,
            "quoted_message_author_id": quoted_message_author_id,
            "quoted_message_channel_id": quoted_message_channel_id,
            "quoted_message_text": quoted_message_text,
            "quoted_message_content": quoted_message_content,
            "local_message_id": local_message_id,
            "is_client": is_client
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
            channel_types.channel_type_name::text,
            null as operator_id,
            array_agg(distinct channels_organizations_relationship.organization_id)::text[] as organizations_ids
        from
            chat_rooms
        left join channels_organizations_relationship on
            chat_rooms.channel_id = channels_organizations_relationship.channel_id
        left join channels on
            chat_rooms.channel_id = channels.channel_id
        left join channel_types on
            channels.channel_type_id = channel_types.channel_type_id
        where
            chat_rooms.chat_room_id = %(chat_room_id)s
        and
            chat_rooms.chat_room_status = 'non_accepted'
        group by
            chat_rooms.channel_id,
            channel_types.channel_type_name
        limit 1;
        """
    elif chat_room_status == "accepted":
        sql_statement = """
        select
            chat_rooms.channel_id::text,
            channel_types.channel_type_name::text,
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
        left join channels on
            chat_rooms.channel_id = channels.channel_id
        left join channel_types on
            channels.channel_type_id = channel_types.channel_type_id
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
            channel_types.channel_type_name,
            users.user_id,
            chat_rooms_users_relationship.entry_created_date_time
        order by
            chat_rooms_users_relationship.entry_created_date_time desc
        limit 1;
        """
    else:
        raise Exception("Processing of a chat room with the status '{0}' is not possible.".format(chat_room_status))

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
        message_content,
        message_created_date_time,
        message_deleted_date_time,
        message_is_delivered,
        message_is_read,
        message_is_sent,
        message_text,
        message_updated_date_time,
        quoted_message_author_id,
        quoted_message_channel_id,
        quoted_message_content,
        quoted_message_id,
        quoted_message_text
    ) values (
        %(chat_room_id)s,
        %(message_id)s,
        %(message_author_id)s,
        %(message_channel_id)s,
        %(message_content)s,
        toTimestamp(now()),
        null,
        false,
        false,
        true,
        %(message_text)s,
        toTimestamp(now()),
        %(quoted_message_author_id)s,
        %(quoted_message_channel_id)s,
        %(quoted_message_content)s,
        %(quoted_message_id)s,
        %(quoted_message_text)s
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
        organizations_ids = cql_arguments["organizations_ids"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        is_client: object = cql_arguments["is_client"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Check the status value of the chat room.
    if chat_room_status == "non_accepted":
        # Prepare a request to update the contents of the last message.
        cql_statement = f"""
        update
            non_accepted_chat_rooms
        set
            {"last_message_from_client_date_time = toTimestamp(now())," if is_client else ""}
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
        # Prepare a request to update the contents of the last message.
        cql_statement = f"""
        update
            accepted_chat_rooms
        set
            {"last_message_from_client_date_time = toTimestamp(now())," if is_client else ""}
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
        raise Exception("Processing of a chat room with the status '{0}' is not possible.".format(chat_room_status))

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
        message_content,
        message_created_date_time,
        message_deleted_date_time,
        message_is_delivered,
        message_is_read,
        message_is_sent,
        message_text,
        message_updated_date_time,
        quoted_message_author_id,
        quoted_message_channel_id,
        quoted_message_content,
        quoted_message_id,
        quoted_message_text
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
        if key.endswith("_date_time") and value is not None:
            value = value.isoformat()
        elif key.endswith("_id") and value is not None:
            value = str(value)
        if key.startswith("quoted_"):
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
                global CASSANDRA_CONNECTION
                CASSANDRA_CONNECTION = cassandra_connection
            except Exception as error:
                logger.error(error)
                raise Exception(error)

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
    channel_type_name = aggregated_data["channel_type_name"]
    message_id = uuid.uuid1()
    chat_room_id = uuid.UUID(input_arguments["chat_room_id"])
    message_author_id = uuid.UUID(input_arguments["message_author_id"])
    message_channel_id = uuid.UUID(input_arguments["message_channel_id"])
    message_text = input_arguments["message_text"]
    message_content = input_arguments["message_content"]
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
    quoted_message_text = input_arguments["quoted_message_text"]
    quoted_message_content = input_arguments["quoted_message_content"]
    local_message_id = input_arguments["local_message_id"]
    last_message_content = {
        "messageText": message_text,
        "messageContent": None if message_content is None else json.loads(message_content)
    }
    is_client = input_arguments["is_client"]

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
                    "message_content": message_content,
                    "message_text": message_text,
                    "quoted_message_author_id": quoted_message_author_id,
                    "quoted_message_channel_id": quoted_message_channel_id,
                    "quoted_message_content": quoted_message_content,
                    "quoted_message_id": quoted_message_id,
                    "quoted_message_text": quoted_message_text
                }
            }
        },
        {
            "function_object": update_last_message_content_data,
            "function_arguments": {
                "cassandra_connection": cassandra_connection,
                "cql_arguments": {
                    "chat_room_status": chat_room_status,
                    "organizations_ids": organizations_ids,
                    "chat_room_id": chat_room_id,
                    "operator_id": operator_id,
                    "channel_id": uuid.UUID(channel_id),
                    "last_message_content": json.dumps(last_message_content),
                    "is_client": is_client
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

    # Return the channel type name to the frontend.
    chat_room_message["channelTypeName"] = channel_type_name

    # Return the chat room status to the frontend.
    chat_room_message["chatRoomStatus"] = chat_room_status

    # Return data of the created chat room message.
    return chat_room_message
