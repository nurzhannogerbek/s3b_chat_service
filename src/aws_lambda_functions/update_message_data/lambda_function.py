import logging
import os
from psycopg2.extras import RealDictCursor
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
        input_arguments = kwargs["event"]["arguments"]["input"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        queue = kwargs["queue"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Check the format and values of required arguments in the list of input arguments.
    required_arguments = ["chatRoomId", "messagesIds", "messageStatus", "isClient"]
    for argument_name, argument_value in input_arguments.items():
        if argument_name not in required_arguments:
            raise Exception("The '{0}' argument doesn't exist.".format(argument_name))
        if argument_value is None:
            raise Exception("The '{0}' argument can't be None/Null/Undefined.".format(argument_name))
        if argument_name.endswith("Id"):
            try:
                uuid.UUID(argument_value)
            except ValueError:
                raise Exception("The '{0}' argument format is not UUID.".format(argument_name))

    # Put the result of the function in the queue.
    queue.put({
        "input_arguments": {
            "chat_room_id": input_arguments.get("chatRoomId", None),
            "messages_ids": input_arguments.get("messagesIds", None),
            "message_status": input_arguments.get("messageStatus", None),
            "is_client": input_arguments.get("isClient", None)
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
        raise Exception("Processing of a chat room with the status '{0}' is not possible.".format(chat_room_status))

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Return the chat room status.
    return cursor.fetchone()


def update_unread_messages_number(**kwargs) -> int:
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
        is_client = cql_arguments["is_client"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        organizations_ids = cql_arguments["organizations_ids"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        messages_number = cql_arguments["messages_number"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    if chat_room_status == "non_accepted":
        # Prepare the CQL request to get the number of unread messages.
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

        # Increase/Decrease the number of unread messages.
        if is_client:
            unread_messages_number += messages_number
        else:
            unread_messages_number -= messages_number
            unread_messages_number = 0 if unread_messages_number < 0 else unread_messages_number

        # Add the value of the number of unread messages to the CQL argument dictionary.
        cql_arguments["unread_messages_number"] = unread_messages_number

        # Prepare the CQL request to update the number of unread messages.
        cql_statement = """
        update
            non_accepted_chat_rooms
        set
            unread_messages_number = %(unread_messages_number)s
        where
            organization_id = %(organization_id)s
        and
            channel_id = %(channel_id)s
        and
            chat_room_id = %(chat_room_id)s
        if exists;
        """

        # For each organization that can serve the chat room, we update the record in the database..
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
        # Prepare the CQL request to get the number of unread messages.
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

        # Increase/Decrease the number of unread messages.
        if is_client:
            unread_messages_number += messages_number
        else:
            unread_messages_number -= messages_number
            unread_messages_number = 0 if unread_messages_number < 0 else unread_messages_number

        # Add the value of the number of unread messages to the CQL argument dictionary.
        cql_arguments["unread_messages_number"] = unread_messages_number

        # Prepare the CQL request to update the number of unread messages.
        cql_statement = """
        update
            accepted_chat_rooms
        set
            unread_messages_number = %(unread_messages_number)s
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

    # Return the unread messages number.
    return unread_messages_number


def get_unread_messages_number(**kwargs) -> int:
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

    if chat_room_status == "non_accepted":
        # Prepare the CQL request to get the number of unread messages.
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
    elif chat_room_status == "accepted":
        # Prepare the CQL request to get the number of unread messages.
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
    else:
        raise Exception("Processing of a chat room with the status '{0}' is not possible.".format(chat_room_status))

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

    # Return the unread messages number.
    return unread_messages_number


def update_chat_room_messages_statuses(**kwargs) -> None:
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
        column_name = cql_arguments["column_name"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)
    try:
        messages_ids = cql_arguments["messages_ids"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Prepare the CQL query that updates the status of the message.
    cql_statement = """
    update
        chat_rooms_messages
    set
        {0} = true
    where
        chat_room_id = %(chat_room_id)s
    and
        message_id = %(message_id)s
    if exists;
    """.format(column_name.lower())

    # Update the status of each message individually.
    for message_id in messages_ids:
        # Add or update the value of the argument.
        cql_arguments["message_id"] = uuid.UUID(message_id, version=1)

        # Execute the CQL query dynamically, in a convenient and safe way.
        try:
            cassandra_connection.execute(cql_statement, cql_arguments)
        except Exception as error:
            logger.error(error)
            raise Exception(error)

    # Return nothing.
    return None


def get_chat_room_messages(**kwargs) -> List[Dict[AnyStr, Any]]:
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
        messages_ids = cql_arguments["messages_ids"]
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
        quoted_message_type
    from
        chat_rooms_messages
    where
        chat_room_id = %(chat_room_id)s
    and
        message_id = %(message_id)s
    limit 1;
    """

    # Define the empty list to store information about messages.
    chat_room_messages = []

    # Get information of each message individually.
    for message_id in messages_ids:
        # Add or update the value of the argument.
        cql_arguments["message_id"] = uuid.UUID(message_id, version=1)

        # Execute the CQL query dynamically, in a convenient and safe way.
        try:
            chat_room_message = cassandra_connection.execute(cql_statement, cql_arguments).one()
        except Exception as error:
            logger.error(error)
            raise Exception(error)

        # Add the message information to the list
        chat_room_messages.append(chat_room_message)

    # Return the list of chat room messages.
    return chat_room_messages


def analyze_and_format_chat_room_messages(**kwargs) -> List[Dict[AnyStr, Any]]:
    # Check if the input dictionary has all the necessary keys.
    try:
        chat_room_messages = kwargs["chat_room_messages"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Format the last message data.
    messages = []
    for chat_room_message in chat_room_messages:
        message = {}
        quoted_message = {}
        for key, value in chat_room_message.items():
            if key.endswith("_date_time") and value is not None:
                value = value.isoformat()
            elif key.endswith("_id") and value is not None:
                value = str(value)
            if key.startswith("quoted_"):
                quoted_message[utils.camel_case(key.replace("quoted_", ""))] = value
            else:
                message[utils.camel_case(key)] = value
        message["quotedMessage"] = quoted_message
        messages.append(message)

    # Return analyzed and formatted messages.
    return messages


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

    # Define the input arguments of the AWS Lambda function.
    input_arguments = results_of_tasks["input_arguments"]
    chat_room_id = input_arguments["chat_room_id"]
    messages_ids = input_arguments["messages_ids"]
    # Available values: "MESSAGE_IS_DELIVERED", "MESSAGE_IS_READ", "MESSAGE_IS_SENT".
    message_status = input_arguments["message_status"]
    is_client = input_arguments["is_client"]

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
                global POSTGRESQL_CONNECTION
                POSTGRESQL_CONNECTION = cassandra_connection
            except Exception as error:
                logger.error(error)
                raise Exception(error)

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
    channel_id = uuid.UUID(aggregated_data["channel_id"])
    operator_id = uuid.UUID(aggregated_data["operator_id"]) if aggregated_data["operator_id"] else None
    organizations_ids = aggregated_data["organizations_ids"]

    # Check who the initiator of the request.
    if is_client and message_status == "MESSAGE_IS_SENT" or not is_client and message_status == "MESSAGE_IS_READ":
        unread_messages_number = update_unread_messages_number(
            cassandra_connection=cassandra_connection,
            cql_arguments={
                "chat_room_status": chat_room_status,
                "is_client": is_client,
                "chat_room_id": uuid.UUID(chat_room_id),
                "channel_id": channel_id,
                "operator_id": operator_id,
                "organizations_ids": organizations_ids,
                "messages_number": len(messages_ids)
            }
        )
    else:
        unread_messages_number = get_unread_messages_number(
            cassandra_connection=cassandra_connection,
            cql_arguments={
                "chat_room_status": chat_room_status,
                "chat_room_id": uuid.UUID(chat_room_id),
                "channel_id": channel_id,
                "operator_id": operator_id,
                "organization_id": uuid.UUID(organizations_ids[0])
            }
        )

    # Update the statuses of different messages.
    update_chat_room_messages_statuses(
        cassandra_connection=cassandra_connection,
        cql_arguments={
            "column_name": message_status,
            "chat_room_id": uuid.UUID(chat_room_id),
            "messages_ids": messages_ids
        }
    )

    # Define the variable that stores information about messages.
    chat_room_messages = get_chat_room_messages(
        cassandra_connection=cassandra_connection,
        cql_arguments={
            "chat_room_id": uuid.UUID(chat_room_id),
            "messages_ids": messages_ids
        }
    )

    # Define variables that stores formatted information about messages.
    chat_room_messages = analyze_and_format_chat_room_messages(chat_room_messages=chat_room_messages)

    # Create the response structure and return it.
    return {
        "chatRoomId": chat_room_id,
        "chatRoomStatus": chat_room_status,
        "chatRoomMessages": chat_room_messages,
        "unreadMessagesNumber": unread_messages_number
    }
