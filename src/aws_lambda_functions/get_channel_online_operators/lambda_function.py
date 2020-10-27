import databases
import utils
import logging
import sys
import os
from psycopg2.extras import RealDictCursor


"""
Define the connection to the database outside of the "lambda_handler" function.
The connection to database will be created the first time the function is called.
Any subsequent function call will use the same database connection.
"""
postgresql_connection = None

# Define databases settings parameters.
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
    # Since the connection with the database were defined outside of the function, we create global variables.
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
    channel_technical_id = event["arguments"]["channelTechnicalId"]
    channel_type_name = event["arguments"]["channelTypeName"]
    offset = event["arguments"]["currentPageNumber"]
    limit = event["arguments"]["recordsNumber"]
    offset = (offset - 1) * limit

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that returns information about operators of the specific chat room.
    statement = """
    select
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
        channels_organizations_relationship
    left join channels on
        channels_organizations_relationship.channel_id = channels.channel_id
    left join channel_types on
        channels.channel_type_id = channel_types.channel_type_id
    left join organizations on
        channels_organizations_relationship.organization_id = organizations.organization_id
    left join internal_users on
        organizations.organization_id = internal_users.organization_id
    left join users on
        internal_users.internal_user_id = users.internal_user_id
    left join genders on
        internal_users.gender_id = genders.gender_id
    left join countries on
        internal_users.country_id = countries.country_id
    left join roles on
        internal_users.role_id = roles.role_id
    where
        channels.channel_technical_id = '{0}'
    and
        channel_types.channel_type_name = '{1}'
    and
        users.internal_user_id is not null
    and
        users.identified_user_id is null
    and
        users.unidentified_user_id is null
    offset {2} limit {3};
    """.format(
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
    operators_entries = cursor.fetchall()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Analyze the data about client received from the database.
    operators = list()
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
                else:
                    operator[utils.camel_case(key)] = value
            operator["gender"] = gender
            operator["country"] = country
            operator["role"] = role
            operator["organization"] = organization
            operators.append(operator)

    # Return the list of online operators as the response.
    return operators
