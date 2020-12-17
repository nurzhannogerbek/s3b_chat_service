import logging

# Configure the logging tool in the AWS Lambda function.
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


def lambda_handler(event, context):
    """
    :param event: The AWS Lambda function uses this parameter to pass in event data to the handler.
    :param context: The AWS Lambda function uses this parameter to provide runtime information to your handler.
    """
    # Make sure that all the necessary arguments for the AWS Lambda function are present.
    try:
        input_arguments = event["arguments"]["input"]
    except KeyError as error:
        logger.error(error)
        raise Exception(error)

    # Return the same data that came to the input.
    return input_arguments
