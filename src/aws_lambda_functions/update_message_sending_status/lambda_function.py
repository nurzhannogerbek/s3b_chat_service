"""
This function is intended for a subscription called "watchUpdateMessageSendingStatus" in the GraphQL schema.
The subscription itself is designed to show users the status of activity in a particular chat room.
For example: "The interlocutor writes", "the file is loading", and so on.
"""


def lambda_handler(event, context):
    return event["arguments"]["input"]
