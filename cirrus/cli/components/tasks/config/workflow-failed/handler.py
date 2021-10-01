import boto3
import json
from os import getenv

from cirruslib import Catalog, StateDB, get_task_logger

# envvars
FAILED_TOPIC_ARN = getenv('CIRRUS_FAILED_TOPIC_ARN', None)
INVALID_TOPIC_ARN = getenv('CIRRUS_INVALID_TOPIC_ARN', None)

# boto3 clients
SNS_CLIENT = boto3.client('sns')

# Cirrus state database
statedb = StateDB()

def handler(payload, context):
    catalog = Catalog.from_payload(payload)
    logger = get_task_logger(f"{__name__}.workflow-failed", catalog=catalog)

    # parse errors
    error = payload.get('error', {})

    # error type
    error_type = error.get('Error', "unknown")

    # check if cause is JSON
    try:
        cause = json.loads(error['Cause'])
        error_msg = 'unknown'
        if 'errorMessage' in cause:
            error_msg = cause.get('errorMessage', 'unknown')
    except Exception:
        error_msg = error['Cause']

    error = f"{error_type}: {error_msg}"
    logger.info(error)

    try:
        if error_type == "InvalidInput":
            statedb.set_invalid(catalog['id'], error)
            notification_topic_arn = INVALID_TOPIC_ARN
        else:
            statedb.set_failed(catalog['id'], error)
            notification_topic_arn = FAILED_TOPIC_ARN
    except Exception as err:
        msg = f"Failed marking as failed: {err}"
        logger.error(msg, exc_info=True)
        raise err

    if notification_topic_arn is not None:
        try:
            item = statedb.dbitem_to_item(statedb.get_dbitem(catalog['id']))
            attrs = {
                'collections': {
                    'DataType': 'String',
                    'StringValue': item['collections']
                },
                'workflow': {
                    'DataType': 'String',
                    'StringValue': item['workflow']
                },
                'error': {
                    'DataType': 'String',
                    'StringValue': error
                }
            }
            logger.debug(f"Publishing item to {notification_topic_arn}")
            SNS_CLIENT.publish(
                TopicArn=notification_topic_arn,
                Message=json.dumps(item),
                MessageAttributes=attrs,
            )
        except Exception as err:
            msg = f"Failed publishing to {notification_topic_arn}: {err}"
            logger.error(msg, exc_info=True)
            raise err

    return catalog
