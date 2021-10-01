#!/usr/bin/env python
import logging
import json
import boto3

from os import getenv

from cirruslib import Catalog, StateDB


logger = logging.getLogger(__name__)


# envvars
FAILED_TOPIC_ARN = getenv('CIRRUS_FAILED_TOPIC_ARN', None)
INVALID_TOPIC_ARN = getenv('CIRRUS_INVALID_TOPIC_ARN', None)

# boto3 clients
SNS_CLIENT = boto3.client('sns')
LOG_CLIENT = boto3.client('logs')
SFN_CLIENT = boto3.client('stepfunctions')

# Cirrus state database
statedb = StateDB()

# how many execution events to request/check
# for an error cause in a FAILED state
MAX_EXECUTION_EVENTS = 10

FAILED = 'FAILED'


def workflow_completed(catalog, error):
    statedb.set_completed(catalog['id'])


def workflow_failed(catalog, error):
    # error type
    error_type = error.get('Error', "unknown")

    # check if cause is JSON
    try:
        cause = json.loads(error['Cause'])
        error_msg = 'unknown'
        if 'errorMessage' in cause:
            error_msg = cause.get('errorMessage', 'unknown')
        elif 'Attempts' in cause:
            try:
                # batch
                reason = cause['Attempts'][-1]['StatusReason']
                if 'Essential container in task exited' in reason:
                    # get the message from batch logs
                    logname = cause['Attempts'][-1]['Container']['LogStreamName']
                    error_type, error_msg = get_error_from_batch(logname)
            except Exception as err:
                logger.error(err, exc_info=True)
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


STATUS_UPDATE_MAP = {
    FAILED: workflow_failed,
    'SUCCESS': workflow_completed,
}


def get_execution_error(arn):
    try:
        history = SFN_CLIENT.get_execution_history(
            executionArn=arn,
            maxResults=MAX_EXECUTION_EVENTS,
            reverseOrder=True,
        )
        for event in history['events']:
            try:
                details = event['stateEnteredEventDetails']
                return json.loads(details['input'])['error']
            except KeyError:
                pass
    except Exception as e:
        logger.exception(e)
    logger.warning(
        'Could not find execution error in last %s events',
        MAX_EXECUTION_EVENTS,
    )



# TODO: in cirruslib make a factory class that returns classes
# for each error type, and generalize the processing here into
# a well-known type interface
def parse_payload(payload):
    # return a tuple of:
    #   - catalog object
    #   - status string
    #   - error object
    try:
        if 'error' in payload:
            logger.debug('looks like a stac record with an error message, i.e., workflow-failed')
            return (
                Catalog.from_payload(payload),
                FAILED,
                payload.get('error', {}),
            )
        elif payload.get('source', '') == "aws.states":
            status = payload['detail']['status']
            logger.debug("looks like a step function event message, status '%s'", status)
            error = None
            if status == FAILED:
                error = get_execution_error(payload['detail']['executionArn'])
            return (
                Catalog.from_payload(json.loads(payload['detail']['input'])),
                status,
                error,
            )
        else:
            raise Exception()
    except Exception:
        logger.error('Unknown payload: %s', json.dumps(payload))
        return None, None, None


def get_error_from_batch(logname):
    try:
        logs = LOG_CLIENT.get_log_events(logGroupName='/aws/batch/job', logStreamName=logname)
        msg = logs['events'][-1]['message'].lstrip('cirruslib.errors.')
        parts = msg.split(':', maxsplit=1)
        if len(parts) > 1:
            error_type = parts[0]
            msg = parts[1]
            return error_type, msg
        return "Unknown", msg
    except Exception:
        return "Exception", "Unable to get Error Log"


def handler(payload, context={}):
    logger.debug(payload)
    catalog, status, error = parse_payload(payload)

    if status not in STATUS_UPDATE_MAP:
        logger.info("Status does not support updates")
        return

    STATUS_UPDATE_MAP[status](catalog, error)
