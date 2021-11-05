import logging
import argparse
import boto3
import json

from typing import Dict, Tuple, NamedTuple, Optional


class Message(NamedTuple):
    message_id: str
    body: str
    attributes: Optional[Dict]
    receipt_handle: str


Messages = Tuple[Message, ...]

logger = logging.getLogger("sqs_mover")


def get_queue_url(sqs_client, queue_name: str) -> str:
    return sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]


def get_messages(sqs_client, queue_url: str, message_batch_size: int) -> Messages:
    if message_batch_size > 0:
        raw_messages = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=message_batch_size,
            MessageAttributeNames=["All"],
        ).get("Messages")

        if not raw_messages:
            return tuple()

        return tuple(
            Message(
                message_id=raw_message["MessageId"],
                body=raw_message["Body"],
                attributes=raw_message.get("MessageAttributes", {}),
                receipt_handle=raw_message["ReceiptHandle"],
            )
            for raw_message in raw_messages
        )
    return tuple()


def send_messages(sqs_client, queue_url: str, messages: Messages) -> Messages:
    send_entries = [
        {
            "Id": message.message_id,
            "MessageBody": message.body,
            "MessageAttributes": message.attributes,
        }
        for message in messages
    ]

    logger.debug("Sending: %s", send_entries)

    send_response = sqs_client.send_message_batch(QueueUrl=queue_url, Entries=send_entries)

    failed_ids = {failure["MessageId"] for failure in send_response.get("Failed", [])}
    if failed_ids:
        logger.error("Failed to send messages: %s", send_response)

    return tuple(message for message in messages if message.message_id in failed_ids)


def delete_messages(sqs_client, queue_url: str, messages: Messages) -> Messages:
    delete_entries = [
        {"Id": message.message_id, "ReceiptHandle": message.receipt_handle} for message in messages
    ]

    delete_response = sqs_client.delete_message_batch(QueueUrl=queue_url, Entries=delete_entries)

    failed_ids = {failure["MessageId"] for failure in delete_response.get("Failed", [])}
    if failed_ids:
        logger.error("Failed to delete messages: %s", delete_response)

    return tuple(message for message in messages if message.message_id in failed_ids)


def get_approximate_queue_size(sqs_client, queue_url: str) -> str:
    queue_attributes = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
    )
    return queue_attributes["Attributes"]["ApproximateNumberOfMessages"]


def move_messages(
    source_queue_name: str,
    dest_queue_name: str,
    message_batch_size: int,
    message_limit=None,
    sqs_client=None,
):
    sqs_client = sqs_client or boto3.client("sqs")

    source_url = get_queue_url(sqs_client, source_queue_name)
    dest_url = get_queue_url(sqs_client, dest_queue_name)

    total_messages = get_approximate_queue_size(sqs_client, source_url)

    logger.info(
        "Approximately %s messages are in %s ", total_messages, source_queue_name
    )

    messages = None
    messages_moved = 0
    # In every 10 iteration, check for messages left in queue and log.
    iteration = 0
    while True:
        effective_limit = None
        if message_limit is not None:
            effective_limit = message_limit - messages_moved

        effective_batch_size = (
            message_batch_size
            if effective_limit is None
            else effective_limit
            if effective_limit < message_batch_size
            else message_batch_size
        )

        messages = get_messages(sqs_client, source_url, effective_batch_size)
        if not messages:
            break

        logger.debug("Received messages: %s", messages)

        failed_sends = send_messages(sqs_client, dest_url, messages)
        if failed_sends:
            return

        failed_deletions = delete_messages(sqs_client, source_url, messages)
        if failed_deletions:
            return

        iteration += 1
        messages_moved += len(messages)

        if iteration % 10 == 0:
            total_messages = get_approximate_queue_size(sqs_client, source_url)
            logger.info("Moved %d messages, approximately %s left", messages_moved, total_messages)

    logger.info("Moved total %d message(s)", messages_moved)


def poll_messages(
    source_queue_name: str, message_batch_size: int, message_limit=None, sqs_client=None
):
    sqs_client = sqs_client or boto3.client("sqs")
    source_url = get_queue_url(sqs_client, source_queue_name)
    processed_messages = 0
    while True:
        effective_limit = None
        if message_limit is not None:
            effective_limit = message_limit - processed_messages

        effective_batch_size = (
            message_batch_size
            if effective_limit is None
            else effective_limit
            if effective_limit < message_batch_size
            else message_batch_size
        )

        messages = get_messages(sqs_client, source_url, effective_batch_size)

        if not messages:
            break

        message_bodies = [message.body for message in messages]
        logger.info("Messages: %s", json.dumps(message_bodies, indent=4))
        processed_messages += len(messages)


def setup_logging(verbose: bool = False):
    logging.getLogger("botocore").setLevel("WARNING")
    logging.getLogger("urllib3").setLevel("WARNING")
    if verbose:
        logging.basicConfig(format="%(asctime)s %(name)s - %(message)s", level=logging.DEBUG)
    else:
        logging.basicConfig(format="%(asctime)s %(name)s - %(message)s", level=logging.INFO)


def run_from_cli():
    parser = argparse.ArgumentParser(description="Move messages between SQS queues.")
    parser.add_argument(
        "-p",
        "--poll",
        help="Poll messages from the source queue without moving.",
        action="store_true",
    )
    parser.add_argument("-s", "--source", help="Source queue name", required=True)
    parser.add_argument("-d", "--dest", help="Destination queue name", required=False)
    parser.add_argument(
        "-b",
        "--batch",
        help="The number of messages to request each iteration, 10 maximum",
        type=int,
        required=False,
        default=10,
    )
    parser.add_argument(
        "-l", "--limit", type=int, help="Limit on number of messages to operate", required=False
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="print the contents of the messages as they are being moved",
        required=False,
    )

    args = parser.parse_args()
    if args.verbose:
        setup_logging(verbose=True)
    else:
        setup_logging()
    if args.poll:
        poll_messages(args.source, args.batch, args.limit)
    else:
        if not args.dest:
            parser.error("-d argument is required if not polling")
        move_messages(args.source, args.dest, args.batch, args.limit)


if __name__ == "__main__":
    run_from_cli()
