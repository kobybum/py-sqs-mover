import logging
import argparse
import os

import boto3

from typing import List, Dict, Tuple, NamedTuple, Optional

from tqdm import tqdm

CLI_COLOR = "CYAN"
DISABLE_TQDM = False


class Message(NamedTuple):
    message_id: str
    body: str
    attributes: Optional[Dict]
    receipt_handle: str

class Input(NamedTuple):
    region: str
    source_queue_name: str
    destination_queue_name: List[str]
    message_batch_size: int
    verbose: bool
    poll_message_path: str
    is_copy: bool
    message_limit: int

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


def get_approximate_queue_size(sqs_client, queue_url: str) -> int:
    queue_attributes = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
    )
    return int(queue_attributes["Attributes"]["ApproximateNumberOfMessages"])


def move_messages(
        source_queue_name: str,
        dest_queue_names: List[str],
        message_batch_size: int,
        message_limit=None,
        sqs_client=None,
):
    sqs_client = sqs_client or boto3.client("sqs")

    source_url = get_queue_url(sqs_client, source_queue_name)
    dest_urls = [get_queue_url(sqs_client, dest_queue_name) for dest_queue_name in dest_queue_names]

    total_messages = get_approximate_queue_size(sqs_client, source_url)

    messages = None
    messages_moved = 0
    # In every 10 iteration, check for messages left in queue and log.
    iteration = 0
    messages_to_poll = (
        total_messages if message_limit is None else min(message_limit, total_messages)
    )
    with tqdm(total=messages_to_poll, colour=CLI_COLOR, unit="msg", ncols=120, disable=DISABLE_TQDM) as progress_bar:
        tqdm.write("Approximately %s messages are in %s" % (total_messages, source_queue_name))
        tqdm.write(
            "Moving %s messages from %s to %s"
            % (messages_to_poll, source_queue_name, dest_urls)
        )
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
            for dest_url in dest_urls:
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
                logger.debug(
                    "Moved %d messages, approximately %s left", messages_moved, total_messages
                )
            progress_bar.update(len(messages))
    tqdm.write("Moved total %d message(s)" % messages_moved)

def copy_messages(
        source_queue_name: str,
        dest_queue_names: List[str],
        message_batch_size: int,
        message_limit=None,
        sqs_client=None,
):
    sqs_client = sqs_client or boto3.client("sqs")

    source_url = get_queue_url(sqs_client, source_queue_name)
    dest_urls = [get_queue_url(sqs_client, dest_queue_name) for dest_queue_name in dest_queue_names]

    total_messages = get_approximate_queue_size(sqs_client, source_url)

    messages = None
    messages_moved = 0
    # In every 10 iteration, check for messages left in queue and log.
    iteration = 0
    messages_to_poll = (
        total_messages if message_limit is None else min(message_limit, total_messages)
    )
    with tqdm(total=messages_to_poll, colour=CLI_COLOR, unit="msg", ncols=120, disable=DISABLE_TQDM) as progress_bar:
        tqdm.write("Approximately %s messages are in %s" % (total_messages, source_queue_name))
        tqdm.write(
            "Copying %s messages from %s to %s"
            % (messages_to_poll, source_queue_name, dest_urls)
        )
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
            for dest_url in dest_urls:
                failed_sends = send_messages(sqs_client, dest_url, messages)
                if failed_sends:
                    return

            iteration += 1
            messages_moved += len(messages)

            if iteration % 10 == 0:
                total_messages = get_approximate_queue_size(sqs_client, source_url)
                logger.debug(
                    "Copied %d messages, approximately %s left", messages_moved, total_messages
                )
            progress_bar.update(len(messages))
    tqdm.write("Copied total %d message(s)" % messages_moved)


def poll_messages(
        source_queue_name: str,
        message_batch_size: int,
        output_file_name: str,
        message_limit=None,
        sqs_client=None,
):
    sqs_client = sqs_client or boto3.client("sqs")
    source_url = get_queue_url(sqs_client, source_queue_name)
    total_messages = get_approximate_queue_size(sqs_client, source_url)

    with open(output_file_name, "w+") as output_file:
        messages_to_poll = (
            total_messages if message_limit is None else min(message_limit, total_messages)
        )
        with tqdm(total=messages_to_poll, colour=CLI_COLOR, unit="msg", ncols=120,
                  disable=DISABLE_TQDM) as progress_bar:
            tqdm.write("Total %s messages are in queue %s" % (total_messages, source_queue_name))
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
                output_file.writelines(f"{message}\n" for message in message_bodies)
                processed_messages += len(messages)
                progress_bar.update(len(messages))
        tqdm.write("%d message(s) polled" % processed_messages)


def setup_logging(verbose: bool = False):
    logging.getLogger("botocore").setLevel("WARNING")
    logging.getLogger("urllib3").setLevel("WARNING")
    if verbose:
        logging.basicConfig(format="%(asctime)s %(name)s - %(message)s", level=logging.DEBUG)
    else:
        logging.basicConfig(format="%(message)s", level=logging.INFO)


def range_limited_int_type(min_value: int, max_value: int):
    """Type function for argparse - a int within some predefined bounds"""

    def _range_limited_int_type(arg):
        try:
            f = int(arg)
        except ValueError:
            raise argparse.ArgumentTypeError("Must be a floating point number")
        if f < min_value or f > max_value:
            raise argparse.ArgumentTypeError(f"Argument must be < {min_value} and > {max_value}")
        return f

    return _range_limited_int_type


def main(input_value: Input):
    if input_value.verbose:
        setup_logging(verbose=True)
        global DISABLE_TQDM
        DISABLE_TQDM = True
    else:
        setup_logging()

    sqs_client = boto3.client("sqs", region_name=input_value.region)

    if input_value.poll_message_path:
        poll_messages(
            source_queue_name=input_value.source_queue_name,
            output_file_name=input_value.poll_message_path,
            message_batch_size=input_value.message_batch_size,
            message_limit=input_value.message_limit,
            sqs_client=sqs_client
        )
    elif input_value.is_copy:
        if not input_value.destination_queue_name:
            logging.error("-d argument is required if not polling")
            exit(1)
        copy_messages(
            source_queue_name=input_value.source_queue_name,
            dest_queue_names=input_value.destination_queue_name,
            message_batch_size=input_value.message_batch_size,
            message_limit=input_value.message_limit,
            sqs_client=sqs_client,
        )
    else:
        if not input_value.destination_queue_name:
            logging.error("-d argument is required if not polling")
            exit(1)
        move_messages(
            source_queue_name=input_value.source_queue_name,
            dest_queue_names=input_value.destination_queue_name,
            message_batch_size=input_value.message_batch_size,
            message_limit=input_value.message_limit,
            sqs_client=sqs_client,
        )

def run_from_cli():
    parser = argparse.ArgumentParser(description="Move, Copy or Poll messages from SQS queue.")
    parser.add_argument(
        "-p",
        "--poll",
        help="Poll messages from the source queue without moving."
             " User must pass a writeable file path where messages will be written",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-c",
        "--copy",
        help="Copy messages from the source queue to destination queue without moving.",
        action="store_true",
    )
    parser.add_argument("-s", "--source", help="Source queue name", required=True)
    parser.add_argument(
        "-d", "--dest", help="Destination queue names", type=str, nargs="*", required=False
    )
    parser.add_argument(
        "-b",
        "--batch",
        help="The number of messages to request each iteration, 10 maximum",
        type=range_limited_int_type(0, 10),
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
        action='store_true',
        required=False
    )
    parser.add_argument(
        "-r",
        "--region",
        help="AWS region where queues are located",
        required=False,
        default=os.environ.get("AWS_REGION"),
    )

    args = parser.parse_args()

    if (args.copy or args.dest) and not args.dest:
        parser.error("-d argument is required if not polling")

    main(
        Input(
            region=args.region,
            source_queue_name=args.source,
            destination_queue_name=args.dest,
            message_batch_size=args.batch,
            verbose=args.verbose,
            poll_message_path=args.poll,
            is_copy=args.copy,
            message_limit=args.limit,
        )
    )


if __name__ == "__main__":
    run_from_cli()
