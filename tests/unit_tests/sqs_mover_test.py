from unittest.mock import Mock, patch, call

from sqs_mover.sqs_mover import (
    setup_logging,
    get_messages,
    send_messages,
    delete_messages,
    move_messages,
    Message,
    get_approximate_queue_size,
    MESSAGE_BATCH_SIZE,
)


setup_logging()


def test_get_messages_returns_messages():
    sqs_client = Mock()
    attributes = {"environment": "staging"}

    sqs_client.receive_message.return_value = {
        "Messages": [
            {
                "MessageId": 1,
                "Body": "message",
                "MessageAttributes": attributes,
                "ReceiptHandle": "1234",
            }
        ]
    }

    messages = get_messages(sqs_client, "my-queue")

    assert messages == (Message(1, "message", attributes, "1234"),)

    sqs_client.receive_message.assert_called_once_with(
        QueueUrl="my-queue", MaxNumberOfMessages=MESSAGE_BATCH_SIZE, MessageAttributeNames=["All"]
    )


def test_get_messages_supports_empty_response():
    sqs_client = Mock()

    sqs_client.receive_message.return_value = {}

    messages = get_messages(sqs_client, "my-queue")

    assert messages == tuple()


def test_get_messages_supports_empty_attributes():
    sqs_client = Mock()

    sqs_client.receive_message.return_value = {
        "Messages": [{"MessageId": 1, "Body": "message", "ReceiptHandle": "1234"}]
    }

    messages = get_messages(sqs_client, "my-queue")

    assert messages == (Message(1, "message", {}, "1234"),)


def test_send_messagse_sends_messages():
    sqs_client = Mock()

    sqs_client.send_message_batch.return_value = {}

    failed_messages = send_messages(sqs_client, "my-queue", (Message(1, "message", {}, "1234"),))

    sqs_client.send_message_batch.assert_called_once_with(
        QueueUrl="my-queue", Entries=[{"Id": 1, "MessageBody": "message", "MessageAttributes": {}}]
    )

    assert failed_messages == tuple()


def test_send_messagse_returns_failed_messages():
    sqs_client = Mock()

    sqs_client.send_message_batch.return_value = {"Failed": [{"MessageId": 2}]}

    messages = (Message(1, "message", {}, "1234"), Message(2, "another", {}, "5678"))

    failed_messages = send_messages(sqs_client, "my-queue", messages)
    assert failed_messages == messages[1:]


def test_delete_messages_deletes_messages():
    sqs_client = Mock()

    sqs_client.delete_message_batch.return_value = {}

    failed_messages = delete_messages(sqs_client, "my-queue", (Message(1, "message", {}, "1234"),))

    sqs_client.delete_message_batch.assert_called_once_with(
        QueueUrl="my-queue", Entries=[{"Id": 1, "ReceiptHandle": "1234"}]
    )

    assert failed_messages == tuple()


def test_delete_messages_returns_failed_messages():
    sqs_client = Mock()

    sqs_client.delete_message_batch.return_value = {"Failed": [{"MessageId": 2}]}

    messages = (Message(1, "message", {}, "1234"), Message(2, "another", {}, "5678"))
    failed_messages = delete_messages(sqs_client, "my-queue", messages)

    sqs_client.delete_message_batch.assert_called_once_with(
        QueueUrl="my-queue",
        Entries=[{"Id": 1, "ReceiptHandle": "1234"}, {"Id": 2, "ReceiptHandle": "5678"}],
    )

    assert failed_messages == tuple(messages[1:])


@patch("sqs_mover.sqs_mover.get_approximate_queue_size", Mock(return_value=20))
@patch("sqs_mover.sqs_mover.delete_messages")
@patch("sqs_mover.sqs_mover.send_messages")
@patch("sqs_mover.sqs_mover.get_messages")
@patch("sqs_mover.sqs_mover.get_queue_url")
def test_move_messages_moves_in_bulks(get_queue_url, get_messages, send_messages, delete_messages):
    sqs_client = Mock()

    messages = [Message(i, str(i), {}, str(i)) for i in range(10)]
    batches = [messages[:5], messages[5:], tuple()]

    send_messages.side_effect = [tuple(), tuple()]
    delete_messages.side_effect = [tuple(), tuple()]

    def _get_queue_url(_, queue_name):
        return {"source": "http://source", "dest": "http://dest"}[queue_name]

    get_queue_url.side_effect = _get_queue_url
    get_messages.side_effect = batches

    move_messages("source", "dest", sqs_client=sqs_client)

    assert get_queue_url.call_args_list == [call(sqs_client, "source"), call(sqs_client, "dest")]
    assert get_messages.call_args_list == [call(sqs_client, "http://source")] * 3
    assert send_messages.call_args_list == [
        call(sqs_client, "http://dest", batches[0]),
        call(sqs_client, "http://dest", batches[1]),
    ]
    assert delete_messages.call_args_list == [
        call(sqs_client, "http://source", batches[0]),
        call(sqs_client, "http://source", batches[1]),
    ]
