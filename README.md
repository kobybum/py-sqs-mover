# Python SQS Mover

[![Build Status](https://travis-ci.org/kobybum/py-sqs-mover.svg?branch=master)](https://travis-ci.org/kobybum/py-sqs-mover)

## Use Case

Copying messages between SQS queues. Useful when retrying messages from a DLQ.

## Supported features

The current version supports copying messages with the message attributes between queues in the same account.

## Usage

Install the CLI tool:

```sh
pip install sqs_mover
sqsmover -s <source_queue_name> -d <destination_queue_name>
```

If you'd like to run using a specific AWS profile, you can set the `AWS_PROFILE` and `AWS_DEFAULT_REGION` environment variables to your desired configuration:
```
AWS_PROFILE=production AWS_DEFAULT_REGION=us-west-2 sqsmover -s <source_queue_name> -d <destination_queue_name>
```
