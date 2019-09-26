# Python SQS Mover

## Use Case
Copying messages between SQS queues. Useful when retrying messages from a DLQ.

## Usage

### Installing CLI tool

```sh
pip install sqs_mover
sqsmover -s <source_queue_name> -d <destination_queue_name>
```

### Cloning the repo

```sh
git clone https://github.com/kobybum/py-sqs-mover
cd py-sqs-mover
python -m sqs_mover.sqs_mover -s <source_queue_name> -d <destination_queue_name>
```
