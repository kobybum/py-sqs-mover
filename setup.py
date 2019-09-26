import setuptools

setuptools.setup(
    name="sqs_mover",
    version="0.0.4",
    author="Koby Bass",
    description="Utility for moving message between SQS queues",
    url="https://github.com/kobybum/py-sqs-mover",
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
    scripts=["bin/sqsmover"],
    install_requires=["boto3>=1.9.236"],
)
