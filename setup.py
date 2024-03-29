"""setup.py"""
from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="altimeter",
    version="0.0.1",
    packages=find_packages(exclude=["tests"]),
    author="Tableau",
    description="Graph AWS resources in Neptune",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tableau/altimeter",
    python_requires=">=3.8,<3.10",
    install_requires=[
        "MarkupSafe==2.1.1",
        "aws-requests-auth==0.4.3",
        "rdflib==6.0.2",
        "structlog==20.2.0",
        "boto3==1.28.80",
        "jinja2==3.0.3",
        "pydantic==1.9.0",
        "toml==0.10.2",
        "gremlinpython==3.4.12",
        "requests==2.31.0",
        "certifi==2023.7.22",
        "urllib3==1.26.18",
    ],
    extras_require={
        "qj": [
            "MarkupSafe==2.1.1",
            "alembic==1.4.2",
            "boto3==1.28.80",
            "fastapi==0.96.0",
            "psycopg2-binary==2.9.2",
            "sqlalchemy==1.3.24",
            "tableauhyperapi==0.0.18161",
            "tableauserverclient==0.17.0",
            "uvicorn==0.16.0",
            "urllib3==1.26.18",
        ],
    },
    data_files=[
        (
            "altimeter/services/qj/alembic",
            ["services/qj/alembic/env.py", "services/qj/alembic/alembic.ini",],
        ),
        (
            "altimeter/services/qj/alembic/versions",
            [
                "services/qj/alembic/versions/dc8f1df07766_init.py",
                "services/qj/alembic/versions/60990e9bc347_added_notify_if_results_bool_on_job.py",
                "services/qj/alembic/versions/9d956e753055_added_remediate_sqs_queue_column_to_qj_.py",
                "services/qj/alembic/versions/e6e2a6bf2a39_adding_query_job_raw_query_column.py",
                "services/qj/alembic/versions/94f36533d115_remove_result_synthetic_pk.py",
            ],
        ),
    ],
    scripts=[
        "bin/altimeter",
        "bin/aws2n.py",
        "bin/aws2neptune.py",
        "bin/rdf2blaze",
        "bin/runquery.py",
        "bin/scan_resource.py",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
