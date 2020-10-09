"""setup.py"""
from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="altimeter",
    version="LOCALVERSION",
    packages=find_packages(exclude=["tests"]),
    author="Tableau",
    description="Graph AWS resources in Neptune",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tableau/altimeter",
    python_requires=">=3.8,<4",
    install_requires=[
        "aws-requests-auth>=0.4.3,<1",
        "rdflib==4.2.2",
        "structlog>=20.1.0,<21",
        "boto3>=1.9.130,<2",
        "jinja2>=2.11.1,<3",
        "toml>=0.10.0,<1",
        "gremlinpython>=3.4.3"
    ],
    extras_require={
        "qj": [
            "alembic==1.4.2",
            "fastapi>=0.60.1,<1",
            "psycopg2-binary>=2.8.5,<3",
            "pydantic>=1.6.1,<2",
            "sqlalchemy>=1.3.16,<2",
            "uvicorn>=0.11.5,<2",
        ],
        "hyper": [
            "tableauhyperapi>=0.0.11355,<1",
            "pydantic>=1.6.1,<2",
        ]
    },
    data_files=[
        (
            "services/qj/alembic",
            ["services/qj/alembic/env.py", "services/qj/alembic/alembic.ini",],
        ),
        (
            "services/qj/alembic/versions",
            ["services/qj/alembic/versions/dc8f1df07766_init.py",],
        ),
    ],
    scripts=[
        "bin/account_scan.py",
        "bin/altimeter",
        "bin/aws2n.py",
        "bin/aws2neptune.py",
        "bin/graphpruner.py",
        "bin/json2hyper.py",
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
