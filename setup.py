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
    python_requires=">=3.7,<4",
    install_requires=[
        "aws-requests-auth==0.4.2",
        "rdflib==4.2.2",
        "structlog>=18.2.0,<20",
        "boto3>=1.9.130",
        "typing_extensions>=3.7.4.1,<3.8",
        "jinja2>=2.11.1,<3",
    ],
    scripts=[
        "bin/account_scan.py",
        "bin/altimeter",
        "bin/aws2json.py",
        "bin/graphpruner.py",
        "bin/json2rdf.py",
        "bin/rdf2blaze",
        "bin/rdf2n.py",
        "bin/runquery.py",
        "bin/scan_resource.py",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
