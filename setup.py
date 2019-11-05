"""setup.py"""
import os
from setuptools import setup, find_packages

setup(
    name="altimeter",
    version=os.environ.get("PKG_VERSION", "0.0.1dev0+local"),
    packages=find_packages(),
    python_requires=">=3.7,<4",
    install_requires=[
        "aws-requests-auth==0.4.2",
        "rdflib==4.2.2",
        "structlog==18.2.0",
        "boto3>=1.9.130",
    ],
    scripts=[
        "bin/json2rdf.py",
        "bin/rdf2n.py",
        "bin/aws2json.py",
        "bin/account_scan.py",
        "bin/scan_resource.py",
    ],
)
