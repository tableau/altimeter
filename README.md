# Altimeter

[![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-457387.svg)](https://www.tableau.com/support-levels-it-and-developer-tools)
[![Build Status](https://api.travis-ci.com/tableau/altimeter.svg?branch=master)](https://travis-ci.com/tableau/altimeter)
[![GitHub](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://raw.githubusercontent.com/Tableau/altimeter/master/LICENSE)

[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)

Altimeter is a system to graph and scan AWS resources across multiple
AWS Organizations and Accounts.

Altimeter generates RDF files which can be loaded into a triplestore
such as AWS Neptune for querying.

# Quickstart

## Installation

    pip install altimeter

## Generating the Graph

Assuming you have configured AWS CLI credentials
(see <https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html>),
run:

    altimeter --base_dir /tmp/altimeter --regions us-east-1

This will scan all resources in the *us-east-1* region. Multiple regions
can be specified in this argument or it can be omitted entirely in which
case all regions will be scanned.

The full path to the generated RDF file will printed, for example:

    Created /tmp/altimeter/20191018/1571425383/graph.rdf

This RDF file can then be loaded into a triplestore such as Neptune or
Blazegraph for querying.

For more user documentation see <https://tableau.github.io/altimeter/>
