"""AWS scan settings

Attributes:
    GRAPH_NAME: name for the AWS graph.
    GRAPH_VERSION: AWS graph version.  Generally this should change very rarely.
"""
import os

GRAPH_NAME: str = "alti"
GRAPH_VERSION: str = "1"

# The following variables control scan concurrency.
#
# The execution model of aws2json is as such:
#
# aws2json.py (single process)
#       AccountScan (up to MAX_<exectype>_ACCOUNT_SCAN_THREADS concurrently)
#             AWSScanMuxer (up to MAX_SCAN_THREADS concurrently)
#
# In general, the maximum number of concurrent scan operations is:
#
#   MAX_<exectype>_ACCOUNT_SCAN_THREADS * MAX_SCAN_THREADS
#
# Note that each of these variables is overridable via env vars, see os.environ.get calls below.

# in lambda, number of AccountScan lambdas to run concurrently
MAX_LAMBDA_ACCOUNT_SCAN_THREADS = int(os.environ.get("MAX_MUXER_THREADS", 64))
# in lambda, the number of accounts to scan in each AccountScan lambda
MAX_LAMBDA_ACCOUNTS_PER_THREAD = int(os.environ.get("MAX_ACCOUNTS_PER_THREAD", 1))

# in local usage, number of AccountScans to run concurrently
MAX_LOCAL_ACCOUNT_SCAN_THREADS = int(os.environ.get("MAX_MUXER_THREADS", 4))
# in lcoal usage, the number of accounts to scan in each AccountScan
MAX_LOCAL_ACCOUNTS_PER_THREAD = int(os.environ.get("MAX_ACCOUNTS_PER_THREAD", 1))

# the number of scan threads to spawn in each account scan
MAX_ACCOUNT_SCANNER_THREADS = int(os.environ.get("MAX_ACCOUNT_SCANNER_THREADS", 32))

# for ACCOUNT granularity resources, this is the list of preferred scan regions
PREFERRED_ACCOUNT_SCAN_REGIONS = ("us-west-1", "us-west-2", "us-east-1", "us-east-2")
