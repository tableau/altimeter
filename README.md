# Altimeter

[![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-457387.svg)](https://www.tableau.com/support-levels-it-and-developer-tools)
[![GitHub](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://raw.githubusercontent.com/Tableau/altimeter/master/LICENSE)

[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)

Altimeter is a system to graph and scan AWS resources across multiple
AWS Organizations and Accounts.

Altimeter generates RDF files which can be loaded into a triplestore
such as AWS Neptune for querying.

# Quickstart

## Installation

    pip install altimeter

## Configuration

Altimeter's behavior is driven by a toml configuration file.  A few sample
configuration files are included in the `conf/` directory:

* `current_single_account.toml` - scans the current account - this is the account
  for which the environment's currently configured AWS CLI credentials are.
* `current_master_multi_account.toml` - scans the current account and attempts to
  scan all organizational subaccounts - this configuration should be used if you
  are scanning  all accounts in an organization.  To do this the currently
  configured AWS CLI credentials should be pointing to an AWS Organizations
  master account.

To scan a subset of regions, set the region list parameter `regions` in the `scan`
section to a list of region names.

## Required IAM permissions

The following permissions are required for a scan of all supported resource types:

    acm:DescribeCertificate
    acm:ListCertificates
    cloudtrail:DescribeTrails
    dynamodb:DescribeContinuousBackups
    dynamodb:DescribeTable
    dynamodb:ListTables
    ec2:DescribeFlowLogs
    ec2:DescribeImages
    ec2:DescribeInstances
    ec2:DescribeInternetGateways
    ec2:DescribeNetworkInterfaces
    ec2:DescribeRegions
    ec2:DescribeRouteTables
    ec2:DescribeSecurityGroups
    ec2:DescribeSnapshots
    ec2:DescribeSubnets
    ec2:DescribeTransitGatways
    ec2:DescribeTransitGatwayAttachments
    ec2:DescribeVolumes
    ec2:DescribeVpcEndpoints
    ec2:DescribeVpcEndpointServiceConfigurations
    ec2:DescribeVpcPeeringConnections
    ec2:DescribeTransitGatewayVpcAttachments
    ec2:DescribeVpcs
    elasticloadbalancing:DescribeLoadBalancers
    elasticloadbalancing:DescribeLoadBalancerAttributes
    elasticloadbalancing:DescribeTargetGroups
    elasticloadbalancing:DescribeTargetGroupAttributes
    elasticloadbalancing:DescribeTargetHealth
    eks:ListClusters
    events:ListRules
    events:ListTargetsByRule
    events:DescribeEventBus
    guardduty:GetDetector
    guardduty:GetMasterAccount
    guardduty:ListDetectors
    guardduty:ListMembers
    iam:GetAccessKeyLastUsed
    iam:GetAccountPasswordPolicy
    iam:GetGroup
    iam:GetGroupPolicy
    iam:GetLoginProfile
    iam:GetOpenIDConnectProvider
    iam:GetPolicyVersion
    iam:GetRolePolicy
    iam:GetSAMLProvider
    iam:GetUserPolicy
    iam:ListAccessKeys
    iam:ListAttachedGroupPolicies
    iam:ListAttachedRolePolicies
    iam:ListAttachedUserPolicies
    iam:ListGroupPolicies
    iam:ListGroups
    iam:ListinstanceProfiles
    iam:ListMFADevices
    iam:ListOpenIDConnectProviders
    iam:ListPolicies
    iam:ListPolicies
    iam:ListRolePolicies
    iam:ListRoles
    iam:ListSAMLProviders
    iam:ListUserPolicies
    iam:ListUsers
    kms:ListKeys
    lambda:ListFunctions
    rds:DescribeDBInstances
    rds:DescribeDBInstanceAutomatedBackups
    rds:ListTagsForResource
    rds:DescribeDBSnapshots
    route53:ListHostedZones
    route53:ListResourceRecordSets
    s3:ListBuckets
    s3:GetBucketLocation
    s3:GetBucketEncryption
    s3:GetBucketTagging
    sts:GetCallerIdentity
    support:DescribeSeverityLevels

Additionally if you are doing multi-account scanning via an MPA master account you
will also need:

    organizations:DescribeOrganization
    organizations:ListAccounts
    organizations:ListAccountsForParent
    organizations:ListOrganizationalUnitsForParent
    organizations:ListRoots

## Generating the Graph

Assuming you have configured AWS CLI credentials
(see <https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html>),
run:

    altimeter <path-to-config>

This will scan all resources in regions specified in the config file.

The full path to the generated RDF file will printed, for example:

    Created /tmp/altimeter/20191018/1571425383/graph.rdf

This RDF file can then be loaded into a triplestore such as Neptune or
Blazegraph for querying.

For more user documentation see <https://tableau.github.io/altimeter/>
