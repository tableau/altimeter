Multi-Account Scanning
======================

When scanning a single account as described in the :doc:`Quickstart <quickstart>`,
Altimeter uses the local AWS credentials directly to gather data via AWS APIs.

IAM Role Configuration
----------------------

To scan multiple accounts Altimeter requires IAM role trusts between a source
account and each of the accounts to be scanned.

The recommended setup is to have a role 'AltimeterRole' in all accounts which
should be scanned.  This role should have readonly access to all resources.
One of the accounts must be designated as a 'master' account (if your are using
AWS Organizations this could be the org master account).

All non-master accounts should have an assume role policy on 'AltimeterRole'
which allows the master account 'AltimeterRole' to access it.

Altimeter Access Configuration
------------------------------

Altimeter requires an access configuration file which describes how to access
accounts.  Given the recommended setup above this would look like:

::

    {
        "accessors": [
            {
                "role_session_name": "altimeter",
                "access_steps": [
                    {
                        "external_id_env_var": "ALTIMETER_EXT_ID",
                        "role_name": "AltimeterRole"
                    }
                ]
            }
        ]
    }

The 'external_id_env_var' key refers to the name of an environment variable
(ALTIMETER_EXT_ID) which should contain the external id for the IAM trust.
If you do not wish to use an external id this can be omitted.

Running aws2json.py
-------------------

Given an access config as described above in 'access_config.json', aws2json.py
can now be run as the following command assuming you currently have AWS API
credentials configured for the master account:

::

    bin/altimeter_local.sh  --access_config access_config.json \
                            --base_dir /tmp \
                            --regions us-east-1 \
                            --accounts 1234 4567 7890

where 1234 4567 7890 are account ids which can be accessed using the trust above.

This will generate a JSON file which can be converted to RDF as described
in :doc:`Quickstart <quickstart>` and loaded into a local Blazegraph instance
and queried as described in :doc:`Local Querying with Blazegraph <local_blazegraph>`

Organizations
-------------

If you are using AWS Organizations and specify the master account id as the
account to scan, Altimeter can discover all subaccounts and attempt
to scan them as well.  To enable this behavior use the `--scan_sub_accounts`
flag, e.g.:

::

    bin/altimeter_local.sh  --access_config access_config.json \
                            --base_dir /tmp \
                            --regions us-east-1 \
                            --accounts master-account-id \
                            --scan_sub_accounts