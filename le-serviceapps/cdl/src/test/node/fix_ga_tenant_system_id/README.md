Script to Fix Entity Match GA Tenant's System IDs
=========

EntityMatchGA tenants created before M35 will **NOT** have system account/contact ID set in default system,
nor their mapToLatticeAccount|Contact flag. However, at import/PA time, behavior is as if these values were set.
This will make the tenant not compatible with full multi-template entity match (and not able to switch on the flag).

The purpose of these scripts is to set those ID/flags with the same format as Atlas and migrate tenant's existing data by copying CustomerAccount|ContactId to the generated ID field.

## Prerequisites

1. Install `NodeJS`, tested version is `v11.7.0`
2. Install aws cli
3. Have access to EMR's jupyter notebook, usually at port `9443` (with HTTPS)

## Installation

1. `npm install`
2. copy `config.example.json` to `./config/xxx.json` and configure MySQL credential.

## Fix Tenant Process

1. generate random ID for account/contact with the same format as Atlas.
    - run `node index.js generate-id`
    - copy id to jupyter script
    - also save ID later for step 4
2. make sure the tenant is entity match GA only and don't have system ID/flag set in default system
    - run `node index.js show-tables <TENANT> -C <CONFIG_JSON_FILE>`
    - check default system, fields in batch store, etc, make sure no `user_DefaultSystem_XXX_XXXId`
3. run jupyter script to migrate customer's existing data
    - there are four tables, account batch/system store, contact batch/system store
    - configure tenant, IDs, tablenames in jupyter script
    - after fixing each table, run `copy-s3.sh` to switch the original table and fixed table
    - also delete those table in HDFS
4. run node script to fix default system and metadata for import template and fixed tables in MySQL
    - `node index.js fix-tables <TENANT> -C <CONFIG_PATH> -a <ACCOUNT_SYSTEM_ID> -c <CONTACT_SYSTEM_ID>`
    - `ACCOUNT_SYSTEM_ID` & `CONTACT_SYSTEM_ID` are the ones generated in step 1
