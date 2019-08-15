import argparse
import sys
from os import getenv

import requests
from sqlalchemy.sql import func

sys.path.append('{}/le-migration/python'.format(getenv('WSHOME')))
from models.metadata_data_collection_table import MetadataDataCollectionTable
from models.metadata_data_collection_status import MetadataDataCollectionStatus

USAGE = 'Usage: rollbackTenant -u <username> -p <password> -x <host> [-d <db name>] -t TENANT_PID -a <API Domain>'
API = None
storage = None


def checkAPI():
    if not API:
        print('API domain is not provided')
        return False
    try:
        headers = {'MagicAuthentication': 'Security through obscurity!', 'Content-Type': 'application/json'}
        return requests.get('https://{}/metadata/health'.format(API), headers=headers, verify=False).ok
    except Exception as e:
        print('Invalid API domain')
        print(e)
        return False


def parseAPI(rawAPI):
    rawAPI = rawAPI.strip('/')
    if rawAPI.startswith('https://'):
        rawAPI = rawAPI[8:]
    elif rawAPI.startswith('http://'):
        rawAPI = rawAPI[7:]
    return rawAPI


def checkCanRollback(tenant):
    if not tenant:
        print('Tenant not found')
        return False
    if not (tenant.migrationTrack and tenant.migrationTrack.status == 'STARTED'):
        print('Tenant has not been tracked for migration or its migration status is not eligible for rollback')
        return False
    if not tenant.migrationTrack.fkImportTracking:
        print('No import tracking record is found')
        return False
    targetVersion = tenant.migrationTrack.version
    if len([stats for stats in tenant.metadataStatistics if stats.version == targetVersion]) != 1:
        print('Tenant must have only one stats for original active data collection')
        return False
    if not checkAPI():
        print('Failed to verify API')
        return False
    return True


def getArgs():
    parser = argparse.ArgumentParser(description='Parse conn variables')
    parser.add_argument('-u', dest='user', type=str)
    parser.add_argument('-p', dest='pwd', type=str)
    parser.add_argument('-x', dest='host', type=str)
    parser.add_argument('-d', dest='db', type=str, default='PLS_MultiTenant')
    parser.add_argument('-t', dest='tenant', type=str)
    parser.add_argument('-a', dest='api', type=str)
    return parser.parse_args()


def getStorage(args):
    try:
        if not all([item for item in [args.user, args.pwd, args.host, args.db]]):
            raise AttributeError('Missing conn variables')
        return __import__('models').db_storage.DBStorage(args.user, args.pwd, args.host, args.db)
    except AttributeError:
        print('Missing required option(s) for connections.\n{}'.format(USAGE))
        quit(-1)


def getTableHelper(tenant, tableName):
    tables = tenant.metadataTable
    for table in tables:
        if table.name == tableName:
            return table


def rebuildLinks(tenant, dataCollection, tablesMapping):
    """unlink tables of target versions and relink"""
    print('Rebuilding links of version {}'.format(dataCollection.version))
    for link in dataCollection.activeMetadataDataCollectionTable:
        storage.delete(link)
    for role, tableNames in tablesMapping.items():
        for tableName in tableNames:
            link = MetadataDataCollectionTable()
            link.role = role
            link.tenantId = tenant.tenantPid
            link.version = dataCollection.version
            link.fkCollectionId = dataCollection.pid
            link.fkTableId = getTableHelper(tenant, tableName).pid
            link.fkTenantId = tenant.tenantPid
            storage.new(link)


def rollbackTenant(tenant):
    migrationTrack = tenant.migrationTrack
    targetVersion = migrationTrack.version

    dataCollection = migrationTrack.metadataDataCollection
    dataCollection.version = targetVersion

    if len(dataCollection.activeMetadataDataCollectionStatus):
        dataCollection.activeMetadataDataCollectionStatus[0].version = targetVersion
        dataCollection.activeMetadataDataCollectionStatus[0].detail = migrationTrack.collectionStatusDetail
    else:
        dataCollectionStatus = MetadataDataCollectionStatus()
        time = func.now()
        dataCollectionStatus.creationTime, dataCollectionStatus.updateTime = time, time
        dataCollectionStatus.version = targetVersion
        dataCollectionStatus.detail = migrationTrack.collectionStatusDetail
        dataCollectionStatus.fkCollectionId = dataCollection.pid
        dataCollectionStatus.tenantId = tenant.tenantPid
        storage.new(dataCollectionStatus)

    stats = tenant.activeMetadataStatistics[0]
    stats.cubesData = migrationTrack.statsCubesData
    stats.name = migrationTrack.statsName

    rebuildLinks(tenant, dataCollection, migrationTrack.curActiveTable)
    try:
        importMigrateTracking = migrationTrack.importMigrateTracking
        deleteActions(importMigrateTracking)
        deleteTables(importMigrateTracking)
    except Exception as e:
        print('Error encountered while deleting imports and actions')
        print(e)


def deleteActions(importMigrateTracking):
    actionIds = [item for item in [
        importMigrateTracking.report.get('account_action_id'),
        importMigrateTracking.report.get('contact_action_id'),
        importMigrateTracking.report.get('transaction_action_id')
    ] if item is not None]
    for actionId in actionIds:
        action = storage.getByPid('Action', actionId)
        if action:
            storage.delete(action)
        else:
            print('Failed to delete action with Id: {}. Not found'.format(actionId))


def deleteTables(importMigrateTracking):
    tables = []
    if importMigrateTracking.report.get('account_data_tables'):
        tables += importMigrateTracking.report.get('account_data_tables')
    if importMigrateTracking.report.get('contact_data_tables'):
        tables += importMigrateTracking.report.get('contact_data_tables')
    if importMigrateTracking.report.get('transaction_data_tables'):
        tables += importMigrateTracking.report.get('transaction_data_tables')
    customerSpace = importMigrateTracking.tenant.tenantId
    for table in tables:
        try:
            headers = {'MagicAuthentication': 'Security through obscurity!', 'Content-Type': 'application/json'}
            print('Cleanning up table {}'.format(table))
            res = requests.delete('https://{}/metadata/customerspaces/{}/tables/{}'.format(API, customerSpace, table),
                                  headers=headers, verify=False)
            if not res.ok:
                raise Exception('Request returns with an unsuccessful status code')
        except Exception as e:
            print('Unable to delete table {}'.format(table))
            print(e)


if __name__ == '__main__':
    args = getArgs()
    storage = getStorage(args)
    tenant = storage.getByPid('Tenant', args.tenant)
    API = parseAPI(args.api)
    try:
        if checkCanRollback(tenant):
            rollbackTenant(tenant)
        else:
            raise AttributeError('Unable to rollback tenant.')
    except Exception as e:
        if storage:
            storage.rollback()
        raise e
    else:
        tenant.migrationTrack.status = 'FAILED'
        storage.save()
        print('Rollback successful.')
    finally:
        if storage:
            storage.close()
