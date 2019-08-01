import argparse
import sys
from os import getenv
from sqlalchemy.sql import func

sys.path.append('{}/le-migration/python'.format(getenv('WSHOME')))
from models.metadata_data_collection_table import MetadataDataCollectionTable
from models.metadata_data_collection_status import MetadataDataCollectionStatus

USAGE = 'Usage: rollbackTenant -u <username> -p <password> -x <host> [-d <db name>] -t TENANT_PID'


def checkCanRollback(tenant):
    if not tenant:
        print('Tenant not found')
        return False
    if not (tenant.migrationTrack and tenant.migrationTrack.status == 'STARTED'):
        print('Tenant has not been tracked for migration or its migration status is not eligible for rollback')
        return False
    # TODO - uncomment this after importTracking table is in
    # if not tenant.migrationTrack.fkImportTracking:
    #     print('No import tracking record is found')
    #     return False
    targetVersion = tenant.migrationTrack.version
    if len([stats for stats in tenant.metadataStatistics if stats.version == targetVersion]) != 1:
        print('Tenant must have only one stats for original active data collection')
        return False
    return True


def getArgs():
    parser = argparse.ArgumentParser(description='Parse conn variables')
    parser.add_argument('-u', dest='user', type=str)
    parser.add_argument('-p', dest='pwd', type=str)
    parser.add_argument('-x', dest='host', type=str)
    parser.add_argument('-d', dest='db', type=str, default='PLS_MultiTenant')
    parser.add_argument('-t', dest='tenant', type=str)
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


def rebuildLinks(storage, tenant, dataCollection, tablesMapping):
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


def rollbackTenant(storage, tenant):
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

    rebuildLinks(storage, tenant, dataCollection, migrationTrack.curActiveTable)
    # TODO - add rollback import after importTracking table is in


if __name__ == '__main__':
    args = getArgs()
    storage = getStorage(args)
    tenant = storage.getByPid('Tenant', args.tenant)
    try:
        if checkCanRollback(tenant):
            rollbackTenant(storage, tenant)
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
