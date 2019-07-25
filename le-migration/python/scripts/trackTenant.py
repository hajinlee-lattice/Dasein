import argparse
import sys
from os import getenv, getcwd

sys.path.append('{}/le-migration/python'.format(getenv('WSHOME')))
import models
from models.migration_track import MigrationTrack

"""
import migrating tenant into MIGRATION_TRACK table

Usage: trackTenant -u <username> -p <password> -x <host> [-d <db name>] -t TENANT_PID
"""


def checkEnvironment():
    valid = True
    print('\n===== Checking environment ==========\n')
    if not getenv('WSHOME'):
        print('Environment variable WSHOME is not set')
        valid = False
    if getcwd() != getenv('WSHOME'):
        print('Please run this script at WSHOME')
        valid = False
    print('\n===== Finish checking environment ===\n')
    if not valid:
        quit(-1)


def checkCanTrack(tenant=None):
    if not tenant:
        print('Tenant not found.')
        return False
    elif len(tenant.migrationTrack):
        print('Tenant already being tracked.')
        return False
    elif len(tenant.metadataDataCollection) != 1:
        print('Tenant must have one active data collection')
        return False
    elif len(tenant.metadataDataCollection[0].metadataDataCollectionTable) <= 0:
        print('No records found for the active data collection of this tenant')
        return False
    elif len([stats for stats in tenant.metadataStatistics if
              tenant.metadataDataCollection[0].version == stats.version]) != 1:
        print('Tenant must have one statistics record for its active data collection')
        return False
    elif len(tenant.metadataDataCollection[0].metadataDataCollectionStatus) != 1:
        print('Tenant must have one status record for its active data collection')
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
        if any([not item for item in [args.user, args.pwd, args.host, args.db]]):
            raise AttributeError('Missing conn variables')
        return __import__('models').db_storage.DBStorage(args.user, args.pwd, args.host, args.db)
    except AttributeError:
        print('Missing required option(s) for connections.')
        print('Usage: trackTenant -u <username> -p <password> -x <host> [-d <db name>] -t TENANT_PID\n')
        quit(-1)


def createTenantTrack(tenant):
    track = MigrationTrack()
    track.fkCollectionId = tenant.metadataDataCollection[0].pid
    track.fkTenantId = tenant.tenantPid
    track.status = 'STARTED'
    track.version = tenant.metadataDataCollection[0].version
    track.curActiveTable = {}
    for record in tenant.metadataDataCollection[0].metadataDataCollectionTable:
        if not track.curActiveTable.get(record.role):
            track.curActiveTable[record.role] = [record.metadataTable.name]
        else:
            track.curActiveTable[record.role].append(record.metadataTable.name)
    # TODO - double check with Jinyang for import action (maybe list of ACTION.PID).
    # TODO - If needs to be list of action objects, write model for ACTION object (table)
    track.importAction = {'actions': []}
    if tenant.metadataDataCollectionStatus:
        track.collectionStatusDetail = tenant.metadataDataCollection[0].metadataDataCollectionStatus[0].detail
    if tenant.metadataStatistics:
        stats = [stats for stats in tenant.metadataStatistics if tenant.metadataDataCollection[0].version == stats.version][0]
        track.statsCubesData = stats.cubesData
        track.statsName = stats.name
    track.save()
    return track


if __name__ == '__main__':
    checkEnvironment()
    args = getArgs()
    storage = getStorage(args)
    models.storage = storage
    tenant = storage.getByPid('Tenant', args.tenant)
    if checkCanTrack(tenant):
        migrationTrack = createTenantTrack(tenant)
        print('Tenant added to tracking table:\n')
        print(migrationTrack)
    storage.close()
    print('\n=====\n')
