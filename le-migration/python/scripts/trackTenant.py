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


def checkCanTrack(storage, tenant=None):
    if not tenant:
        print('Tenant not found.')
        return False
    elif not len(tenant.metadataDataCollection):
        print("Tenant doesn't have an active data collection.")
        return False
    elif len(tenant.metadataDataCollection) != 1:
        print("Tenant has invalid number of active data collections")
        return False
    exist = storage.getByColumn('MigrationTrack', 'fkTenantId', tenant.tenantPid)
    if len(exist):
        print('Tenant already in tracking table:\n')
        print(exist[0])
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
    track.status = 'SCHEDULED'
    track.version = tenant.metadataDataCollection[0].version
    track.curActiveTable = {}
    for roleTable in tenant.metadataDataCollectionTable:
        track.curActiveTable[roleTable.role] = roleTable.fkTableId
    # TODO - double check with Jinyang for import action (maybe list of ACTION.PID).
    # TODO - May need to write model for ACTION table
    track.importAction = {'actions': []}
    track.collectionStatusDetail = tenant.metadataDataCollectionStatus[0].detail
    track.statsCubesData = tenant.metadataStatistics[0].cubesData
    track.statsData = tenant.metadataStatistics[0].data
    track.statsName = tenant.metadataStatistics[0].name
    track.save()
    return track


if __name__ == '__main__':
    checkEnvironment()
    args = getArgs()
    storage = getStorage(args)
    models.storage = storage
    tenant = storage.getByPid('Tenant', args.tenant)
    if checkCanTrack(storage, tenant):
        migrationTrack = createTenantTrack(tenant)
        print('Tenant added to tracking table:\n')
        print(migrationTrack)
    storage.close()
    print('\n=====\n')
