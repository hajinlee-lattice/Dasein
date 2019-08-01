import argparse
import sys
from os import getenv

sys.path.append('{}/le-migration/python'.format(getenv('WSHOME')))

STATUS = ["STARTED", "FAILED", "COMPLETED"]
USAGE = 'Usage: updateMigrationStatus -u <username> -p <password> -x <host> [-d <db name>] -t TENANT_PID -s <STARTED/FAILED/COMPLETED>\n'


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


def getArgs():
    parser = argparse.ArgumentParser(description='Parse conn variables')
    parser.add_argument('-u', dest='user', type=str)
    parser.add_argument('-p', dest='pwd', type=str)
    parser.add_argument('-x', dest='host', type=str)
    parser.add_argument('-d', dest='db', type=str, default='PLS_MultiTenant')
    parser.add_argument('-t', dest='tenant', type=str)  # tenant PID
    parser.add_argument('-s', dest='status', type=str)
    args = parser.parse_args()
    if args.status:
        args.status = args.status.upper()
    return args


def getStorage(args):
    if any([not item for item in [args.user, args.pwd, args.host, args.db]]):
        raise Exception('Missing conn variables.\n{}'.format(USAGE))
    return __import__('models').db_storage.DBStorage(args.user, args.pwd, args.host, args.db)


def checkCanUpdate(tenant, args):
    if not tenant:
        print('Tenant not found.')
        return False
    elif not tenant.migrationTrack:
        print('Tenant has never been tracked for migration')
        return False
    elif not args.status:
        print('Missing status')
        return False
    elif args.status not in STATUS:
        print('{} is not a valid status.\n{}'.format(args.status, USAGE))
        return False
    return True


if __name__ == '__main__':
    checkEnvironment()
    args, storage = None, None
    try:
        args = getArgs()
        if not args.tenant:
            raise Exception('Missing tenant. \n{}'.format(USAGE))
        storage = getStorage(args)
    except Exception as e:
        print(e)
    else:
        tenant = storage.getByPid('Tenant', args.tenant)
        if checkCanUpdate(tenant, args):
            print('\nUpdating migration status for tenant {} to {}\n'.format(tenant.tenantId, args.status))
            tenant.migrationTrack.status = args.status
            storage.save()
            print('\nUpdated tenant {} status to {}\n'.format(tenant.tenantId, args.status))
    finally:
        if storage:
            storage.close()
