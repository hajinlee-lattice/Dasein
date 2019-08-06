import argparse
import sys
from os import getenv

sys.path.append('{}/le-migration/python'.format(getenv('WSHOME')))
USAGE = 'Usage: viewTrack -u <username> -p <password> -x <host> [-d <db name>] -t TENANT_PID'


def checkCanView(tenant):
    if not tenant:
        print('Tenant not found.')
        return False
    if not tenant.migrationTrack:
        print('This tenant has not been tracked for migration')
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
        print('Missing required option(s) for connections.')
        print(USAGE)


if __name__ == '__main__':
    args = getArgs()
    storage, tenant = None, None
    try:
        storage = getStorage(args)
        tenant = storage.getByPid('Tenant', args.tenant)
        if checkCanView(tenant):
            print(tenant.migrationTrack)
    except Exception as e:
        print(e)
    finally:
        if storage:
            storage.close()
