'''
Created on Dec 1, 2015

@author: smeng
'''

import logging
import argparse
from zkutility import ZookeeperUtility

def main():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest='command')

    # getOrgId
    parser_getOrgId = subparsers.add_parser('getOrgId', help='get org id')
    parser_getOrgId.add_argument('-pod', '--PodID', dest='pod', action='store', required=False, help='pod id', default='QA')
    parser_getOrgId.add_argument('-t', '--Tenant', dest='tenant', action='store', required=True, help='tenant id')

    # listTenants
    parser_listTenants = subparsers.add_parser('listTenants', help='list all tenants')
    parser_listTenants.add_argument('-pod', '--PodID', dest='pod', action='store', required=False, help='pod id', default='QA')

    # cleanUpOrgId
    parser_cleanUpOrgId = subparsers.add_parser('cleanUpOrgId', help='clean up org id')
    parser_cleanUpOrgId.add_argument('-pod', '--PodID', dest='pod', action='store', required=False, help='pod id', default='QA')
    parser_cleanUpOrgId.add_argument('-orgid', '--OrgID', dest='orgid', action='store', required=True, help='org id')

    # updateOrgId
    parser_updateOrgId = subparsers.add_parser('updateOrgId', help='update org id for a tenant')
    parser_updateOrgId.add_argument('-pod', '--PodID', dest='pod', action='store', required=False, help='pod id', default='QA')
    parser_updateOrgId.add_argument('-orgid', '--OrgID', dest='orgid', action='store', required=True, help='org id')
    parser_updateOrgId.add_argument('-t', '--Tenant', dest='tenant', action='store', required=True, help='tenant id')

    # getProperties
    parser_getProperties = subparsers.add_parser('getProperties', help='get properties for a tenant')
    parser_getProperties.add_argument('-pod', '--PodID', dest='pod', action='store', required=False, help='pod id', default='QA')
    parser_getProperties.add_argument('-t', '--Tenant', dest='tenant', action='store', required=True, help='tenant id')

    # deleteTenant
    parser_deleteTenant = subparsers.add_parser('deleteTenant', help='delete a tenant')
    parser_deleteTenant.add_argument('-pod', '--PodID', dest='pod', action='store', required=False, help='pod id', default='QA')
    parser_deleteTenant.add_argument('-t', '--Tenant', dest='tenant', action='store', required=True, help='tenant id')

    # displayNode
    parser_displayNode = subparsers.add_parser('displayNode', help='display the value of a node')
    parser_displayNode.add_argument('-pod', '--PodID', dest='pod', action='store', required=False, help='pod id', default='QA')
    parser_displayNode.add_argument('-path', '--Path', dest='path', action='store', required=True, help='node path')

    # exists
    parser_exists = subparsers.add_parser('exists', help='check if a tenant exists')
    parser_exists.add_argument('-pod', '--PodID', dest='pod', action='store', required=False, help='pod id', default='QA')
    parser_exists.add_argument('-t', '--Tenant', dest='tenant', action='store', required=True, help='tenant id')

    args = parser.parse_args()

    zkutil = ZookeeperUtility(pod=args.pod)
    if args.command == 'getOrgId':
        print zkutil.getOrgId(args.tenant)
    elif args.command == 'listTenants':
        zkutil.listTenants()
    elif args.command == 'cleanUpOrgId':
        zkutil.cleanUpOrgId(args.orgid)
    elif args.command == 'updateOrgId':
        zkutil.updateOrgId(args.tenant, args.orgid)
    elif args.command == 'getProperties':
        print zkutil.getProperties(args.tenant)
    elif args.command == 'deleteTenant':
        zkutil.deleteTenant(args.tenant)
    elif args.command == 'displayNode':
        zkutil.displayNode(args.path)
    elif args.command == 'exists':
        print zkutil.exists(args.tenant)
    else:
        logging.error('No such function: ' + args.function_name)


if __name__ == '__main__':
    main()
