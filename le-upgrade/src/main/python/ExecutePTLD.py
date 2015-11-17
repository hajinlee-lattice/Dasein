#!/usr/bin/python

import sys, datetime, time, requests, re
import appsequence
from liaison import *

def ExecutePTLD( tenantFileName, resultsFileName ):
    
    tenants = []
    
    with open( tenantFileName ) as tenantFile:
        for line in tenantFile:
            cols = line.strip().split(',')
            tenants.append(cols[0])

    with open( resultsFileName, mode = 'w' ) as resultsFile:

        resultsFile.write( 'TenantName,LaunchID\n' )
        
        for t in tenants:

            print '{0}'.format( t )

            try:
                conn_mgr = ConnectionMgrFactory.Create( 'visiDB', tenant_name=t )
            except TenantNotMappedToURL:
                print 'Not on LP DataLoader'
                continue

            launchid = conn_mgr.executeGroup( 'PushToLeadDestination', 'mwilson@lattice-engines.com' )
                
            resultsFile.write( '{0},{1}\n'.format(t,launchid) )


def Usage( cmd, exit_code ):
    path = ''
    i = cmd.rfind('\\')
    if( i != -1 ):
        path = cmd[:i]
        cmd = cmd[i+1:]
    
    print ''
    print 'Usage: {0} <tenant_list.csv> <results.csv>'.format( cmd )
    print ''
    
    exit( exit_code )


if __name__ == "__main__":

    if len(sys.argv) == 1:
        Usage( sys.argv[0], 0 )
    
    if len(sys.argv) != 3:
        Usage( sys.argv[0], 1 )

    tenantFileName = sys.argv[1]
    resultsFileName = sys.argv[2]
    
    ExecutePTLD( tenantFileName, resultsFileName )
