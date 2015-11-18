#!/usr/bin/python

import sys, datetime, time, requests, re
from liaison import *

def GetVersionForTenants( tenantFileName, versionFileName ):
    
    tenants = []
    
    with open( tenantFileName ) as tenantFile:
        for line in tenantFile:
            cols = line.strip().split(',')
            tenants.append(cols[0])

    with open( versionFileName, mode = 'w' ) as versionFile:

        versionFile.write( 'TenantName,Version,CallTime\n' )
        
        for t in tenants:

            print '{0}...'.format( t )

            version = 'Unknown'
            calltime = 0.0
            isLP = False

            try:
                conn_mgr = ConnectionMgrFactory.Create( 'visiDB', tenant_name=t, verify=False, verbose=True )
                isLP = True
            except TenantNotMappedToURL:
                version = 'Not on LP DataLoader'
            
            t0 = time.time()

            if isLP:
                try:
                    exp_version = conn_mgr.getNamedExpression( 'Version' )
                    defn = exp_version.Object().definition()
                    c = re.search( 'LatticeFunctionExpressionConstant\(\"(.*?)\".*LatticeFunctionExpressionConstant\(\"(.*?)\"', defn )
                    if c:
                        version = c.group(2)
                    else:
                        version = 'Nonstandard version'
                except requests.exceptions.SSLError:
                    ## Not on a PROD DataLoader
                    pass
                except TenantNotFoundAtURL:
                    ## Not on a PROD DataLoader
                    pass
                except UnknownVisiDBSpec:
                    version = 'No template version'

            t1 = time.time()

            calltime = t1-t0
            
            versionFile.write( '{0},{1},{2}\n'.format(t,version,calltime) )


def Usage( cmd, exit_code ):
    path = ''
    i = cmd.rfind('\\')
    if( i != -1 ):
        path = cmd[:i]
        cmd = cmd[i+1:]
    
    print ''
    print 'Usage: {0} <tenant_list.csv> <version_data.csv>'.format( cmd )
    print ''
    
    exit( exit_code )


if __name__ == "__main__":

    if len(sys.argv) == 1:
        Usage( sys.argv[0], 0 )
    
    if len(sys.argv) != 3:
        Usage( sys.argv[0], 1 )

    tenantFileName = sys.argv[1]
    versionFileName = sys.argv[2]
    
    GetVersionForTenants( tenantFileName, versionFileName )
