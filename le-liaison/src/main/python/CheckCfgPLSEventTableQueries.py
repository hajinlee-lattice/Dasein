import sys, datetime, time, requests, re
from liaison import *

def CheckCfgPLSEventTableQueries( tenantFileName, resultsFileName ):
    
    tenants = []
    
    with open( tenantFileName ) as tenantFile:
        for line in tenantFile:
            cols = line.strip().split(',')
            tenants.append(cols[0])

    with open( resultsFileName, mode = 'w' ) as resultsFile:

        resultsFile.write( 'TenantName,OperationalConfig\n' )
        
        for t in tenants:

            opcfg = False

            print '{0}'.format( t ),

            try:
                conn_mgr = ConnectionMgrFactory.Create( 'visiDB', tenant_name=t )
            except TenantNotMappedToURL:
                print 'Not on LP DataLoader'
                continue
            
            query_result = conn_mgr.executeQuery( 'View_Table_Cfg_PLS_EventTableQueries' )

            while( not query_result.IsReady() ):
                print '.',
                time.sleep(5)

            print '.'

            query_rows = query_result.FetchAll()
            if len(query_rows) > 0:
                (rn, kernel_qname, model_qname, model_fname, bulk_qname, bulk_fname, incr_qname, incr_fname, event_fname) = query_rows[0]
            
                if( bulk_qname is not None and bulk_qname == 'PLS_Scoring_Bulk' and incr_qname is not None and incr_qname == 'PLS_Scoring_Incremental' ):
                    opcfg = True
                
            resultsFile.write( '{0},{1}\n'.format(t,opcfg) )


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
    
    CheckCfgPLSEventTableQueries( tenantFileName, resultsFileName )