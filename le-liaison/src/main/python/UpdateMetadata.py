
import sys
from liaison import *


def UpdateMetadata( tenant, url, verify ):

    query_name = 'Q_PLS_Modeling'

    conn_mgr = ConnectionMgrFactory.Create( 'visiDB', tenant_name=tenant, dataloader_url=url, verify=verify )
    print 'Initializing...',
    modelcols = conn_mgr.GetMetadata( query_name )
    print 'Done'

    updates = []
    cmd = ''

    while( cmd.lower() != 'exit' ):
        print ''
        cmd = raw_input('(list, get, set, refresh, commit, exit): ')

        if cmd.lower() == 'list':
            PrintModelColumns( modelcols )

        elif cmd.lower() == 'get':
            PrintMetadataForColumn( modelcols )

        elif cmd.lower() == 'set':
            colname  = raw_input('Column name: ')
            type = raw_input('Metadata type: ')
            value = raw_input('Metadata value: ')
            print ''
            updates.append( (colname,type,value) )

        elif cmd.lower() == 'refresh':
            modelcols = RefreshCache( conn_mgr, query_name )

        elif cmd.lower() == 'commit':
            print 'Committing...',
            q = conn_mgr.GetQuery( query_name )
            for (colname,type,value) in updates:
                if colname not in q.getColumnNames():
                    print ''
                    print 'Column \'{0}\' does not exist in query; ignoring'.format( colname )
                    continue
                qc = q.getColumn( colname )
                qc.SetMetadata( type, value )
                q.updateColumn( qc )
            conn_mgr.SetQuery( q )
            print 'Done'
            updates = []
            modelcols = RefreshCache( conn_mgr, query_name )


def PrintModelColumns( modelcols ):

    colnames = sorted( list(modelcols.keys()) )
    n_rows     = len(colnames)/3
    extraidx = len(colnames)%3
    offset = 0
    if extraidx > 0:
        offset = 1
    i = 0
    print ''
    print 'There are a total of {0} columns.'.format( len(colnames) )
    print ''
    while i < n_rows:
        if i > 0 and i % 30 == 0:
            print ''
            cont = raw_input('Press \'Enter\' to continue...')
            print ''
        v1 = FormatValue( colnames[i], 25 )
        v2 = FormatValue( colnames[i+n_rows+offset], 25 )
        v3 = FormatValue( colnames[i+2*n_rows+offset], 25 )
        print '{0:25} {1:25} {2:25}'.format( v1, v2, v3 )
        i += 1
    if extraidx == 1:
        v1 = FormatValue( colnames[n_rows], 25 )
        print '{0:25}'.format( v1 )
    elif extraidx == 2:
        v1 = FormatValue( colnames[n_rows], 25 )
        v2 = FormatValue( colnames[n_rows+n_rows+offset], 25 )
        print '{0:25} {1:25}'.format( v1, v2 )
    print ''


def PrintMetadataForColumn( modelcols ):

    candidates = []
    while True:
        candidates = []
        col = raw_input('Column name: ')
        if col == '':
            break
        if col in modelcols:
            candidates.append(col)
            break
        for name in modelcols:
            if name[:len(col)] == col:
                candidates.append(name)
        if len(candidates) < 2:
            break
        print ''
        print 'Possible columns are:'
        print ''
        for c in candidates:
            print '  * {0}'.format( c )
        print ''

    if len(candidates) == 0:
        return

    name = candidates[0]
    md = modelcols[name]

    print ''
    print 'Metadata for column \'{0}\':'.format( name )
    print ''
    for t, v in md.items():
        if v is None:
            v = '<empty>'
        if t in ['DataType','DataSource']:
            t = t + ' (read-only)'
        print '  * {0:23}: {1}'.format( t, v.encode('ascii','ignore') )


def RefreshCache( conn_mgr, query_name ):
    print 'Refreshing cache...',
    modelcols = conn_mgr.GetMetadata( query_name )
    print 'Done'
    return modelcols


def FormatValue( v, max_length ):
    v = v.encode('ascii','ignore')
    if len(v) > max_length:
            v = v[:max_length-3] + '...'
    return v


def Usage( cmd, exit_code ):
    path = ''
    i = cmd.rfind('\\')
    if( i != -1 ):
        path = cmd[:i]
        cmd = cmd[i+1:]
    
    print ''
    print 'Usage: {0} --tenant <tenant_name_in_prod>'.format( cmd )
    print 'Usage: {0} --qatenant <tenant_name_in_qa> <dataloader_url>'.format( cmd )
    print ''
    
    exit( exit_code )


if __name__ == "__main__":

    if len(sys.argv) == 1:
        Usage( sys.argv[0], 0 )
    
    option = sys.argv[1]

    if option not in ['--tenant','--qatenant']:
        Usage( sys.argv[0], 1 )

    tenant = None
    url = None

    if option == '--tenant':
        if len(sys.argv) != 3:
            Usage( sys.argv[0], 1 )
        tenant = sys.argv[2]
        verify = True

    elif option == '--qatenant':
        if len(sys.argv) != 4:
            Usage( sys.argv[0], 1 )
        tenant = sys.argv[2]
        url = sys.argv[3]
        verify = False
    
    UpdateMetadata( tenant, url, verify )
