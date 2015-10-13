
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class QueryResult( object ):
 
    def __init__( self, conn_mgr, query_handle = None ):

        self._conn_mgr = conn_mgr
        self._qhandle  = query_handle


    def IsReady( self ):
        raise NotImplementedError( 'QueryResult.IsReady()' )


    def GetStatus( self ):
        raise NotImplementedError( 'QueryResult.GetStatus()' )


    def ColumnNames( self ):
        raise NotImplementedError( 'QueryResult.ColumnNames()' )


    def FetchAll( self ):
        raise NotImplementedError( 'QueryResult.FetchAll()' )


    def FetchMany( self, n_rows ):
        raise NotImplementedError( 'QueryResult.FetchMany()' )


    def Reset( self ):
        raise NotImplementedError( 'QueryResult.Reset()' )

    
    def ConnectionMgr( self ):
        return self._conn_mgr


    def QueryHandle( self ):
        return self._qhandle
    