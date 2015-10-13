
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class ConnectionMgr( object ):
    
    def __init__( self, type, verbose = False ):

        self._type = type
        self._verbose = verbose

    def Type( self ):
        return self._type

    def IsVerbose( self ):
        return self._verbose

    def GetTable( self, table_name ):
        raise NotImplementedError( 'ConnectionMgr.GetTable()' )

    def SetTable( self, table ):
        raise NotImplementedError( 'ConnectionMgr.SetTable()' )

    def GetNamedExpression( self, expression_name ):
        raise NotImplementedError( 'ConnectionMgr.GetExpression()' )

    def SetNamedExpression( self, expression ):
        raise NotImplementedError( 'ConnectionMgr.SetExpression()' )

    def GetQuery( self, query_name ):
        raise NotImplementedError( 'ConnectionMgr.GetQuery()' )

    def GetMetadata( self, query_name ):
        raise NotImplementedError( 'ConnectionMgr.GetMetadata()' )

    def SetQuery( self, query ):
        raise NotImplementedError( 'ConnectionMgr.SetQuery()' )

    def ExecuteQuery( self, query_name ):
        raise NotImplementedError( 'ConnectionMgr.ExecuteQuery()' )

    def GetLoadGroupMgr( self ):
        raise NotImplementedError( 'ConnectionMgr.GetDataLoaderMgr()' )
