
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class TableColumn( object ):
 
    def __init__( self, name, tablename, datatype ):

        self.InitFromValues( name, tablename, datatype )


    def Name( self ):
        return self._name

    def SetName( self, n ):
        self._name = n
        return self._name

    def TableName( self ):
        return self._tablename

    def SetTableName( self, tn ):
        self._tablename = tn
        return self._tablename

    def Datatype( self ):
        return self._datatype

    def SetDatatype( self, dt ):
        self._datatype = dt
        return self._datatype


    
    def Definition( self ):
        raise NotImplementedError( 'TableColumn.Definition()' )

    def InitFromValues( self, name, tablename, datatype ):

        self._name         = name
        self._tablename    = tablename
        self._datatype     = datatype

