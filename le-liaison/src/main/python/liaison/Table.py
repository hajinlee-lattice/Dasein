
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class Table( object ):
 
    def __init__( self, name, columns = [] ):

        self.InitFromValues( name, columns )


    def Name( self ):
        return self._name

    def SetName( self, n ):
        self._name = n
        return self._name

    def Columns( self ):
        return self._columns

    def ColumnNames( self ):
        return self._column_names

    def SetColumns( self, cc ):
        self._columns = cc
        self._column_names = []
        for c in cc:
            self._column_names.append( c.Name() )
        return self._columns

    def AppendColumn( self, c ):
        if c.Name() not in self._column_names:
            self._columns.append( c )
            self._column_names.append( c.Name() )
        return c

    def RemoveColumn( self, colname ):
        if colname in self._column_names:
            self._column_names.remove( colname )
            c = (c for c in self._columns if c.Name() == colname).next()
            self._columns.remove( c )

    def InitFromValues( self, name, columns ):

        self._name         = name
        self._columns      = columns
        self._column_names = []
        for c in columns:
            self._column_names.append( c.Name() )
