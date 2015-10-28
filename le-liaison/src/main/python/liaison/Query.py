
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class Query( object ):

    def __init__( self, name, columns = [], filters = [], entities = [] ):

        self.initFromValues( name, columns, filters, entities )


    def getName( self ):
        return self._name

    def setName( self, n ):
        self._name = n
        return self._name

    def getColumns( self ):
        return self._columns

    def getColumnNames( self ):
        return self._column_names

    def setColumns( self, cc ):
        self._columns = cc
        self._column_names = []
        for c in cc:
            self._column_names.append( c.Name() )
        return self._columns

    def getColumn( self, colname ):
        if colname in self._column_names:
            idx = self._column_names.index(colname)
            return self._columns[idx]
        return None

    def appendColumn( self, c ):
        if c.getName() not in self._column_names:
            self._columns.append( c )
            self._column_names.append( c.Name() )
        return c

    def removeColumn( self, colname ):
        if colname in self._column_names:
            self._column_names.remove( colname )
            c = (c for c in self._columns if c.Name() == colname).next()
            self._columns.remove( c )

    def updateColumn( self, c ):
        if c.getName() in self._column_names:
            idx = self._column_names.index(c.Name())
            self._columns[idx] = c
        else:
            self.appendColumn( c )
        return c

    def getFilters( self ):
        return self._filters

    def setFilters( self, ff ):
        self._filters = ff
        return self._filters

    def getEntities( self ):
        return self._entities

    def setEntities( self, ee ):
        self._entities = ee
        return self._entities

    def initFromValues( self, name, columns, filters, entities ):

        self._name         = name
        self._columns      = columns
        self._column_names = []
        for c in columns:
            self._column_names.append( c.Name() )
        self._filters      = filters
        self._entities     = entities
