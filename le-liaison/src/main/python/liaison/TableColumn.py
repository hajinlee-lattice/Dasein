
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class TableColumn(object):
 
    def __init__(self, name, tablename, datatype):

        self.initFromValues(name, tablename, datatype)


    def getName(self):
        return self.name_

    def setName(self, n):
        self.name_ = n

    def getTableName(self):
        return self.tablename_

    def setTableName(self, tn):
        self.tablename_ = tn

    def getDatatype(self):
        return self.datatype_

    def setDatatype(self, dt):
        self.datatype_ = dt

    def definition( self ):
        raise NotImplementedError( 'TableColumn.Definition()' )

    def initFromValues(self, name, tablename, datatype):

        self.name_         = name
        self.tablename_    = tablename
        self.datatype_     = datatype

