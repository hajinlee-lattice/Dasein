
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class Table(object):
 
    def __init__(self, name, columns = []):

        self.initFromValues(name, columns)


    def getName(self):
        return self.name_

    def setName(self, n):
        self.name_ = n

    def getColumns(self):
        return self.columns_

    def getColumnNames(self):
        return self.column_names_

    def getColumn(self, colname):
        if colname in self.column_names_:
            idx = self.column_names_.index(colname)
            return self.columns_[idx]
        return None

    def setColumns(self, cc):
        self.columns_ = cc
        self.column_names_ = []
        for c in cc:
            self.column_names_.append( c.getName() )

    def appendColumn(self, c):
        if c.getName() not in self.column_names_:
            self.columns_.append( c )
            self.column_names_.append( c.getName() )

    def removeColumn(self, colname):
        if colname in self.column_names_:
            self.column_names_.remove( colname )
            c = (c for c in self.columns_ if c.getName() == colname).next()
            self.columns_.remove( c )

    def initFromValues(self, name, columns):

        self.name_         = name
        self.columns_      = columns
        self.column_names_ = []
        for c in columns:
            self.column_names_.append( c.getName() )
