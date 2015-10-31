
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class Query(object):

    def __init__(self, name, columns = [], filters = [], entities = [], resultSet = None):

        self.initFromValues(name, columns, filters, entities, resultSet)


    def getName(self):
        return self.name_

    def setName(self, n):
        self.name_ = n

    def getColumns(self):
        return self.columns_

    def getColumnNames(self):
        return self.column_names_

    def setColumns(self, cc):
        self.columns_ = cc
        self.column_names_ = []
        for c in cc:
            self.column_names_.append(c.getName())

    def getColumn(self, colname):
        if colname in self.column_names_:
            idx = self.column_names_.index(colname)
            return self.columns_[idx]
        return None

    def appendColumn(self, c):
        if c.getName() not in self.column_names_:
            self.columns_.append(c)
            self.column_names_.append(c.getName())

    def removeColumn(self, colname):
        if colname in self.column_names_:
            self.column_names_.remove(colname)
            c = (c for c in self.columns_ if c.getName() == colname).next()
            self.columns_.remove(c)

    def updateColumn(self, c):
        if c.getName() in self.column_names_:
            idx = self.column_names_.index(c.getName())
            self.columns_[idx] = c
        else:
            self.appendColumn(c)

    def getFilters(self):
        return self.filters_

    def setFilters(self, ff):
        self.filters_ = ff

    def getEntities(self):
        return self.entities_

    def setEntities(self, ee):
        self.entities_ = ee

    def initFromValues(self, name, columns, filters, entities, resultSet):

        self.name_         = name
        self.columns_      = columns
        self.column_names_ = []
        for c in columns:
            self.column_names_.append( c.getName() )
        self.filters_      = filters
        self.entities_     = entities
        self.resultSet_    = resultSet
