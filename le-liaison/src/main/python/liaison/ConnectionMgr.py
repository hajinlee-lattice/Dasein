
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class ConnectionMgr(object):

    def __init__(self, type, verbose = False):

        self.type_    = type
        self.verbose_ = verbose

    def getType(self):
        return self.type_

    def isVerbose(self):
        return self.verbose_

    def getTable(self, table_name):
        raise NotImplementedError('ConnectionMgr.getTable()')

    def setTable(self, table):
        raise NotImplementedError('ConnectionMgr.setTable()')

    def getNamedExpression(self, expression_name):
        raise NotImplementedError('ConnectionMgr.getNamedExpression()')

    def setNamedExpression(self, expression):
        raise NotImplementedError('ConnectionMgr.setNamedExpression()')

    def getQuery(self, query_name):
        raise NotImplementedError('ConnectionMgr.getQuery()')

    def getMetadata(self, query_name):
        raise NotImplementedError('ConnectionMgr.getMetadata()')

    def setQuery(self, query):
        raise NotImplementedError('ConnectionMgr.setQuery()')

    def executeQuery(self, query_name):
        raise NotImplementedError('ConnectionMgr.executeQuery()')

    def getLoadGroupMgr(self):
        raise NotImplementedError('ConnectionMgr.getLoadGroupMgr()')
