
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class QueryResult(object):

    def __init__(self, conn_mgr, query_handle = None):

        self._conn_mgr = conn_mgr
        self._qhandle  = query_handle


    def isReady(self):
        raise NotImplementedError('QueryResult.isReady()')


    def getStatus(self):
        raise NotImplementedError('QueryResult.getStatus()')


    def columnNames(self):
        raise NotImplementedError('QueryResult.columnNames()')


    def fetchAll(self):
        raise NotImplementedError('QueryResult.fetchAll()')


    def fetchMany(self, n_rows):
        raise NotImplementedError('QueryResult.fetchMany()')


    def reset(self):
        raise NotImplementedError('QueryResult.reset()')


    def connectionMgr(self):
        return self._conn_mgr


    def queryHandle(self):
        return self._qhandle
