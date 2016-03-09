
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import datetime
from .exceptions import VisiDBQueryResultsNotReady
from .QueryResult import QueryResult


class QueryResultVDBImpl(QueryResult):

    def __init__(self, conn_mgr, query_handle):

        super(QueryResultVDBImpl, self).__init__(conn_mgr, query_handle)
        self.reset()
        self._last_status = None
        self._last_status_update = datetime.datetime.now() - datetime.timedelta(365)
        self._col_names       = None
        self._col_datatypes   = None


    def isReady(self):

        if (datetime.datetime.now() - self._last_status_update) > datetime.timedelta(0,5):
            self.getStatus()

        if self._last_status == 'Completed':
            return True

        return False


    def getStatus(self):

        if (     self._last_status != 'Completed'
                 and (datetime.datetime.now() - self._last_status_update) > datetime.timedelta(0,5) ):
            self._last_status = self.connectionMgr().getQueryStatus(self.queryHandle())
            self._last_status_update = datetime.datetime.now()

        return self._last_status


    def columnNames(self):

        if not self.isReady():
            raise VisiDBQueryResultsNotReady('Query results are not yet ready for handle {0}'.format(self.queryHandle()))

        if self._col_names is None:
            (self._col_names, self._col_datatypes, self._rows_total, rows ) = self.connectionMgr().getQueryResults( self.queryHandle(), 0, 0)

        return self._col_names


    def fetchAll(self):
        return self.fetchMany(-1)


    def fetchMany(self, n_rows):

        if not self.isReady():
            raise VisiDBQueryResultsNotReady('Query results are not yet ready for handle {0}'.format(self.queryHandle()))

        ## Case 1: First time downloading results.
        ## Need to set set _rows_total

        if self._rows_total is None:
            (self._col_names, self._col_datatypes, remaining_rows, rows) = self.connectionMgr().getQueryResults(self.queryHandle(), 0, n_rows)
            if remaining_rows > 0:
                self._rows_downloaded = n_rows
                self._rows_total = n_rows + remaining_rows
            else:
                self._rows_downloaded = len(rows)
                self._rows_total = self._rows_downloaded
            return rows

        ## Case 2: Not the first time downloading results and there are more to download

        if self._rows_downloaded < self._rows_total:
            (self._col_names, self._col_datatypes, remaining_rows, rows) = self.connectionMgr().getQueryResults(self.queryHandle(), self._rows_downloaded, n_rows)
            if remaining_rows > 0:
                self._rows_downloaded = self._rows_downloaded + n_rows
            else:
                self._rows_downloaded = self._rows_downloaded + len(rows)
            return rows

        ## Case 3: Results have been downloaded already

        return None


    def reset(self):
        self._rows_total      = None
        self._rows_downloaded = 0
