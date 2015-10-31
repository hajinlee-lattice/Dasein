
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import datetime
from .exceptions import VisiDBQueryResultsNotReady
from .QueryResult import QueryResult


class QueryResultVDBImpl( QueryResult ):

    def __init__( self, conn_mgr, query_handle ):

        super( QueryResultVDBImpl, self ).__init__( conn_mgr, query_handle )
        self.Reset()
        self._last_status = None
        self._last_status_update = datetime.datetime.now() - datetime.timedelta(365)
        self._col_names       = None
        self._col_datatypes   = None


    def IsReady( self ):

        if ( datetime.datetime.now() - self._last_status_update ) > datetime.timedelta(0,5):
            self.GetStatus()

        if self._last_status == 'Completed':
            return True

        return False


    def GetStatus( self ):

        if (     self._last_status != 'Completed'
                 and ( datetime.datetime.now() - self._last_status_update ) > datetime.timedelta(0,5) ):
            self._last_status = self.ConnectionMgr().getQueryStatus( self.QueryHandle() )
            self._last_status_update = datetime.datetime.now()

        return self._last_status


    def ColumnNames( self ):
        
        if not self.IsReady():
            raise VisiDBQueryResultsNotReady( 'Query results are not yet ready for handle {0}'.format(self.QueryHandle()) )

        if self._col_names is None:
            ( self._col_names, self._col_datatypes, self._rows_total, rows ) = self.ConnectionMgr().GetQueryResults( self.QueryHandle(), 0, 0 )

        return self._col_names


    def FetchAll( self ):
        return self.FetchMany( -1 )


    def FetchMany( self, n_rows ):
        
        if not self.IsReady():
            raise VisiDBQueryResultsNotReady( 'Query results are not yet ready for handle {0}'.format(self.QueryHandle()) )

        ## Case 1: First time downloading results.
        ## Need to set set _rows_total

        if self._rows_total is None:
            ( self._col_names, self._col_datatypes, remaining_rows, rows ) = self.ConnectionMgr().getQueryResults( self.QueryHandle(), 0, n_rows )
            if remaining_rows > 0:
                self._rows_downloaded = n_rows
                self._rows_total = n_rows + remaining_rows
            else:
                self._rows_downloaded = len(rows)
                self._rows_total = self._rows_downloaded
            return rows

        ## Case 2: Not the first time downloading results and there are more to download

        if self._rows_downloaded < self._rows_total:
            ( self._col_names, self._col_datatypes, remaining_rows, rows ) = self.ConnectionMgr().GetQueryResults( self.QueryHandle(), self._rows_downloaded, n_rows )
            if remaining_rows > 0:
                self._rows_downloaded = self._rows_downloaded + n_rows
            else:
                self._rows_downloaded = self._rows_downloaded + len(rows)
            return rows

        ## Case 3: Results have been downloaded already

        return None


    def Reset( self ):
        self._rows_total      = None
        self._rows_downloaded = 0

