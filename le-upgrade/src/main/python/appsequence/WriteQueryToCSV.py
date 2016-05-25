
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import sys, time, codecs
from liaison import *

from .Applicability import Applicability
from .AppSequence   import AppSequence
from .StepBase      import StepBase

class WriteQueryToCSV(StepBase):

    name        = 'WriteQueryToCSV'
    description = 'Execute a query and write the results to a CSV file'
    version     = '$Rev$'


    def __init__(self, queryname, timestamp, forceApply=False):
        self._queryname = queryname
        self._timestamp = timestamp
        super(WriteQueryToCSV, self).__init__(forceApply)


    def getApplicability(self, appseq):
        try:
            q = appseq.getConnectionMgr().getQuery(self._queryname)
        except UnknownVisiDBSpec:
            return Applicability.cannotApplyFail

        return Applicability.canApply


    def apply( self, appseq ):
        print '\n    * Executing query ({0}) and writing to CSV . .'.format(self._queryname),
        sys.stdout.flush()

        conn_mgr = appseq.getConnectionMgr()
        queryResult = conn_mgr.executeQuery(self._queryname)

        while(not queryResult.isReady()):
            print '.',
            sys.stdout.flush()
            time.sleep(5)

        print '.'
        csvFileName = self._queryname.replace('Q_',conn_mgr.getTenantName()+'_') + '_' + self._timestamp.strftime('%Y%m%d_%H%M%S') + '.csv'
        nRowsRead = 0
        with codecs.open(csvFileName, encoding = 'utf-8', mode = 'w') as csvFile:
            csvFile.write(self._createUnicodeDelimited(queryResult.columnNames(), u',') + u'\n')

            while True:
                queryRows = queryResult.fetchMany(5000)
                if queryRows is None:
                    break
                for row in queryRows:
                    csvFile.write(self._createUnicodeDelimited(row, u',') + u'\n')
                    nRowsRead += 1

        print '      => Query returned {0} rows, and they were written to \"{1}\"'.format(nRowsRead, csvFileName),
        return True


    def _createUnicodeDelimited(self, e, d):
        s = u''
        first_word = True
        for w in e:
            if type(w) is str:
                try:
                   w = unicode(w)
                except:
                  sys.stderr.write('le_unicode_csv.create_unicode_delim(): invalid character in string for this encoding, setting to NULL\n')
                  w = u''
            if type(w) is bytearray:
                hex = ['0x']
                for aChar in w:
                    hex.append('{0:02X}'.format(aChar))
                w = ''.join(hex).strip()
            has_quote = False
            if (w is not None) and (u'"' in unicode(w)):
                w = w.replace('"','""')
                has_quote = True
            if (w is not None) and (u'\n' in unicode(w)):
                has_quote = True
            if (w is not None) and (d in unicode(w) or has_quote):
                w = u'"' + w + u'"'
            if(not first_word):
                s += d
            if type(w) is bool:
                if w is True:
                    s += u'1'
                else:
                    s += u'0'
            elif w is not None:
                s += unicode(w)
            first_word = False
        return s
