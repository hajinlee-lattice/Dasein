#!/usr/local/bin/python

#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import os, sys
import appsequence
import lp020601

PATCH_PATH = os.path.dirname(__file__)
REVISION   = '$Rev$'

print ''
print 'PATH : {0}'.format( PATCH_PATH )
print 'REV  : {0}'.format( REVISION )
print ''

(checkOnly, tenantFileName, resultsFileName) = appsequence.AppArgs.get(sys.argv)

sequence = []
sequence.append(appsequence.LPCheckVersion('2.6.0', True))
sequence.append(lp020601.LP_020601_VDB_LeadRecord_LEDomain())
sequence.append(lp020601.LP_020601_VDB_LeadRecord_Query())
sequence.append(lp020601.LP_020601_DL_LeadRecordReimport())
sequence.append(lp020601.LP_020601_EXEC_LeadRecordReimport())
sequence.append(lp020601.LP_020601_VDB_DomainValidation_Query())
sequence.append(appsequence.LPSetVersion('2.6.1'))

app = appsequence.AppSequence(tenantFileName, resultsFileName, sequence, checkOnly)
app.execute()
