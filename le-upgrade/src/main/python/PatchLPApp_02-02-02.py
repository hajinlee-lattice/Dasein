#!/usr/bin/python

#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import os, sys
import appsequence
import lp020202

PATCH_PATH = os.path.dirname(__file__)
REVISION   = '$Rev$'

print ''
print 'PATH : {0}'.format( PATCH_PATH )
print 'REV  : {0}'.format( REVISION )
print ''

(checkOnly, tenantFileName, resultsFileName) = appsequence.AppArgs.get(sys.argv)

sequence = []
sequence.append( appsequence.LPCheckVersion('2.2.1') )
sequence.append( lp020202.LP_020202_SupportBulkScoringWithoutWriteBack() )
sequence.append( appsequence.LPSetVersion('2.2.2') )

app = appsequence.AppSequence( tenantFileName, resultsFileName, sequence, checkOnly )
app.execute()
