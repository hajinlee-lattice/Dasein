#!/usr/bin/python

#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import os, sys
import appsequence
import lp020001

PATCH_PATH = os.path.dirname(__file__)
REVISION   = '$Rev$'

print ''
print 'PATH : {0}'.format( PATCH_PATH )
print 'REV  : {0}'.format( REVISION )
print ''

(checkOnly, tenantFileName, resultsFileName) = appsequence.AppArgs.get(sys.argv)

sequence = []
sequence.append( appsequence.LPCheckVersion('2.0') )
sequence.append( lp020001.LP_020001_Rescoring() )
sequence.append( lp020001.LP_020001_RescoringDisable(forceApply=True) )
sequence.append( lp020001.LP_020001_ReportsDB() )
sequence.append( lp020001.LP_020001_Diagnostic() )
sequence.append( appsequence.LPSetVersion('2.0.1') )

app = appsequence.AppSequence( tenantFileName, resultsFileName, sequence, checkOnly )
app.execute()
