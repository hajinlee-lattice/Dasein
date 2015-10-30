
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import os, sys
import appsequence
import lp020002

PATCH_PATH = os.path.dirname(__file__)
REVISION   = '$Rev$'

print ''
print 'PATH : {0}'.format( PATCH_PATH )
print 'REV  : {0}'.format( REVISION )
print ''

(checkOnly, tenantFileName, resultsFileName) = appsequence.AppArgs.get(sys.argv)

sequence = []
sequence.append( appsequence.LPCheckVersion('2.0.1') )
sequence.append( lp020002.LP_020002_ImportCfgTables() )
sequence.append( appsequence.LPSetVersion('2.0.2') )

app = appsequence.AppSequence( tenantFileName, resultsFileName, sequence, checkOnly )
app.execute()
