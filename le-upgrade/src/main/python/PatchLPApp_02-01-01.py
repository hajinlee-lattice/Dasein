
#
# $LastChangedBy: YTian $
# $LastChangedDate: 2015-10-30 13:59:04 +0800 (Fri, 30 Oct 2015) $
# $Rev: 70693 $
#

import os, sys
import appsequence
import lp020101

PATCH_PATH = os.path.dirname(__file__)
REVISION   = '$Rev: 70693 $'

print ''
print 'PATH : {0}'.format( PATCH_PATH )
print 'REV  : {0}'.format( REVISION )
print ''

(checkOnly, tenantFileName, resultsFileName) = appsequence.AppArgs.get(sys.argv)

sequence = []
sequence.append( appsequence.LPCheckVersion('2.1.0') )
sequence.append( lp020101.LP_020101_PushLeadsLastScoredToDestination() )
sequence.append( appsequence.LPSetVersion('2.1.1') )

app = appsequence.AppSequence( tenantFileName, resultsFileName, sequence, checkOnly )
app.execute()
