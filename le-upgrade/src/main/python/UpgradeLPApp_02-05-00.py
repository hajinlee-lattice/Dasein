#!/usr/bin/python

import os, sys
import appsequence
import lp020500

PATCH_PATH = os.path.dirname(__file__)
REVISION = '$Rev: 70953 $'

print ''
print 'PATH : {0}'.format(PATCH_PATH)
print 'REV  : {0}'.format(REVISION)
print ''

(checkOnly, tenantFileName, resultsFileName) = appsequence.AppArgs.get(sys.argv)
sequence = []
sequence.append(appsequence.LPCheckVersion('2.4.3'))
#sequence.append(lp020500.LP_020500_VDB_NewSpecs())
sequence.append(lp020500.LP_020500_RePDMatch_ReScore())
sequence.append(appsequence.LPSetVersion('2.4.3'))
app = appsequence.AppSequence(tenantFileName, resultsFileName, sequence, checkOnly)
app.execute()