#!/usr/bin/python

import os, sys
import appsequence
import lp020300

PATCH_PATH = os.path.dirname(__file__)
REVISION = '$Rev: 70953 $'

print ''
print 'PATH : {0}'.format(PATCH_PATH)
print 'REV  : {0}'.format(REVISION)
print ''

(checkOnly, tenantFileName, resultsFileName) = appsequence.AppArgs.get(sys.argv)
sequence = []
sequence.append(appsequence.LPCheckVersion('2.2.1'))
sequence.append(lp020300.LP_020300_Refine_PushToLeadDestination_Validation())
sequence.append(lp020300.LP_020300_NewSpecs())
sequence.append(appsequence.LPSetVersion('2.3.0'))
app = appsequence.AppSequence(tenantFileName, resultsFileName, sequence, checkOnly)
app.execute()