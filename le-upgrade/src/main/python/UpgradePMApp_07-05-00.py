#!/usr/bin/python

import os, sys
import appsequence
import pm070500

PATCH_PATH = os.path.dirname(__file__)
REVISION = '$Rev: 70953 $'

print ''
print 'PATH : {0}'.format(PATCH_PATH)
print 'REV  : {0}'.format(REVISION)
print ''

(checkOnly, tenantFileName, resultsFileName) = appsequence.AppArgs.get(sys.argv)
sequence = []
sequence.append(appsequence.LPCheckVersion('7.3.0'))
sequence.append(pm070500.PM_070500_DL_UpdateLeadMetadata())
sequence.append(appsequence.LPSetVersion('7.5.0'))
app = appsequence.AppSequence(tenantFileName, resultsFileName, sequence, checkOnly)
app.execute()
