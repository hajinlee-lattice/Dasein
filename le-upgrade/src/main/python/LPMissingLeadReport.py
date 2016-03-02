#!/usr/bin/python

import os, sys
import appsequence
import lpMissingLeadsReport

PATCH_PATH = os.path.dirname(__file__)
REVISION = '$Rev: 70953 $'

print ''
print 'PATH : {0}'.format(PATCH_PATH)
print 'REV  : {0}'.format(REVISION)
print ''

(tenantName,resultsFileName) = appsequence.AppArgs.get(sys.argv)
sequence = []
sequence.append(appsequence.LPCheckVersion('2.3.0'))
sequence.append(lpMissingLeadsReport.MissingLeadsReport())
#sequence.append(appsequence.LPSetVersion('2.3.0'))
app = appsequence.AppSequence(tenantName, resultsFileName, sequence, False)
app.execute()