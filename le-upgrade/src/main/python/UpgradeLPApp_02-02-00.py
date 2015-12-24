#!/usr/bin/python

import os, sys
import appsequence
import lp020200

PATCH_PATH = os.path.dirname(__file__)
REVISION = '$Rev: 70953 $'

print ''
print 'PATH : {0}'.format(PATCH_PATH)
print 'REV  : {0}'.format(REVISION)
print ''

(checkOnly, tenantFileName, resultsFileName) = appsequence.AppArgs.get(sys.argv)
sequence = []
sequence.append(appsequence.LPCheckVersion('2.1.2'))
sequence.append(lp020200.LP_020200_DL_BulkScoring_PushToScoringDB())
sequence.append(lp020200.LP_020200_DL_PropDataMatch())
sequence.append(lp020200.LP_020200_DL_PushToScoringDB())
sequence.append(lp020200.LP_020200_DL_PushToLeadDestination())
sequence.append(lp020200.LP_020200_DL_POC_Model())
sequence.append(lp020200.LP_020200_NewSpecs())
sequence.append(lp020200.LP_020200_VDB_ModifiedSpec())
sequence.append(lp020200.LP_020200_VDB_ModifiedColumns())
sequence.append(appsequence.LPSetVersion('2.2.0'))
app = appsequence.AppSequence(tenantFileName, resultsFileName, sequence, checkOnly)
app.execute()
