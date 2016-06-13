#!/usr/bin/python

import os, sys
import appsequence
import lp020600

PATCH_PATH = os.path.dirname(__file__)
REVISION = '$Rev: 70953 $'

print ''
print 'PATH : {0}'.format(PATCH_PATH)
print 'REV  : {0}'.format(REVISION)
print ''

(checkOnly, tenantFileName, resultsFileName) = appsequence.AppArgs.get(sys.argv)
sequence = []
sequence.append(appsequence.LPCheckVersion('2.5.1'))
# sequence.append(lp020600.LP_020600_AddDataProvider())
# sequence.append(lp020600.LP_020600_DL_SwitchtousingSFDCasynDataProviderType())
# sequence.append(lp020600.LP_020600_VDB_EliminateUnusedColumnsinMKTO_LeadRecord())
# sequence.append(lp020600.LP_020600_VDB_ModifiedSpec_SelectedForDantetoHandleNULLValues())
sequence.append(lp020600.LP_020600_DL_SetMissingLeadsForFinalDailyJobs())
sequence.append(appsequence.LPSetVersion('2.6.0'))
app = appsequence.AppSequence(tenantFileName, resultsFileName, sequence, checkOnly)
app.execute()
