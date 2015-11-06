import os, sys
import appsequence
import lp020100
import lp020002

PATCH_PATH = os.path.dirname(__file__)
REVISION = '$Rev$'

print ''
print 'PATH : {0}'.format(PATCH_PATH)
print 'REV  : {0}'.format(REVISION)
print ''

(checkOnly, tenantFileName, resultsFileName) = appsequence.AppArgs.get(sys.argv)
sequence = []
sequence.append(appsequence.LPCheckVersion('2.0.1'))
sequence.append(lp020002.LP_020002_ImportCfgTables())
sequence.append(lp020100.LP_020100_DL_LoadCRMData())
sequence.append(lp020100.LP_020100_DL_InsightsAllSteps())
sequence.append(lp020100.LP_020100_DL_PushToLeadDestination())
sequence.append(lp020100.LP_020100_DL_BulkScoring_Dante())
sequence.append(lp020100.LP_020100_VDB_NewSpecs())
sequence.append(lp020100.LP_020100_VDB_ModifiedSpec())
sequence.append(lp020100.LP_020100_VDB_ModifiedFilters())
sequence.append(lp020100.LP_020100_VDB_ModifiedEntity())
sequence.append(lp020100.LP_020100_VDB_ModifiedColumns())
sequence.append(lp020100.LP_020100_Diagnostic())
sequence.append(appsequence.LPSetVersion('2.1.0'))

app = appsequence.AppSequence(tenantFileName, resultsFileName, sequence, checkOnly)
app.execute()
