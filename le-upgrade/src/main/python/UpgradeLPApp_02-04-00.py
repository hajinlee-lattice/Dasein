#!/usr/bin/python

import os, sys
import appsequence
import lp020400

PATCH_PATH = os.path.dirname(__file__)
REVISION = '$Rev: 70953 $'

print ''
print 'PATH : {0}'.format(PATCH_PATH)
print 'REV  : {0}'.format(REVISION)
print ''

(checkOnly, tenantFileName, resultsFileName) = appsequence.AppArgs.get(sys.argv)
sequence = []
sequence.append(appsequence.LPCheckVersion('2.3.0'))
sequence.append(lp020400.LP_020400_LG_PreMissingLeadReport())
sequence.append(lp020400.LP_020400_LG_Update_Services())
sequence.append(lp020400.LP_020400_LG_UploadLeadFile())
sequence.append(lp020400.LP_020400_VDB_ModifiedSpec())
sequence.append(lp020400.LP_020400_VDB_ModifiedFilters())
sequence.append(lp020400.LP_020400_VDB_ModifiedColumns())
sequence.append(lp020400.LP_020400_AddSchemas())
sequence.append(lp020400.LP_020400_VDB_NewSpecs())
sequence.append(appsequence.LPSetVersion('2.4.0'))
app = appsequence.AppSequence(tenantFileName, resultsFileName, sequence, checkOnly)
app.execute()