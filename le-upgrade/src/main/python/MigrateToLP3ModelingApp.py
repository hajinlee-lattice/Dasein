#!/usr/bin/python

import os, sys, datetime
import appsequence
import lpmigration

PATCH_PATH = os.path.dirname(__file__)
REVISION = '$Rev$'

print ''
print 'PATH : {0}'.format(PATCH_PATH)
print 'REV  : {0}'.format(REVISION)
print ''

timestamp = datetime.datetime.now()

(checkOnly, tenantFileName, resultsFileName) = appsequence.AppArgs.get(sys.argv)
sequence = []
sequence.append(appsequence.LPCheckVersion('0.0.0',forceApply=True))

## General modeling updates
sequence.append(lpmigration.LPMigration_ModelingUpdates())

## Modeling query for LP3
sequence.append(lpmigration.LPMigration_1MoActivityForModeling())
sequence.append(lpmigration.LPMigration_LP3ModelingQuery())
sequence.append(lpmigration.LPMigration_LP3ModelingQuery1MoActivity())
sequence.append(appsequence.WriteQueryToCSV('Q_LP3_ModelingLead_OneLeadPerDomain', timestamp))

## Scoring queries for validating LP3 score distributions
sequence.append(lpmigration.LPMigration_1MoActivityForScoring())
sequence.append(lpmigration.LPMigration_LP3ScoringRecentAllRowsQuery())
sequence.append(lpmigration.LPMigration_LP3ScoringQuery1MoActivity())
sequence.append(appsequence.WriteQueryToCSV('Q_LP3_ScoringLead_RecentAllRows', timestamp))

app = appsequence.AppSequence(tenantFileName, resultsFileName, sequence, checkOnly)
app.execute()
