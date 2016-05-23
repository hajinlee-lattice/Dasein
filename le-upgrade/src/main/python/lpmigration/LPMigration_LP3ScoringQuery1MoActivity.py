
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import os, sys
from copy import deepcopy
from liaison import *
from appsequence import Applicability, AppSequence, StepBase

class LPMigration_LP3ScoringQuery1MoActivity(StepBase):

    name        = 'LPMigration_LP3ScoringQuery1MoActivity'
    description = 'Updates activity in event table for LP3 scoring to use one-month aggregations.'
    version     = '$Rev$'

    LP3_ACTIVITY_COLS =  ['Activity_Count_Click_Email','Activity_Count_Email_Bounced_Soft'
                         ,'Activity_Count_Fill_Out_Form','Activity_Count_Interesting_Moment_Any'
                         ,'Activity_Count_Open_Email','Activity_Count_Unsubscribe_Email','Activity_Count_Visit_Webpage']

    def __init__(self, forceApply=False):
        super(LPMigration_LP3ScoringQuery1MoActivity, self).__init__(forceApply)


    def getApplicability(self, appseq):
        try:
            q = appseq.getConnectionMgr().getQuery('Q_LP3_ScoringLead_RecentAllRows')
        except UnknownVisiDBSpec:
            return Applicability.cannotApplyFail

        if appseq.getText('template_type') == 'MKTO':
            return Applicability.canApply

        return Applicability.cannotApplyPass


    def apply( self, appseq ):
        print '\n    * Updating scoring event table with 1-month activity aggregations . .',
        sys.stdout.flush()

        conn_mgr = appseq.getConnectionMgr()

        q_lp3_scoring = conn_mgr.getQuery('Q_LP3_ScoringLead_RecentAllRows')
        for cname in self.LP3_ACTIVITY_COLS:
            c = q_lp3_scoring.getColumn(cname)
            if c is not None:
                fcnNameOrig = c.getExpression().FcnName()
                if fcnNameOrig[-8:] != '_scoring':
                    c.setExpression(ExpressionVDBImplFactory.parse(fcnNameOrig+'_scoring'))

        conn_mgr.setQuery(q_lp3_scoring)

        return True
