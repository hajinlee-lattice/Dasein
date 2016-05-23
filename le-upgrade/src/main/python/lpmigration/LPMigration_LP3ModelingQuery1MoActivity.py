
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import os, sys
from copy import deepcopy
from liaison import *
from appsequence import Applicability, AppSequence, StepBase

class LPMigration_LP3ModelingQuery1MoActivity(StepBase):

    name        = 'LPMigration_LP3ModelingQuery1MoActivity'
    description = 'Updates activity in event table for LP3 modeling to use one-month aggregations.'
    version     = '$Rev$'

    STD_ACTIVITY_COLS =  ['Activity_Count_Click_Email','Activity_Count_Click_Link','Activity_Count_Email_Bounced_Soft'
                         ,'Activity_Count_Fill_Out_Form','Activity_Count_Interesting_Moment_Any'
                         ,'Activity_Count_Open_Email','Activity_Count_Unsubscribe_Email','Activity_Count_Visit_Webpage'
                         ,'Activity_Count_Interesting_Moment_Email'
                         ,'Activity_Count_Interesting_Moment_Event'
                         ,'Activity_Count_Interesting_Moment_Multiple'
                         ,'Activity_Count_Interesting_Moment_Pricing'
                         ,'Activity_Count_Interesting_Moment_Search'
                         ,'Activity_Count_Interesting_Moment_Webinar'
                         ,'Activity_Count_Interesting_Moment_key_web_page']

    LP3_ACTIVITY_COLS =  ['Activity_Count_Click_Email','Activity_Count_Email_Bounced_Soft'
                         ,'Activity_Count_Fill_Out_Form','Activity_Count_Interesting_Moment_Any'
                         ,'Activity_Count_Open_Email','Activity_Count_Unsubscribe_Email','Activity_Count_Visit_Webpage']


    def __init__(self, forceApply=False):
        super(LPMigration_LP3ModelingQuery1MoActivity, self).__init__(forceApply)


    def getApplicability(self, appseq):
        try:
            q = appseq.getConnectionMgr().getQuery('Q_LP3_ModelingLead_OneLeadPerDomain')
        except UnknownVisiDBSpec:
            return Applicability.cannotApplyFail

        if appseq.getText('template_type') == 'MKTO':
            return Applicability.canApply

        return Applicability.cannotApplyPass


    def apply( self, appseq ):
        print '\n    * Updating modeling event table with 1-month activity aggregations . .',
        sys.stdout.flush()

        conn_mgr = appseq.getConnectionMgr()

        q_lp3_modeling = conn_mgr.getQuery('Q_LP3_ModelingLead_OneLeadPerDomain')
        for cname in self.STD_ACTIVITY_COLS:
            c = q_lp3_modeling.getColumn(cname)
            if c is not None:
                if cname in self.LP3_ACTIVITY_COLS:
                    fcnNameOrig = c.getExpression().FcnName()
                    if fcnNameOrig[-4:] != '_1mo':
                        c.setExpression(ExpressionVDBImplFactory.parse(fcnNameOrig+'_1mo'))
                else:
                    q_lp3_modeling.removeColumn(cname)

        conn_mgr.setQuery(q_lp3_modeling)

        return True
