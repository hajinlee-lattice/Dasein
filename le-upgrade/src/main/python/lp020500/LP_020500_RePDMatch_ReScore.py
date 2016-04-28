#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import appsequence
import liaison


class LP_020500_RePDMatch_ReScore(StepBase):
  name = 'LP_020500_RePDMatch_ReScore'
  description = 'LP_020500_RePDMatch_ReScore'
  version = '$Rev: 70934 $'

  def __init__(self, forceApply=False):
    super(LP_020500_RePDMatch_ReScore, self).__init__(forceApply)

  def getApplicability(self, appseq):

    lgm = appseq.getLoadGroupMgr()
    conn_mgr = appseq.getConnectionMgr()

    if not lgm.hasLoadGroup('PropDataMatch_Step2') and not lgm.hasLoadGroup('PushToScoringDB_Step2'):
      return Applicability.cannotApplyPass
    return Applicability.canApply

  def apply(self, appseq):

    success = False
    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText( 'template_type' )

    #Create PropDataMatch_Step2
    lgm.getLoadGroup( 'PropDataMatch_Step2')
    pdms2_xml = ''
    if type == 'MKTO':
      pdms2_xml = '''
      <extractQueries>
        <extractQuery qw="Workspace" queryName="Q_PropDataID_ContNumOfTimes" queryAlias="Q_PropDataID_ContNumOfTimes" sw="Workspace" schemaName="PropDataID_ContNumOfTimes" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms>
            <cm qcn="NumOfTimesSubmitToPD" itcn="NumOfTimesSubmitToPD"/>
            <cm qcn="PropDataID" itcn="PropDataID"/>
            <cm qcn="Time_OfSubmission_MatchToPD" itcn="Time_OfSubmission_MatchToPD"/>
          </cms>
        </extractQuery>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_MatchToPD" queryAlias="Q_Timestamp_MatchToPD" sw="Workspace" schemaName="Timestamp_MatchToPD" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms>
            <cm qcn="MKTO_LeadRecord_ID" itcn="MKTO_LeadRecord_ID"/>
            <cm qcn="Time_OfSubmission_MatchToPD" itcn="Time_OfSubmission_MatchToPD"/>
          </cms>
        </extractQuery>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_MatchToPD_ReMatchLead" queryAlias="Q_Timestamp_MatchToPD_ReMatchLead" sw="Workspace" schemaName="Timestamp_MatchToPD_ReMatchLead" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms>
            <cm qcn="LeadID" itcn="LeadID"/>
            <cm qcn="ReMatched_Scoring_Status" itcn="ReMatched_Scoring_Status"/>
            <cm qcn="Time_OfCompletion_MatchToPD" itcn="Time_OfCompletion_MatchToPD"/>
          </cms>
        </extractQuery>
      </extractQueries>'''
    elif type =='ELQ':
      pdms2_xml = '''
      <extractQueries>
        <extractQuery qw="Workspace" queryName="Q_PropDataID_ContNumOfTimes" queryAlias="Q_PropDataID_ContNumOfTimes" sw="Workspace" schemaName="PropDataID_ContNumOfTimes" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms>
            <cm qcn="NumOfTimesSubmitToPD" itcn="NumOfTimesSubmitToPD"/>
            <cm qcn="PropDataID" itcn="PropDataID"/>
            <cm qcn="Time_OfSubmission_MatchToPD" itcn="Time_OfSubmission_MatchToPD"/>
          </cms>
        </extractQuery>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_MatchToPD" queryAlias="Q_Timestamp_MatchToPD" sw="Workspace" schemaName="Timestamp_MatchToPD" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms>
            <cm qcn="ELQ_Contact_ContactID" itcn="ELQ_Contact_ContactID"/>
            <cm qcn="Time_OfSubmission_MatchToPD" itcn="Time_OfSubmission_MatchToPD"/>
          </cms>
        </extractQuery>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_MatchToPD_ReMatchLead" queryAlias="Q_Timestamp_MatchToPD_ReMatchLead" sw="Workspace" schemaName="Timestamp_MatchToPD_ReMatchLead" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms>
            <cm qcn="LeadID" itcn="LeadID"/>
            <cm qcn="ReMatched_Scoring_Status" itcn="ReMatched_Scoring_Status"/>
            <cm qcn="Time_OfCompletion_MatchToPD" itcn="Time_OfCompletion_MatchToPD"/>
          </cms>
        </extractQuery>
      </extractQueries>'''
    else:
      pdms2_xml = '''
      <extractQueries>
        <extractQuery qw="Workspace" queryName="Q_PropDataID_ContNumOfTimes" queryAlias="Q_PropDataID_ContNumOfTimes" sw="Workspace" schemaName="PropDataID_ContNumOfTimes" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms>
            <cm qcn="NumOfTimesSubmitToPD" itcn="NumOfTimesSubmitToPD"/>
            <cm qcn="PropDataID" itcn="PropDataID"/>
            <cm qcn="Time_OfSubmission_MatchToPD" itcn="Time_OfSubmission_MatchToPD"/>
          </cms>
        </extractQuery>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_MatchToPD" queryAlias="Q_Timestamp_MatchToPD" sw="Workspace" schemaName="Timestamp_MatchToPD" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms>
            <cm qcn="SFDC_Lead_Contact_ID" itcn="SFDC_Lead_Contact_ID"/>
            <cm qcn="Time_OfSubmission_MatchToPD" itcn="Time_OfSubmission_MatchToPD"/>
          </cms>
        </extractQuery>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_MatchToPD_ReMatchLead" queryAlias="Q_Timestamp_MatchToPD_ReMatchLead" sw="Workspace" schemaName="Timestamp_MatchToPD_ReMatchLead" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms/>
        </extractQuery>
      </extractQueries>'''
    lgm.setLoadGroupFunctionality('PropDataMatch_Step2', pdms2_xml)

    #Create PushToScoringDB_Step2
    lgm.createLoadGroup('PushToScoringDB_Step2', 'OperationalProcess\Standard',  'PushToScoringDB_Step2', True, False)
    ptss2_xml = ''
    if type == 'MKTO':
      ptss2_xml = '''
     <extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_PushToScoringIncr" queryAlias="Q_Timestamp_PushToScoringIncr" sw="Workspace" schemaName="Timestamp_PushToScoring" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms>
            <cm qcn="MKTO_LeadRecord_ID" itcn="MKTO_LeadRecord_ID"/>
            <cm qcn="Time_OfSubmission_PushToScoring" itcn="Time_OfSubmission_PushToScoring"/>
          </cms>
        </extractQuery>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_PushToScoringIncr_ReMatched" queryAlias="Q_Timestamp_PushToScoringIncr_ReMatched" sw="Workspace" schemaName="Timestamp_MatchToPD_ReMatchLead" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms>
            <cm qcn="LeadID" itcn="LeadID"/>
            <cm qcn="ReMatched_Scoring_Status" itcn="ReMatched_Scoring_Status"/>
            <cm qcn="Time_OfCompletion_MatchToPD" itcn="Time_OfCompletion_MatchToPD"/>
          </cms>
        </extractQuery>
      </extractQueries>'''
    elif type == 'ELQ':
       ptss2_xml = '''<extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_PushToScoringIncr" queryAlias="Q_Timestamp_PushToScoringIncr" sw="Workspace" schemaName="Timestamp_PushToScoring" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms>
            <cm qcn="ELQ_Contact_ContactID" itcn="ELQ_Contact_ContactID"/>
            <cm qcn="Time_OfSubmission_PushToScoring" itcn="Time_OfSubmission_PushToScoring"/>
          </cms>
        </extractQuery>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_PushToScoringIncr_ReMatched" queryAlias="Q_Timestamp_PushToScoringIncr_ReMatched" sw="Workspace" schemaName="Timestamp_MatchToPD_ReMatchLead" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms>
            <cm qcn="LeadID" itcn="LeadID"/>
            <cm qcn="ReMatched_Scoring_Status" itcn="ReMatched_Scoring_Status"/>
            <cm qcn="Time_OfCompletion_MatchToPD" itcn="Time_OfCompletion_MatchToPD"/>
          </cms>
        </extractQuery>
      </extractQueries>'''
    elif type == 'SFDC':
       ptss2_xml = '''<extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_PushToScoringIncr" queryAlias="Q_Timestamp_PushToScoringIncr" sw="Workspace" schemaName="Timestamp_PushToScoring" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms>
            <cm qcn="SFDC_Lead_Contact_ID" itcn="SFDC_Lead_Contact_ID"/>
            <cm qcn="Time_OfSubmission_PushToScoring" itcn="Time_OfSubmission_PushToScoring"/>
          </cms>
        </extractQuery>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_PushToScoringIncr_ReMatched" queryAlias="Q_Timestamp_PushToScoringIncr_ReMatched" sw="Workspace" schemaName="Timestamp_MatchToPD_ReMatchLead" at="False" ucm="False">
          <schemas/>
          <specs/>
          <cms/>
        </extractQuery>
      </extractQueries>'''
    lgm.setLoadGroupFunctionality('PushToScoringDB_Step2', pdms2_xml)
    success = True

    return success
