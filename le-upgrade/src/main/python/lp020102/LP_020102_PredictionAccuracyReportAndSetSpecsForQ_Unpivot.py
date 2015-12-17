#
# $LastChangedBy: YTian $
# $LastChangedDate: 2015-12-24 01:50:33 +0800 (Fri, 20 Nov 2015) $
# $Rev: 71049 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import re
import appsequence
import liaison
import os


class LP_020102_PredictionAccuracyReportAndSetSpecsForQ_Unpivot(StepBase):
  name = 'LP_020102_PredictionAccuracyReportAndSetSpecsForQ_Unpivot'
  description = '1. Set Specs for Q_Unpivot_* Queries in DL Template;2.Add PredictionAccuracy Report '
  version = '$Rev: 71049 $'

  def __init__(self, forceApply=False):
    super(LP_020102_PredictionAccuracyReportAndSetSpecsForQ_Unpivot, self).__init__(forceApply)

  def getApplicability(self, appseq):

    lgm      = appseq.getLoadGroupMgr()
    conn_mgr = appseq.getConnectionMgr()
    type = appseq.getText('template_type')

    if not lgm.hasLoadGroup('ExtractAnalyticAttributesIntoSourceTable'):
      return Applicability.cannotApplyFail


    return Applicability.canApply


  def apply(self, appseq):

    success = False

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText('template_type')

# Set Specs for Q_Unpivot_* Queries DL Load Group ExtractAnalyticAttributesIntoSourceTable
    pltd = lgm.getLoadGroup('ExtractAnalyticAttributesIntoSourceTable')

    if  type == 'MKTO':
      step_xml='''<extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Unpivot_By_SFDC_Lead_ID_PLS_Scoring_Incremental" queryAlias="Q_Unpivot_By_SFDC_Lead_ID_PLS_Scoring_Incremental" sw="Workspace" schemaName="DanteLeadAnalyticAttribute" at="False" ucm="True">
          <schemas>
            <schema name="Bard_LeadScoreHistory" />
            <schema name="Info_PublicDomain" />
            <schema name="Map_LeadID_PropDataID" />
            <schema name="MKTO_ActivityRecord" />
            <schema name="MKTO_LeadRecord" />
            <schema name="PD_DerivedColumns" />
            <schema name="SFDC_Contact" />
            <schema name="SFDC_Lead" />
            <schema name="SFDC_Opportunity" />
            <schema name="SFDC_OpportunityContactRole" />
            <schema name="Timestamp_MatchToPD" />
            <schema name="Timestamp_PushToDante_Stage" />
          </schemas>
          <specs>
            <spec name="Alias_AllLeadID" />
            <spec name="Alias_AllLeadTable" />
            <spec name="Alias_CompanyName_LowerCase" />
            <spec name="Alias_CreatedDate_Lead" />
            <spec name="Alias_Domain_LowerCase" />
            <spec name="Alias_FirstName_LowerCase" />
            <spec name="Alias_ID_Lead" />
            <spec name="Alias_Industry_LowerCase" />
            <spec name="Alias_LastName_LowerCase" />
            <spec name="Alias_ModifiedDate_Lead" />
            <spec name="Alias_Phone" />
            <spec name="Alias_Title_LowerCase" />
            <spec name="All_MKTO_LeadRecord" />
            <spec name="AppData_Lead_IsConsideredForModeling" />
            <spec name="AppData_Lead_OptyConnections" />
            <spec name="Const_ClientDomain" />
            <spec name="Const_DantePlayDisplayName" />
            <spec name="Const_DaysFromLeadCreationDate" />
            <spec name="Const_DaysOfDataForModeling" />
            <spec name="Const_DaysPriorToOptyConv" />
            <spec name="Dante_RankForLead" />
            <spec name="Dante_Stage_IsSelectedForDanteContact" />
            <spec name="Dante_Stage_IsSelectedForDanteLead" />
            <spec name="DS_CompanyName_Entropy" />
            <spec name="DS_CompanyName_IsUnusual" />
            <spec name="DS_CompanyName_Length" />
            <spec name="DS_Domain_IsThisClient" />
            <spec name="DS_Domain_Length" />
            <spec name="DS_FirstName_SameAs_LastName" />
            <spec name="DS_Industry_Group" />
            <spec name="DS_PD_Alexa_RelatedLinks_Count" />
            <spec name="DS_PD_FundingStage_Ordered" />
            <spec name="DS_PD_JobsTrendString_Ordered" />
            <spec name="DS_PD_ModelAction_Ordered" />
            <spec name="DS_Phone_Entropy" />
            <spec name="DS_SpamIndicator" />
            <spec name="DS_Title_IsAcademic" />
            <spec name="DS_Title_IsDirector" />
            <spec name="DS_Title_IsManager" />
            <spec name="DS_Title_IsSenior" />
            <spec name="DS_Title_IsTechRelated" />
            <spec name="DS_Title_IsUnusual" />
            <spec name="DS_Title_IsVPAbove" />
            <spec name="DS_Title_Length" />
            <spec name="DS_Title_Level" />
            <spec name="Email_Domain_IsPublic" />
            <spec name="Email_HasOptyByAccountAssoc" />
            <spec name="Email_HasOptyByContactRoleAssoc" />
            <spec name="Email_HasOptyByLeadAssoc" />
            <spec name="Email_PD" />
            <spec name="Entity_Lead_OpportunityConnection" />
            <spec name="Lead_ActivityCountThreeMonthsAgo" />
            <spec name="Lead_CreationDateRecencyForModeling" />
            <spec name="Lead_DateDiffCreationThreeMonthsAgo" />
            <spec name="Lead_InDateRangeForModeling" />
            <spec name="Lead_IsSelectedForModeling" />
            <spec name="Lead_OpportunityAssociationScore" />
            <spec name="Lead_OpportunityConnection" />
            <spec name="Lead_OpportunityEventScore" />
            <spec name="Lead_RankForModeling" />
            <spec name="LeadConversionDate" />
            <spec name="LeadID_50Char" />
            <spec name="MKTO_Activity_ClickEmail_cnt" />
            <spec name="MKTO_Activity_ClickLink_cnt" />
            <spec name="MKTO_Activity_Email" />
            <spec name="MKTO_Activity_EmailBncedHrd_cnt" />
            <spec name="MKTO_Activity_EmailBncedSft_cnt" />
            <spec name="MKTO_Activity_Event" />
            <spec name="MKTO_Activity_FillOutForm_cnt" />
            <spec name="MKTO_Activity_InterestingMoment_cnt" />
            <spec name="MKTO_Activity_keywebpage" />
            <spec name="MKTO_Activity_Multiple" />
            <spec name="MKTO_Activity_OpenEmail_cnt" />
            <spec name="MKTO_Activity_Pricing" />
            <spec name="MKTO_Activity_Search" />
            <spec name="MKTO_Activity_SendEmail_cnt" />
            <spec name="MKTO_Activity_UnsubscribeEmail_cnt" />
            <spec name="MKTO_Activity_VisitWeb_cnt" />
            <spec name="MKTO_Activity_Webinar" />
            <spec name="MKTO_LeadRecord_ActivityDate" />
            <spec name="MKTO_LeadRecord_ActivityDateMin" />
            <spec name="MKTO_LeadRecord_CountIDForDomain" />
            <spec name="MKTO_LeadRecord_CountIDForDomain_ZeroAllowed" />
            <spec name="MKTO_LeadRecord_ID_LeadCreationDateTime" />
            <spec name="MKTO_LeadRecord_MinOptyCreatedDate" />
            <spec name="Opty_IsCreatedAfterLead_MKTO" />
            <spec name="Opty_IsWon" />
            <spec name="P1_Event" />
            <spec name="SelectedForDante" />
            <spec name="SelectedForModeling_MKTO" />
            <spec name="Time_OfMostRecentMatchToPD" />
          </specs>
          <cms>
            <cm qcn="SFDC_Lead_ID" itcn="SFDCLeadID" />
            <cm qcn="AttributeName" itcn="AttributeName" />
            <cm qcn="AttributeValue" itcn="AttributeValue" />
          </cms>
        </extractQuery>
        <extractQuery qw="Workspace" queryName="Q_Unpivot_By_SFDC_Contact_ID_PLS_Scoring_Incremental" queryAlias="Q_Unpivot_By_SFDC_Contact_ID_PLS_Scoring_Incremental" sw="Workspace" schemaName="DanteContactAnalyticAttribute" at="False" ucm="True">
          <schemas>
            <schema name="Bard_LeadScoreHistory" />
            <schema name="Info_PublicDomain" />
            <schema name="Map_LeadID_PropDataID" />
            <schema name="MKTO_ActivityRecord" />
            <schema name="MKTO_LeadRecord" />
            <schema name="PD_DerivedColumns" />
            <schema name="SFDC_Contact" />
            <schema name="SFDC_Lead" />
            <schema name="SFDC_Opportunity" />
            <schema name="SFDC_OpportunityContactRole" />
            <schema name="Timestamp_MatchToPD" />
            <schema name="Timestamp_PushToDante_Stage" />
          </schemas>
          <specs>
            <spec name="Alias_AllLeadID" />
            <spec name="Alias_AllLeadTable" />
            <spec name="Alias_CompanyName_LowerCase" />
            <spec name="Alias_CreatedDate_Lead" />
            <spec name="Alias_Domain_LowerCase" />
            <spec name="Alias_FirstName_LowerCase" />
            <spec name="Alias_ID_Lead" />
            <spec name="Alias_LastName_LowerCase" />
            <spec name="Alias_ModifiedDate_Lead" />
            <spec name="Alias_Phone" />
            <spec name="Alias_Title_LowerCase" />
            <spec name="AppData_Lead_IsConsideredForModeling" />
            <spec name="AppData_Lead_OptyConnections" />
            <spec name="Const_ClientDomain" />
            <spec name="Const_DaysFromLeadCreationDate" />
            <spec name="Const_DaysOfDataForModeling" />
            <spec name="Const_DaysPriorToOptyConv" />
            <spec name="Dante_RankForLead" />
            <spec name="Dante_Stage_IsSelectedForDanteContact" />
            <spec name="Dante_Stage_IsSelectedForDanteLead" />
            <spec name="DS_CompanyName_Entropy" />
            <spec name="DS_CompanyName_IsUnusual" />
            <spec name="DS_CompanyName_Length" />
            <spec name="DS_Domain_IsThisClient" />
            <spec name="DS_Domain_Length" />
            <spec name="DS_FirstName_SameAs_LastName" />
            <spec name="DS_PD_Alexa_RelatedLinks_Count" />
            <spec name="DS_PD_FundingStage_Ordered" />
            <spec name="DS_PD_JobsTrendString_Ordered" />
            <spec name="DS_PD_ModelAction_Ordered" />
            <spec name="DS_Phone_Entropy" />
            <spec name="DS_SpamIndicator" />
            <spec name="DS_Title_IsAcademic" />
            <spec name="DS_Title_IsDirector" />
            <spec name="DS_Title_IsManager" />
            <spec name="DS_Title_IsSenior" />
            <spec name="DS_Title_IsTechRelated" />
            <spec name="DS_Title_IsVPAbove" />
            <spec name="DS_Title_Length" />
            <spec name="DS_Title_Level" />
            <spec name="Email_Domain_IsPublic" />
            <spec name="Email_HasOptyByAccountAssoc" />
            <spec name="Email_HasOptyByContactRoleAssoc" />
            <spec name="Email_HasOptyByLeadAssoc" />
            <spec name="Entity_Lead_OpportunityConnection" />
            <spec name="Lead_ActivityCountThreeMonthsAgo" />
            <spec name="Lead_CreationDateRecencyForModeling" />
            <spec name="Lead_DateDiffCreationThreeMonthsAgo" />
            <spec name="Lead_InDateRangeForModeling" />
            <spec name="Lead_IsSelectedForModeling" />
            <spec name="Lead_OpportunityAssociationScore" />
            <spec name="Lead_OpportunityConnection" />
            <spec name="Lead_OpportunityEventScore" />
            <spec name="Lead_RankForModeling" />
            <spec name="LeadConversionDate" />
            <spec name="LeadID_50Char" />
            <spec name="MKTO_Activity_ClickEmail_cnt" />
            <spec name="MKTO_Activity_ClickLink_cnt" />
            <spec name="MKTO_Activity_Email" />
            <spec name="MKTO_Activity_EmailBncedSft_cnt" />
            <spec name="MKTO_Activity_Event" />
            <spec name="MKTO_Activity_FillOutForm_cnt" />
            <spec name="MKTO_Activity_InterestingMoment_cnt" />
            <spec name="MKTO_Activity_keywebpage" />
            <spec name="MKTO_Activity_Multiple" />
            <spec name="MKTO_Activity_OpenEmail_cnt" />
            <spec name="MKTO_Activity_Pricing" />
            <spec name="MKTO_Activity_Search" />
            <spec name="MKTO_Activity_UnsubscribeEmail_cnt" />
            <spec name="MKTO_Activity_VisitWeb_cnt" />
            <spec name="MKTO_Activity_Webinar" />
            <spec name="MKTO_LeadRecord_ActivityDate" />
            <spec name="MKTO_LeadRecord_ActivityDateMin" />
            <spec name="MKTO_LeadRecord_CountIDForDomain" />
            <spec name="MKTO_LeadRecord_CountIDForDomain_ZeroAllowed" />
            <spec name="MKTO_LeadRecord_ID_LeadCreationDateTime" />
            <spec name="MKTO_LeadRecord_MinOptyCreatedDate" />
            <spec name="Opty_IsCreatedAfterLead_MKTO" />
            <spec name="Opty_IsWon" />
            <spec name="P1_Event" />
            <spec name="SelectedForDante" />
            <spec name="SelectedForModeling_MKTO" />
            <spec name="Time_OfMostRecentMatchToPD" />
          </specs>
          <cms>
            <cm qcn="SFDC_Contact_ID" itcn="SFDCContactID" />
            <cm qcn="AttributeName" itcn="AttributeName" />
            <cm qcn="AttributeValue" itcn="AttributeValue" />
          </cms>
        </extractQuery>
      </extractQueries>'''
    elif  type == 'ELQ':
      step_xml='''<extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Unpivot_By_SFDC_Lead_ID_PLS_Scoring_Incremental" queryAlias="Q_Unpivot_By_SFDC_Lead_ID_PLS_Scoring_Incremental" sw="Workspace" schemaName="DanteLeadAnalyticAttribute" at="False" ucm="True">
          <schemas>
            <schema name="Bard_LeadScoreHistory" />
            <schema name="ELQ_Contact" />
            <schema name="Info_PublicDomain" />
            <schema name="Map_ContactID_PropDataID" />
            <schema name="PD_DerivedColumns" />
            <schema name="SFDC_Contact" />
            <schema name="SFDC_Lead" />
            <schema name="SFDC_Opportunity" />
            <schema name="SFDC_OpportunityContactRole" />
            <schema name="Timestamp_MatchToPD" />
            <schema name="Timestamp_PushToDante_Stage" />
          </schemas>
          <specs>
            <spec name="Alias_AllLeadID" />
            <spec name="Alias_AllLeadTable" />
            <spec name="Alias_CompanyName_LowerCase" />
            <spec name="Alias_CreatedDate_Lead" />
            <spec name="Alias_Domain_LowerCase" />
            <spec name="Alias_FirstName_LowerCase" />
            <spec name="Alias_ID_Lead" />
            <spec name="Alias_Industry_LowerCase" />
            <spec name="Alias_LastName_LowerCase" />
            <spec name="Alias_ModifiedDate_Lead" />
            <spec name="Alias_Phone" />
            <spec name="Alias_Title_LowerCase" />
            <spec name="All_ELQ_Contact" />
            <spec name="AppData_Lead_IsConsideredForModeling" />
            <spec name="AppData_Lead_OptyConnections" />
            <spec name="Const_ClientDomain" />
            <spec name="Const_DantePlayDisplayName" />
            <spec name="Const_DaysFromLeadCreationDate" />
            <spec name="Const_DaysOfDataForModeling" />
            <spec name="Const_DaysPriorToOptyConv" />
            <spec name="Const_MaxNumberOfLeadsToDownload" />
            <spec name="Const_MaxNumberOfLeadsToScore" />
            <spec name="Const_MinOptyAmount" />
            <spec name="Dante_RankForLead" />
            <spec name="Dante_Stage_IsSelectedForDanteContact" />
            <spec name="Dante_Stage_IsSelectedForDanteLead" />
            <spec name="DS_CompanyName_Entropy" />
            <spec name="DS_CompanyName_IsUnusual" />
            <spec name="DS_CompanyName_Length" />
            <spec name="DS_Domain_IsThisClient" />
            <spec name="DS_Domain_Length" />
            <spec name="DS_FirstName_SameAs_LastName" />
            <spec name="DS_Industry_Group" />
            <spec name="DS_PD_Alexa_RelatedLinks_Count" />
            <spec name="DS_PD_FundingStage_Ordered" />
            <spec name="DS_PD_JobsTrendString_Ordered" />
            <spec name="DS_PD_ModelAction_Ordered" />
            <spec name="DS_Phone_Entropy" />
            <spec name="DS_SpamIndicator" />
            <spec name="DS_Title_IsAcademic" />
            <spec name="DS_Title_IsDirector" />
            <spec name="DS_Title_IsManager" />
            <spec name="DS_Title_IsSenior" />
            <spec name="DS_Title_IsTechRelated" />
            <spec name="DS_Title_IsUnusual" />
            <spec name="DS_Title_IsVPAbove" />
            <spec name="DS_Title_Length" />
            <spec name="DS_Title_Level" />
            <spec name="ELQ_Contact_ContactID_IsSelectedForPD" />
            <spec name="ELQ_Contact_CountContactIDForDomain" />
            <spec name="ELQ_Contact_CountContactIDForDomain_ZeroAllowed" />
            <spec name="ELQ_Contact_MinOptyCreatedDate" />
            <spec name="ELQ_Contact_PropDataID" />
            <spec name="Email_Domain_IsPublic" />
            <spec name="Email_HasOptyByAccountAssoc" />
            <spec name="Email_HasOptyByContactRoleAssoc" />
            <spec name="Email_HasOptyByLeadAssoc" />
            <spec name="Entity_Lead_OpportunityConnection" />
            <spec name="Lead_ActivityCountThreeMonthsAgo" />
            <spec name="Lead_CreationDateRecencyForModeling" />
            <spec name="Lead_DateDiffCreationThreeMonthsAgo" />
            <spec name="Lead_InDateRangeForModeling" />
            <spec name="Lead_IsSelectedForModeling" />
            <spec name="Lead_OpportunityAssociationScore" />
            <spec name="Lead_OpportunityConnection" />
            <spec name="Lead_OpportunityEventScore" />
            <spec name="Lead_RankForModeling" />
            <spec name="LeadID_50Char" />
            <spec name="Opty_IsCreatedAfterLead_ELQ" />
            <spec name="Opty_IsWon" />
            <spec name="P1_Event" />
            <spec name="RankForOlderLead" />
            <spec name="SelectedForDante" />
            <spec name="SelectedForModeling_ELQ" />
            <spec name="Time_OfMostRecentMatchToPD" />
          </specs>
          <cms>
            <cm qcn="SFDC_Lead_ID" itcn="SFDCLeadID" />
            <cm qcn="AttributeName" itcn="AttributeName" />
            <cm qcn="AttributeValue" itcn="AttributeValue" />
          </cms>
        </extractQuery>
        <extractQuery qw="Workspace" queryName="Q_Unpivot_By_SFDC_Contact_ID_PLS_Scoring_Incremental" queryAlias="Q_Unpivot_By_SFDC_Contact_ID_PLS_Scoring_Incremental" sw="Workspace" schemaName="DanteContactAnalyticAttribute" at="False" ucm="True">
          <schemas>
            <schema name="Bard_LeadScoreHistory" />
            <schema name="ELQ_Contact" />
            <schema name="Info_PublicDomain" />
            <schema name="Map_ContactID_PropDataID" />
            <schema name="PD_DerivedColumns" />
            <schema name="SFDC_Contact" />
            <schema name="SFDC_Lead" />
            <schema name="SFDC_Opportunity" />
            <schema name="SFDC_OpportunityContactRole" />
            <schema name="Timestamp_MatchToPD" />
            <schema name="Timestamp_PushToDante_Stage" />
          </schemas>
          <specs>
            <spec name="Alias_AllLeadID" />
            <spec name="Alias_AllLeadTable" />
            <spec name="Alias_CompanyName_LowerCase" />
            <spec name="Alias_CreatedDate_Lead" />
            <spec name="Alias_Domain_LowerCase" />
            <spec name="Alias_FirstName_LowerCase" />
            <spec name="Alias_ID_Lead" />
            <spec name="Alias_Industry_LowerCase" />
            <spec name="Alias_LastName_LowerCase" />
            <spec name="Alias_ModifiedDate_Lead" />
            <spec name="Alias_Phone" />
            <spec name="Alias_Title_LowerCase" />
            <spec name="All_ELQ_Contact" />
            <spec name="AppData_Lead_IsConsideredForModeling" />
            <spec name="AppData_Lead_OptyConnections" />
            <spec name="AppData_Model_GUID" />
            <spec name="AppData_Segments" />
            <spec name="Const_ClientDomain" />
            <spec name="Const_DantePlayDisplayName" />
            <spec name="Const_DaysFromLeadCreationDate" />
            <spec name="Const_DaysOfDataForDanteBulk" />
            <spec name="Const_DaysOfDataForModeling" />
            <spec name="Const_DaysOfDataForScoring_OlderLeads" />
            <spec name="Const_DaysOfDataForScoringBulk" />
            <spec name="Const_DaysOfValidityForPD" />
            <spec name="Const_DaysOfValidityForScore" />
            <spec name="Const_DaysPriorToOptyConv" />
            <spec name="Dante_RankForLead" />
            <spec name="Dante_Stage_IsSelectedForDanteContact" />
            <spec name="Dante_Stage_IsSelectedForDanteLead" />
            <spec name="DS_CompanyName_Entropy" />
            <spec name="DS_CompanyName_IsUnusual" />
            <spec name="DS_CompanyName_Length" />
            <spec name="DS_Domain_IsThisClient" />
            <spec name="DS_Domain_Length" />
            <spec name="DS_FirstName_SameAs_LastName" />
            <spec name="DS_Industry_Group" />
            <spec name="DS_PD_Alexa_RelatedLinks_Count" />
            <spec name="DS_PD_FundingStage_Ordered" />
            <spec name="DS_PD_JobsTrendString_Ordered" />
            <spec name="DS_PD_ModelAction_Ordered" />
            <spec name="DS_Phone_Entropy" />
            <spec name="DS_SpamIndicator" />
            <spec name="DS_Title_IsAcademic" />
            <spec name="DS_Title_IsDirector" />
            <spec name="DS_Title_IsManager" />
            <spec name="DS_Title_IsSenior" />
            <spec name="DS_Title_IsTechRelated" />
            <spec name="DS_Title_IsUnusual" />
            <spec name="DS_Title_IsVPAbove" />
            <spec name="DS_Title_Length" />
            <spec name="DS_Title_Level" />
            <spec name="ELQ_Contact_CountContactIDForDomain" />
            <spec name="ELQ_Contact_CountContactIDForDomain_ZeroAllowed" />
            <spec name="ELQ_Contact_MinOptyCreatedDate" />
            <spec name="ELQ_Contact_PropDataID" />
            <spec name="Email_Domain_IsPublic" />
            <spec name="Email_HasOptyByAccountAssoc" />
            <spec name="Email_HasOptyByContactRoleAssoc" />
            <spec name="Email_HasOptyByLeadAssoc" />
            <spec name="Entity_Lead_OpportunityConnection" />
            <spec name="Lead_ActivityCountThreeMonthsAgo" />
            <spec name="Lead_CreationDateRecencyForModeling" />
            <spec name="Lead_DateDiffCreationThreeMonthsAgo" />
            <spec name="Lead_InDateRangeForModeling" />
            <spec name="Lead_IsSelectedForModeling" />
            <spec name="Lead_OpportunityAssociationScore" />
            <spec name="Lead_OpportunityConnection" />
            <spec name="Lead_OpportunityEventScore" />
            <spec name="Lead_RankForModeling" />
            <spec name="LeadID_50Char" />
            <spec name="Opty_IsCreatedAfterLead_ELQ" />
            <spec name="Opty_IsWon" />
            <spec name="P1_Event" />
            <spec name="SelectedForDante" />
            <spec name="SelectedForModeling_ELQ" />
            <spec name="Time_OfMostRecentMatchToPD" />
          </specs>
          <cms>
            <cm qcn="SFDC_Contact_ID" itcn="SFDCContactID" />
            <cm qcn="AttributeName" itcn="AttributeName" />
            <cm qcn="AttributeValue" itcn="AttributeValue" />
          </cms>
        </extractQuery>
      </extractQueries>'''

    else:
      step_xml='''<extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Unpivot_By_SFDC_Lead_Contact_ID_PLS_Scoring_Incremental" queryAlias="Q_Unpivot_By_SFDC_Lead_Contact_ID_PLS_Scoring_Incremental" sw="Workspace" schemaName="DanteLeadAnalyticAttribute" at="False" ucm="True">
          <schemas>
            <schema name="Bard_LeadScoreHistory" />
            <schema name="Info_PublicDomain" />
            <schema name="Map_Lead_Contact_ID_PropDataID" />
            <schema name="PD_DerivedColumns" />
            <schema name="SFDC_Account" />
            <schema name="SFDC_Contact" />
            <schema name="SFDC_Lead" />
            <schema name="SFDC_Opportunity" />
            <schema name="SFDC_OpportunityContactRole" />
            <schema name="Timestamp_MatchToPD" />
            <schema name="Timestamp_PushToDante_Stage" />
          </schemas>
          <specs>
            <spec name="Alias_AllLeadID" />
            <spec name="Alias_AllLeadTable" />
            <spec name="Alias_CompanyName_LowerCase" />
            <spec name="Alias_CreatedDate_Lead" />
            <spec name="Alias_Domain_LowerCase" />
            <spec name="Alias_FirstName_LowerCase" />
            <spec name="Alias_ID_Lead" />
            <spec name="Alias_Industry_LowerCase" />
            <spec name="Alias_LastName_LowerCase" />
            <spec name="Alias_ModifiedDate_Lead" />
            <spec name="Alias_Phone" />
            <spec name="Alias_Title_LowerCase" />
            <spec name="AppData_Lead_IsConsideredForModeling" />
            <spec name="AppData_Lead_OptyConnections" />
            <spec name="AppData_Model_GUID" />
            <spec name="AppData_Segments" />
            <spec name="Const_ClientDomain" />
            <spec name="Const_DantePlayDisplayName" />
            <spec name="Const_DaysFromLeadCreationDate" />
            <spec name="Const_DaysOfDataForModeling" />
            <spec name="Const_DaysOfValidityForPD" />
            <spec name="Const_DaysOfValidityForScore" />
            <spec name="Const_DaysPriorToOptyConv" />
            <spec name="DS_CompanyName_Entropy" />
            <spec name="DS_CompanyName_IsUnusual" />
            <spec name="DS_CompanyName_Length" />
            <spec name="DS_Domain_IsThisClient" />
            <spec name="DS_Domain_Length" />
            <spec name="DS_FirstName_SameAs_LastName" />
            <spec name="DS_Industry_Group" />
            <spec name="DS_PD_Alexa_RelatedLinks_Count" />
            <spec name="DS_PD_FundingStage_Ordered" />
            <spec name="DS_PD_JobsTrendString_Ordered" />
            <spec name="DS_PD_ModelAction_Ordered" />
            <spec name="DS_Phone_Entropy" />
            <spec name="DS_SpamIndicator" />
            <spec name="DS_Title_IsAcademic" />
            <spec name="DS_Title_IsDirector" />
            <spec name="DS_Title_IsManager" />
            <spec name="DS_Title_IsSenior" />
            <spec name="DS_Title_IsTechRelated" />
            <spec name="DS_Title_IsUnusual" />
            <spec name="DS_Title_IsVPAbove" />
            <spec name="DS_Title_Length" />
            <spec name="DS_Title_Level" />
            <spec name="Email_Domain_IsPublic" />
            <spec name="Entity_Lead_OpportunityConnection" />
            <spec name="Is_Contact" />
            <spec name="Is_Lead" />
            <spec name="Lead_ActivityCountThreeMonthsAgo" />
            <spec name="Lead_CreationDateRecencyForModeling" />
            <spec name="Lead_DateDiffCreationThreeMonthsAgo" />
            <spec name="Lead_HasOptyByAccountAssoc" />
            <spec name="Lead_HasOptyByContactRoleAssoc" />
            <spec name="Lead_HasOptyByLeadAssoc" />
            <spec name="Lead_InDateRangeForModeling" />
            <spec name="Lead_IsSelectedForModeling" />
            <spec name="Lead_OpportunityAssociationScore" />
            <spec name="Lead_OpportunityConnection" />
            <spec name="Lead_OpportunityEventScore" />
            <spec name="Lead_RankForModeling" />
            <spec name="LeadID_50Char" />
            <spec name="Option_ContactForModeling" />
            <spec name="Option_LeadForModeling" />
            <spec name="Opty_ClosedAfterContactCreated" />
            <spec name="Opty_IsCreatedAfterLead_SFDC" />
            <spec name="Opty_IsWon" />
            <spec name="P1_Event" />
            <spec name="SelectedForDante" />
            <spec name="SelectedForModeling_SFDC" />
            <spec name="SFDC_City" />
            <spec name="SFDC_Company" />
            <spec name="SFDC_Contact_MinOptyCreatedDate_Contact" />
            <spec name="SFDC_Country" />
            <spec name="SFDC_CreatedDate" />
            <spec name="SFDC_Email" />
            <spec name="SFDC_EmailDomain" />
            <spec name="SFDC_EmailDomain_PD" />
            <spec name="SFDC_FirstName" />
            <spec name="SFDC_Id" />
            <spec name="SFDC_Industry" />
            <spec name="SFDC_LastModifiedDate" />
            <spec name="SFDC_LastName" />
            <spec name="SFDC_Lead_Contact_ID_IsSelectedForPD" />
            <spec name="SFDC_Lead_Contact_PropDataID" />
            <spec name="SFDC_Lead_CountIDForDomain" />
            <spec name="SFDC_Lead_CountIDForDomain_ZeroAllowed" />
            <spec name="SFDC_Lead_MinOptyCreatedDate_Lead" />
            <spec name="SFDC_Phone" />
            <spec name="SFDC_State" />
            <spec name="SFDC_Title" />
            <spec name="Time_OfMostRecentMatchToPD" />
          </specs>
          <cms>
            <cm qcn="SFDC_Lead_Contact_ID" itcn="SFDCLeadID" />
            <cm qcn="AttributeName" itcn="AttributeName" />
            <cm qcn="AttributeValue" itcn="AttributeValue" />
          </cms>
        </extractQuery>
      </extractQueries>'''

    lgm.setLoadGroupFunctionality( 'ExtractAnalyticAttributesIntoSourceTable', step_xml )

# Add PredictionAccuracy Report into visiDB
# add new specs as following:
    #1. PredictionAccuracyReport
    #2. SFDC_Campaign
    #3. SFDC_CampaignMember
    #4. MaxScoreDate_Bucket_BLSH
    #5. MaxScoreDate_Percentile_BLSH
    #6. MaxScoreDate_Score_BLSH

    newSpecsFileName = 'LP_' + type + '_NewSpecs_2.1.2_from_2.1.1.maude'
    newSpecsFileName = os.path.join('..','resources',newSpecsFileName)

    slnes = ''

    with open( newSpecsFileName, mode='r' ) as newSpecsFile:
      slnes = newSpecsFile.read()

    conn_mgr = appseq.getConnectionMgr()
    conn_mgr.setSpec( 'New Specs', slnes )
    success = True

    return success

