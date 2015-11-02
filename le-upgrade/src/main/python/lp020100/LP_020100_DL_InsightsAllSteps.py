
#
# $LastChangedBy: VivianZhao $
# $LastChangedDate: 2015-10-29 14:33:11 +0800 (Wed, 21 Oct 2015) $
# $Rev: 70508 $
#

from lxml import etree
from appsequence import Applicability, StepBase

class LP_020100_DL_InsightsAllSteps( StepBase ):
  
  name        = 'LP_020100_DL_InsightsAllSteps'
  description = 'Reset logic of the InsightsAllSteps'
  version     = '$Rev: 70508 $'

  def __init__( self, forceApply = False ):
    super( LP_020100_DL_InsightsAllSteps, self ).__init__( forceApply )


  def getApplicability( self, appseq ):

    lgm = appseq.getLoadGroupMgr()
    
    if not lgm.hasLoadGroup( 'InsightsAllSteps' ):
      return Applicability.cannotApplyFail

    ptldxml = lgm.getLoadGroupFunctionality( 'InsightsAllSteps', 'ngs' )
    hasModPTLD = ptldxml != '<ngs />'
    hasStep1 = lgm.hasLoadGroup( 'UpdateDanteTimestamp_Step1' )
    hasStep2 = lgm.hasLoadGroup( 'UpdateDanteTimestamp_Step2' )
    hasStep3 = lgm.hasLoadGroup( 'ExtractDanteLeadsIntoSourceTable' )

    if not hasStep1 and not hasStep2 and hasStep3:
      return Applicability.canApply
    elif hasModPTLD and hasStep1:
      return Applicability.alreadyAppliedPass

    return Applicability.cannotApplyPass


  def apply( self, appseq ):
    
    success = False

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText( 'template_type' )

    lgm.createLoadGroup( 'CreateBIQueries', 'ModelBuild\Standard', 'CreateBIQueries', True, False )
    step1xml = ''
    if type == 'MKTO':
      step1xml = '<rdss><rds n="BuyerInsightsAttributes" w="Workspace" sn="Sys_BuyerInsightsAttributes" cn="SQL_MultiTenant" u="False" ss="" tn="V_PREDICTOR_FOR_BI" nmo="1" f="Model_GUID IN #GUIDs" ad="False" em="False" td="False" ic="" dd="" l="1000" tw="False" sr="50000" htw="24" mtw="60" emt="False" acd="False" mgf="False" eo="2" emd="False" eo_sftp="2"><ts><t n="GUIDs" t="2" qn="Q_Get_Model_GUID" cn="Model_GUID" m="100"><schemas /><specs /></t></ts><mcs><mc cn="Column_Name" /><mc cn="Model_GUID" /></mcs></rds></rdss>'
    elif type =='ELQ':
      step1xml = '<rdss><rds n="BuyerInsightsAttributes" w="Workspace" sn="Sys_BuyerInsightsAttributes" cn="SQL_MultiTenant" u="False" ss="" tn="V_PREDICTOR_FOR_BI" nmo="1" f="Model_GUID IN #GUIDs" ad="False" em="False" td="False" ic="" dd="" l="1000" tw="False" sr="50000" htw="24" mtw="60" emt="False" acd="False" mgf="False" eo="2" emd="False" eo_sftp="2"><ts><t n="GUIDs" t="2" qn="Q_Get_Model_GUID" cn="Model_GUID" m="100"><schemas /><specs /></t></ts><mcs><mc cn="Column_Name" /><mc cn="Model_GUID" /></mcs></rds></rdss>'
    else:
      step1xml = '<rdss><rds n="BuyerInsightsAttributes" w="Workspace" sn="Sys_BuyerInsightsAttributes" cn="SQL_MultiTenant" u="False" ss="" tn="V_PREDICTOR_FOR_BI" nmo="1" f="Model_GUID IN #GUIDs" ad="False" em="False" td="False" ic="" dd="" l="1000" tw="False" sr="50000" htw="24" mtw="60" emt="False" acd="False" mgf="False" eo="2" emd="False" eo_sftp="2"><ts><t n="GUIDs" t="2" qn="Q_Get_Model_GUID" cn="Model_GUID" m="100"><schemas /><specs /></t></ts><mcs><mc cn="Column_Name" /><mc cn="Model_GUID" /></mcs></rds></rdss>'
    lgm.setLoadGroupFunctionality( 'CreateBIQueries', step1xml )

    step2xml = ''
    if type == 'MKTO':
      step2xml = '<visiDBConfigurationWithMacros><visiDBConfigurationWithMacro n="LP-GenerateDanteAttributeQueries" w="Workspace" qn="View_Table_Sys_BuyerInsightsAttributes" mf="LP-GenerateDanteAttributeQueries" hf="True" /></visiDBConfigurationWithMacros>'
    elif type =='ELQ':
      step2xml = '<visiDBConfigurationWithMacros><visiDBConfigurationWithMacro n="LP-GenerateDanteAttributeQueries" w="Workspace" qn="View_Table_Sys_BuyerInsightsAttributes" mf="LP-GenerateDanteAttributeQueries" hf="True" /></visiDBConfigurationWithMacros>'
    else:
      step2xml = '<visiDBConfigurationWithMacros><visiDBConfigurationWithMacro n="LP-GenerateDanteAttributeQueries" w="Workspace" qn="View_Table_Sys_BuyerInsightsAttributes" mf="LP-GenerateDanteAttributeQueries" hf="True" /></visiDBConfigurationWithMacros>'
    lgm.setLoadGroupFunctionality( 'CreateBIQueries', step2xml )


    lgm.createLoadGroup( 'UpdateDanteTimestamp_Step1', 'Insights', 'UpdateDanteTimestamp_Step1', True, False )
    step3xml = ''
    if type == 'MKTO':
      step3xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PreDanteIncr" queryAlias="Q_Timestamp_PreDanteIncr" sw="Workspace" schemaName="Timestamp_PushToDante" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="MKTO_LeadRecord_ID" itcn="MKTO_LeadRecord_ID" /><cm qcn="ScoreDate" itcn="ScoreDate" /><cm qcn="SFDC_Contact_ID" itcn="SFDC_Contact_ID" /><cm qcn="SFDC_Lead_ID" itcn="SFDC_Lead_ID" /><cm qcn="Time_OfCompletion_PushToDante" itcn="Time_OfCompletion_PushToDante" /></cms></extractQuery></extractQueries>'
    elif type =='ELQ':
      step3xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PreDanteIncr" queryAlias="Q_Timestamp_PreDanteIncr" sw="Workspace" schemaName="Timestamp_PushToDante" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="ELQ_Contact_ContactID" itcn="ELQ_Contact_ContactID" /><cm qcn="SFDC_Lead_ID" itcn="SFDC_Lead_ID" /><cm qcn="SFDC_Contact_ID" itcn="SFDC_Contact_ID" /><cm qcn="ScoreDate" itcn="ScoreDate" /><cm qcn="Time_OfCompletion_PushToDante" itcn="Time_OfCompletion_PushToDante" /></cms></extractQuery></extractQueries>'
    else:
      step3xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PreDanteIncr" queryAlias="Q_Timestamp_PreDanteIncr" sw="Workspace" schemaName="Timestamp_PushToDante" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="ScoreDate" itcn="ScoreDate" /><cm qcn="SFDC_Lead_Contact_ID" itcn="SFDC_Lead_Contact_ID" /><cm qcn="Time_OfCompletion_PushToDante" itcn="Time_OfCompletion_PushToDante" /></cms></extractQuery></extractQueries>'
    lgm.setLoadGroupFunctionality( 'UpdateDanteTimestamp_Step1', step3xml )

    lgm.createLoadGroup( 'UpdateDanteTimestamp_Step2', 'Insights', 'UpdateDanteTimestamp_Step2', True, False )
    step4xml = ''
    if type == 'MKTO':
      step4xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PushToDante" queryAlias="Q_Timestamp_PushToDante" sw="Workspace" schemaName="Timestamp_PushToDante" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="MKTO_LeadRecord_ID" itcn="MKTO_LeadRecord_ID" /><cm qcn="ScoreDate" itcn="ScoreDate" /><cm qcn="SFDC_Contact_ID" itcn="SFDC_Contact_ID" /><cm qcn="SFDC_Lead_ID" itcn="SFDC_Lead_ID" /><cm qcn="Time_OfCompletion_PushToDante" itcn="Time_OfCompletion_PushToDante" /></cms></extractQuery></extractQueries>'
    elif type =='ELQ':
      step4xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PushToDante" queryAlias="Q_Timestamp_PushToDante" sw="Workspace" schemaName="Timestamp_PushToDante" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="ELQ_Contact_ContactID" itcn="ELQ_Contact_ContactID" /><cm qcn="SFDC_Lead_ID" itcn="SFDC_Lead_ID" /><cm qcn="SFDC_Contact_ID" itcn="SFDC_Contact_ID" /><cm qcn="ScoreDate" itcn="ScoreDate" /><cm qcn="Time_OfCompletion_PushToDante" itcn="Time_OfCompletion_PushToDante" /></cms></extractQuery></extractQueries>'
    else:
      step4xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PushToDante" queryAlias="Q_Timestamp_PushToDante" sw="Workspace" schemaName="Timestamp_PushToDante" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="SFDC_Lead_Contact_ID" itcn="SFDC_Lead_Contact_ID" /><cm qcn="Time_OfCompletion_PushToDante" itcn="Time_OfCompletion_PushToDante" /><cm qcn="SFDC_Contact_ID" itcn="SFDC_Contact_ID" /><cm qcn="SFDC_Lead_ID" itcn="SFDC_Lead_ID" /><cm qcn="ScoreDate" itcn="ScoreDate" /></cms></extractQuery></extractQueries>'
    lgm.setLoadGroupFunctionality( 'UpdateDanteTimestamp_Step2', step4xml )

    lgm.getLoadGroup( 'ExtractDanteLeadsIntoSourceTable')
    step5xml = ''
    if type == 'MKTO':
      step5xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Dante_LeadSourceTable" queryAlias="Q_Dante_LeadSourceTable" sw="Workspace" schemaName="DanteLead" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="SFDCLeadID" itcn="SFDCLeadID" /><cm qcn="Score" itcn="Score" /><cm qcn="Bucket_Display_Name" itcn="Bucket_Display_Name" /><cm qcn="ModelID" itcn="ModelID" /><cm qcn="Probability" itcn="Probability" /><cm qcn="Lift" itcn="Lift" /><cm qcn="Percentile" itcn="Percentile" /><cm qcn="LeadID" itcn="LeadID" /><cm qcn="PlayDisplayName" itcn="PlayDisplayName" /></cms></extractQuery></extractQueries>'
    elif type =='ELQ':
      step5xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Dante_LeadSourceTable" queryAlias="Q_Dante_LeadSourceTable" sw="Workspace" schemaName="DanteLead" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="SFDCLeadID" itcn="SFDCLeadID" /><cm qcn="Score" itcn="Score" /><cm qcn="Bucket_Display_Name" itcn="Bucket_Display_Name" /><cm qcn="ModelID" itcn="ModelID" /><cm qcn="Probability" itcn="Probability" /><cm qcn="Lift" itcn="Lift" /><cm qcn="Percentile" itcn="Percentile" /><cm qcn="LeadID" itcn="LeadID" /><cm qcn="PlayDisplayName" itcn="PlayDisplayName" /></cms></extractQuery></extractQueries>'
    else:
      step5xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Dante_LeadSourceTable" queryAlias="Q_Dante_LeadSourceTable" sw="Workspace" schemaName="DanteLead" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="SFDC_Lead_Contact_ID" itcn="SFDCLeadID" /><cm qcn="Score" itcn="Score" /><cm qcn="Bucket_Display_Name" itcn="Bucket_Display_Name" /><cm qcn="ModelID" itcn="ModelID" /><cm qcn="Probability" itcn="Probability" /><cm qcn="Lift" itcn="Lift" /><cm qcn="Percentile" itcn="Percentile" /><cm qcn="LeadID" itcn="LeadID" /><cm qcn="PlayDisplayName" itcn="PlayDisplayName" /></cms></extractQuery></extractQueries>'
    lgm.setLoadGroupFunctionality( 'ExtractDanteLeadsIntoSourceTable', step5xml )

    step6 = etree.fromstring( lgm.getLoadGroup('LoadCRMDataForModeling').encode('ascii', 'xmlcharrefreplace') )

    step6.set( 'name', 'LoadCRMDataForModeling' )
    step6.set( 'alias', 'LoadCRMDataForModeling' )
    lgm.setLoadGroup( etree.tostring(step6) )

    ptld  = etree.fromstring( lgm.getLoadGroup('InsightsAllSteps').encode('ascii', 'xmlcharrefreplace') )

    ptld.set( 'ng', 'True' )
    lgm.setLoadGroup( etree.tostring(ptld) )
    if type == 'SFDC':
      ngsxml = '<ngs><ng n="CreateBIQueries"/><ng n="ExtractDanteLeadsIntoSourceTable"/><ng n="ExtractAnalyticAttributesIntoSourceTable"/><ng n="PushDanteLeadsAndAnalyticAttributesToDante"/><ng n="UpdateDanteTimestamp_Step1"/><ng n="UpdateDanteTimestamp_Step2"/></ngs>'
    else:
      ngsxml = '<ngs><ng n="CreateBIQueries"/><ng n="ExtractDanteLeadsIntoSourceTable"/><ng n="ExtractDanteContactsIntoSourceTable"/><ng n="ExtractAnalyticAttributesIntoSourceTable"/><ng n="PushDanteContactsAndAnalyticAttributesToDante"/><ng n="PushDanteLeadsAndAnalyticAttributesToDante"/><ng n="UpdateDanteTimestamp_Step1"/><ng n="UpdateDanteTimestamp_Step2"/></ngs>'
    lgm.setLoadGroupFunctionality( 'InsightsAllSteps', ngsxml )

    lgm.commit()

    success = True

    return success

