
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from lxml import etree
from appsequence import Applicability, StepBase

class LP_020100_DL_BulkScoring_Dante( StepBase ):
  
  name        = 'LP_020100_DL_BulkScoring_Dante'
  description = 'Reset logic of the BulkScoring_Dante'
  version     = '$Rev$'

  def __init__( self, forceApply = False ):
    super( LP_020100_DL_BulkScoring_Dante, self ).__init__( forceApply )


  def getApplicability( self, appseq ):

    lgm = appseq.getLoadGroupMgr()
    if lgm.hasLoadGroup( 'PushDataToDante_Bulk' ):
      return Applicability.alreadyAppliedPass
    return Applicability.canApply


  def apply( self, appseq ):
    
    success = False

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText( 'template_type' )

    lgm.createLoadGroup( 'LoadLeadCache', 'BulkScoring\Dante', 'LoadLeadCache', True, False )
    step1xml = ''
    if type == 'MKTO':
      step1xml = '<rdss><rds n="LoadLeadCache_Lead" w="Workspace" sn="LeadCache_Lead" cn="SQL_DanteDB_DataProvider" u="False" ss="" tn="LeadCache" nmo="1" f=" [Customer_ID] = #Customer_ID AND Last_Modification_Date&gt;= #LastModifiedDate AND Account_External_ID in #IDs" ad="False" em="False" td="False" ic="" dd="" l="1000" tw="False" sr="50000" htw="24" mtw="60" emt="False" acd="False" mgf="False" eo="2" emd="False" eo_sftp="2"><ts><t n="LastModifiedDate" t="1" qn="Q_Get_TimeRange_LastModifiedLeadCache" cn="Diff_Of_Last_ModifiedDays" m="0"><schemas /><specs /></t><t n="Customer_ID" t="1" qn="Q_Get_TimeRange_LastModifiedLeadCache" cn="Const_TenantName" m="0"><schemas /><specs /></t><t n="IDs" t="2" qn="Q_Get_TimeRange_SFDC_Lead" cn="SFDC_Lead_ID" m="100"><schemas /><specs /></t></ts><mcs><mc cn="Account_External_ID" /><mc cn="Customer_ID" /><mc cn="Last_Modification_Date" /></mcs></rds><rds n="LoadLeadCache_Contact" w="Workspace" sn="LeadCache_Contact" cn="SQL_DanteDB_DataProvider" u="False" ss="" tn="LeadCache" nmo="1" f=" [Customer_ID] = #Customer_ID AND (Last_Modification_Date&gt;= #LastModifiedDate) AND ([Account_External_ID] in #IDs)" ad="False" em="False" td="False" ic="" dd="" l="1000" tw="False" sr="50000" htw="24" mtw="60" emt="False" acd="False" mgf="False" eo="2" emd="False" eo_sftp="2"><ts><t n="LastModifiedDate" t="1" qn="Q_Get_TimeRange_LastModifiedLeadCache" cn="Diff_Of_Last_ModifiedDays" m="0"><schemas /><specs /></t><t n="Customer_ID" t="1" qn="Q_Get_TimeRange_LastModifiedLeadCache" cn="Const_TenantName" m="0"><schemas /><specs /></t><t n="IDs" t="2" qn="Q_Get_TimeRange_SFDC_Contact" cn="SFDC_Contact_ID" m="100"><schemas /><specs /></t></ts><mcs><mc cn="Account_External_ID" /><mc cn="Customer_ID" /><mc cn="Last_Modification_Date" /></mcs></rds></rdss>'
    elif type =='ELQ':
      step1xml = '<rdss><rds n="LoadLeadCache_Lead" w="Workspace" sn="LeadCache_Lead" cn="SQL_DanteDB_DataProvider" u="False" ss="" tn="LeadCache" nmo="1" f=" [Customer_ID] = #Customer_ID AND Last_Modification_Date&gt;= #LastModifiedDate AND Account_External_ID in #IDs" ad="False" em="False" td="False" ic="" dd="" l="1000" tw="False" sr="50000" htw="24" mtw="60" emt="False" acd="False" mgf="False" eo="2" emd="False" eo_sftp="2"><ts><t n="LastModifiedDate" t="1" qn="Q_Get_TimeRange_LastModifiedLeadCache" cn="Diff_Of_Last_ModifiedDays" m="0"><schemas /><specs /></t><t n="Customer_ID" t="1" qn="Q_Get_TimeRange_LastModifiedLeadCache" cn="Const_TenantName" m="0"><schemas /><specs /></t><t n="IDs" t="2" qn="Q_Get_TimeRange_SFDC_Lead" cn="SFDC_Lead_ID" m="100"><schemas /><specs /></t></ts><mcs><mc cn="Account_External_ID" /><mc cn="Customer_ID" /><mc cn="Last_Modification_Date" /></mcs></rds><rds n="LoadLeadCache_Contact" w="Workspace" sn="LeadCache_Contact" cn="SQL_DanteDB_DataProvider" u="False" ss="" tn="LeadCache" nmo="1" f=" [Customer_ID] = #Customer_ID AND (Last_Modification_Date&gt;= #LastModifiedDate) AND ([Account_External_ID] in #IDs)" ad="False" em="False" td="False" ic="" dd="" l="1000" tw="False" sr="50000" htw="24" mtw="60" emt="False" acd="False" mgf="False" eo="2" emd="False" eo_sftp="2"><ts><t n="LastModifiedDate" t="1" qn="Q_Get_TimeRange_LastModifiedLeadCache" cn="Diff_Of_Last_ModifiedDays" m="0"><schemas /><specs /></t><t n="Customer_ID" t="1" qn="Q_Get_TimeRange_LastModifiedLeadCache" cn="Const_TenantName" m="0"><schemas /><specs /></t><t n="IDs" t="2" qn="Q_Get_TimeRange_SFDC_Contact" cn="SFDC_Contact_ID" m="100"><schemas /><specs /></t></ts><mcs><mc cn="Account_External_ID" /><mc cn="Customer_ID" /><mc cn="Last_Modification_Date" /></mcs></rds></rdss>'
    else:
      step1xml = '<rdss><rds n="LoadLeadCache_Lead" w="Workspace" sn="LeadCache_Lead" cn="SQL_DanteDB_DataProvider" u="False" ss="" tn="LeadCache" nmo="1" f=" [Customer_ID] = #Customer_ID AND Last_Modification_Date&gt;= #LastModifiedDate AND Account_External_ID in #IDs" ad="False" em="False" td="False" ic="" dd="" l="1000" tw="False" sr="50000" htw="24" mtw="60" emt="False" acd="False" mgf="False" eo="2" emd="False" eo_sftp="2"><ts><t n="LastModifiedDate" t="1" qn="Q_Get_TimeRange_LastModifiedLeadCache" cn="Diff_Of_Last_ModifiedDays" m="0"><schemas /><specs /></t><t n="Customer_ID" t="1" qn="Q_Get_TimeRange_LastModifiedLeadCache" cn="Const_TenantName" m="0"><schemas /><specs /></t><t n="IDs" t="2" qn="Q_Get_TimeRange_SFDC_Lead" cn="SFDC_Lead_Contact_ID" m="100"><schemas /><specs /></t></ts><mcs><mc cn="Account_External_ID" /><mc cn="Customer_ID" /><mc cn="Last_Modification_Date" /></mcs></rds><rds n="LoadLeadCache_Contact" w="Workspace" sn="LeadCache_Contact" cn="SQL_DanteDB_DataProvider" u="False" ss="" tn="LeadCache" nmo="1" f=" [Customer_ID] = #Customer_ID AND (Last_Modification_Date&gt;= #LastModifiedDate) AND ([Account_External_ID] in #IDs)" ad="False" em="False" td="False" ic="" dd="" l="1000" tw="False" sr="50000" htw="24" mtw="60" emt="False" acd="False" mgf="False" eo="2" emd="False" eo_sftp="2"><ts><t n="LastModifiedDate" t="1" qn="Q_Get_TimeRange_LastModifiedLeadCache" cn="Diff_Of_Last_ModifiedDays" m="0"><schemas /><specs /></t><t n="Customer_ID" t="1" qn="Q_Get_TimeRange_LastModifiedLeadCache" cn="Const_TenantName" m="0"><schemas /><specs /></t><t n="IDs" t="2" qn="Q_Get_TimeRange_SFDC_Contact" cn="SFDC_Lead_Contact_ID" m="100"><schemas /><specs /></t></ts><mcs><mc cn="Account_External_ID" /><mc cn="Customer_ID" /><mc cn="Last_Modification_Date" /></mcs></rds></rdss>'
    lgm.setLoadGroupFunctionality( 'LoadLeadCache', step1xml )

    lgm.createLoadGroup( 'PushDataToDante_Bulk_Step1', 'BulkScoring\Dante', 'PushDataToDante_Bulk_Step1', True, False )
    step2xml = ''
    if type == 'MKTO':
      step2xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PushToDante_Bulk" queryAlias="Q_Timestamp_PushToDante_Bulk" sw="Workspace" schemaName="Timestamp_PushToDante_Stage" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="MKTO_LeadRecord_ID" itcn="MKTO_LeadRecord_ID" /><cm qcn="SFDC_Contact_ID" itcn="SFDC_Contact_ID" /><cm qcn="SFDC_Lead_ID" itcn="SFDC_Lead_ID" /><cm qcn="Time_OfSubmission_PushToDante" itcn="Time_OfSubmission_PushToDante" /><cm qcn="ScoreDate" itcn="ScoreDate" /></cms></extractQuery></extractQueries>'
    elif type =='ELQ':
      step2xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PushToDante_Bulk" queryAlias="Q_Timestamp_PushToDante_Bulk" sw="Workspace" schemaName="Timestamp_PushToDante_Stage" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="ELQ_Contact_ContactID" itcn="ELQ_Contact_ContactID" /><cm qcn="ScoreDate" itcn="ScoreDate" /><cm qcn="SFDC_Contact_ID" itcn="SFDC_Contact_ID" /><cm qcn="SFDC_Lead_ID" itcn="SFDC_Lead_ID" /><cm qcn="Time_OfSubmission_PushToDante" itcn="Time_OfSubmission_PushToDante" /></cms></extractQuery></extractQueries>'
    else:
      step2xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PushToDante_Bulk" queryAlias="Q_Timestamp_PushToDante_Bulk" sw="Workspace" schemaName="Timestamp_PushToDante_Stage" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="ScoreDate" itcn="ScoreDate" /><cm qcn="SFDC_Lead_Contact_ID" itcn="SFDC_Lead_Contact_ID" /><cm qcn="Time_OfSubmission_PushToDante" itcn="Time_OfSubmission_PushToDante" /></cms></extractQuery></extractQueries>'
    lgm.setLoadGroupFunctionality( 'PushDataToDante_Bulk_Step1', step2xml )

    lgm.createLoadGroup( 'PushDataToDante_Bulk_Step2', 'BulkScoring\Dante', 'PushDataToDante_Bulk_Step2', True, False )
    #step3xml = ''
    #if type == 'MKTO':
      #step3xml = '<ngs><ng n="InsightsAllSteps" /></ngs>'
    #elif type =='ELQ':
      #step3xml = '<ngs><ng n="InsightsAllSteps" /></ngs>'
    #else:
      #step3xml = '<ngs><ng n="InsightsAllSteps" /></ngs>'
    #lgm.setLoadGroupFunctionality( 'PushDataToDante_Bulk_Step2', step3xml )

    lgm.createLoadGroup( 'PushDataToDante_Bulk', 'BulkScoring\Dante', 'PushDataToDante_Bulk', True, False )
    #step4xml = ''
    #if type == 'MKTO':
      #step4xml = '<ngs><ng n="LoadLeadCache" /><ng n="PushDataToDante_Bulk_Step1" /><ng n="PushDataToDante_Bulk_Step2" /></ngs>'
    #elif type =='ELQ':
      #step4xml = '<ngs><ng n="LoadLeadCache" /><ng n="PushDataToDante_Bulk_Step1" /><ng n="PushDataToDante_Bulk_Step2" /></ngs>'
    #else:
      #step4xml = '<ngs><ng n="LoadLeadCache" /><ng n="PushDataToDante_Bulk_Step1" /><ng n="PushDataToDante_Bulk_Step2" /></ngs>'
    #lgm.setLoadGroupFunctionality( 'PushDataToDante_Bulk', step4xml )

    ptld1  = etree.fromstring( lgm.getLoadGroup('BulkScoring_PushToLeadDestination').encode('ascii', 'xmlcharrefreplace') )
    step1_1 = etree.fromstring( lgm.getLoadGroup('PushToLeadDestination').encode('ascii', 'xmlcharrefreplace') )
    lgm.setLoadGroup( etree.tostring(step1_1) )

    ptld2  = etree.fromstring( lgm.getLoadGroup('PushDataToDante_Bulk_Step2').encode('ascii', 'xmlcharrefreplace') )
    step2_1 = etree.fromstring( lgm.getLoadGroup('InsightsAllSteps').encode('ascii', 'xmlcharrefreplace') )
    lgm.setLoadGroup( etree.tostring(step2_1) )

    ptld3  = etree.fromstring( lgm.getLoadGroup('PushDataToDante_Bulk').encode('ascii', 'xmlcharrefreplace') )
    step3_1 = etree.fromstring( lgm.getLoadGroup('LoadLeadCache').encode('ascii', 'xmlcharrefreplace') )
    step3_2 = etree.fromstring( lgm.getLoadGroup('PushDataToDante_Bulk_Step1').encode('ascii', 'xmlcharrefreplace') )
    step3_3 = etree.fromstring( lgm.getLoadGroup('PushDataToDante_Bulk_Step2').encode('ascii', 'xmlcharrefreplace') )
    lgm.setLoadGroup( etree.tostring(step3_1) )
    lgm.setLoadGroup( etree.tostring(step3_2) )
    lgm.setLoadGroup( etree.tostring(step3_3) )

    ptld1.set( 'ng', 'True' )
    lgm.setLoadGroup( etree.tostring(ptld1) )
    ngsxml = '<ngs><ng n="PushToLeadDestination" /></ngs>'
    lgm.setLoadGroupFunctionality( 'BulkScoring_PushToLeadDestination', ngsxml )

    ptld2.set( 'ng', 'True' )
    lgm.setLoadGroup( etree.tostring(ptld2) )
    ngsxml = '<ngs><ng n="InsightsAllSteps" /></ngs>'
    lgm.setLoadGroupFunctionality( 'PushDataToDante_Bulk_Step2', ngsxml )

    ptld3.set( 'ng', 'True' )
    lgm.setLoadGroup( etree.tostring(ptld3) )
    ngsxml = '<ngs><ng n="LoadLeadCache" /><ng n="PushDataToDante_Bulk_Step1" /><ng n="PushDataToDante_Bulk_Step2" /></ngs>'
    lgm.setLoadGroupFunctionality( 'PushDataToDante_Bulk', ngsxml )



    lgm.commit()

    success = True

    return success

