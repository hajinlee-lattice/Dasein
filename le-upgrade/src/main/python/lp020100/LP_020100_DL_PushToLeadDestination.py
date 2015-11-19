#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from lxml import etree
from appsequence import Applicability, StepBase
import re


class LP_020100_DL_PushToLeadDestination(StepBase):
  name = 'LP_020100_DL_PushToLeadDestination'
  description = 'Reset logic of the PushToLeadDestination'
  version = '$Rev$'
  _scoreDateField = ''
  _scoreField = ''

  def __init__(self, forceApply=False):
    super(LP_020100_DL_PushToLeadDestination, self).__init__(forceApply)

  def getScoreField(self):
    return self._scoreField

  def getScoreDateField(self):
    return self._scoreDateField

  def setScoreField(self, str):
    self._scoreField = str

  def setScoreDateField(self, str):
    self._scoreDateField = str


  def parseScoreField(selfs, str, type):
    if type == 'MKTO':
      s = re.search('<cm scn=\"Score\" tcn=\"(.*?)\"/>', str)
    elif type == 'ELQ':
      s = re.search('<cm scn=\"C_Lattice_Predictive_Score1\" tcn=\"(.*?)\"/>', str)
    else:
      s = re.search('<cm scn=\"C_Lattice_Predictive_Score1\" tcn=\"(.*?)\"/>', str)

    if not s:
      print "Error get the Score Field failed"
      return None
    return s.group(1)

  def parseScoreDateField(self, str, type):
    if type == 'MKTO':
      s = re.search('<cm scn=\"Score_Date_Time\" tcn=\"(.*?)\"/>', str)
    elif type == 'ELQ':
      s = re.search('<cm scn=\"C_Lattice_LastScoreDate1\" tcn=\"(.*?)\"/>', str)
    else:
      s = re.search('<cm scn=\"C_Lattice_LastScoreDate1\" tcn=\"(.*?)\"/>', str)

    if not s:
      print "Error get the Score Field failed"
      return None
    return s.group(1)

  def getApplicability(self, appseq):

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText('template_type')

    if not lgm.hasLoadGroup('PushToLeadDestination'):
      return Applicability.cannotApplyFail

    ptld_lbo_xml = lgm.getLoadGroupFunctionality('PushToLeadDestination_Step1', 'lssbardouts')

    self.setScoreField(self.parseScoreField(ptld_lbo_xml, type))
    self.setScoreDateField(self.parseScoreDateField(ptld_lbo_xml, type))

    if not self.getScoreField() or not self.getScoreDateField():
      return Applicability.cannotApplyFail

    ptldxml = lgm.getLoadGroupFunctionality('PushToLeadDestination', 'ngs')
    hasModPTLD = ptldxml != '<ngs />'
    hasStep1 = lgm.hasLoadGroup('LoadScoredLeads_Step1')
    hasStep2 = lgm.hasLoadGroup('LoadScoredLeads_Step2')
    hasStep3 = lgm.hasLoadGroup('PushDataToDante_Hourly')
    hasStep4 = lgm.hasLoadGroup('PushLeadsInDanteToDestination')

    if hasModPTLD and hasStep1 and hasStep2 and hasStep3 and hasStep4:
      return Applicability.alreadyAppliedPass

    return Applicability.canApply

  def apply(self, appseq):

    success = False

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText('template_type')

    conn_mgr = appseq.getConnectionMgr()
    tenantName = conn_mgr.getTenantName()
    lgm.createLoadGroup('LoadScoredLeads_Step1', 'OperationalProcess\Standard', 'LoadScoredLeads_Step1', True, False)
    step1xml = ''
    if type == 'MKTO':
      step1xml = '<lssbardouts><lssbardout n="Q_MKTO_Leads_Score" deid="__DeploymentID__" adid="True" w="Workspace" sn="Bard_LeadScoreStage" ' \
                 'qn="Q_LeadScoreForLeadDestination_Query" bdp="SQL_LSSBard" dp="Marketo_DataProvider" ' \
                 'tn="LeadRecord" er="4" ad="True" mfc="2" ntr="1" nob="5" eo="1" mkc="LeadID"><cms><cm ' \
                 'scn="MKTO_LeadRecord_ID" tcn="Id" /><cm scn="Score_Date_Time" ' \
                 'tcn="latticeforleads__Last_Score_Date__c" /><cm scn="Score" tcn="latticeforleads__Score__c" ' \
                 '/></cms><kcs><kc k="Id" /></kcs></lssbardout></lssbardouts>'

    elif type == 'ELQ':
      step1xml = '<lssbardouts><lssbardout n="Q_ELQ_Contact_Score" deid="__DeploymentID__" adid="True" w="Workspace" sn="Bard_LeadScoreStage" ' \
                 'qn="Q_ELQ_Contact_Score" bdp="SQL_LSSBard" dp="Eloqua_Bulk_DataProvider" tn="Contact" er="4" ' \
                 'ad="True" mfc="2" ntr="1" nob="5" eo="1" mkc="LeadID"><cms><cm scn="ContactID" tcn="ContactID" ' \
                 '/><cm scn="C_Lattice_Predictive_Score1" tcn="latticeforleads__Score__c" /><cm ' \
                 'scn="C_Lattice_LastScoreDate1" tcn="latticeforleads__Last_Score_Date__c" /></cms><kcs><kc k="ContactID" ' \
                 '/></kcs></lssbardout></lssbardouts>'

    else:
      step1xml = '<lssbardouts><lssbardout n="Q_SFDC_Lead_Contact_Score" deid="__DeploymentID__" adid="True" w="Workspace" ' \
                 'sn="Bard_LeadScoreStage" qn="Q_SFDC_Lead_Score" bdp="SQL_LSSBard" dp="SFDC_DataProvider" ' \
                 'tn="Contact" er="4" ad="False" mfc="10" ntr="3" nob="5" eo="1" mkc="LeadID"><cms><cm ' \
                 'scn="SFDC_Lead_Contact_ID" tcn="Id" /><cm scn="C_Lattice_Predictive_Score1" ' \
                 'tcn="latticeforleads__Score__c" /><cm scn="C_Lattice_LastScoreDate1" ' \
                 'tcn="latticeforleads__Last_Score_Date__c" /></cms><kcs><kc k="Id" /></kcs></lssbardout></lssbardouts>'

    step1xml = step1xml.replace('latticeforleads__Score__c', self.getScoreField())
    step1xml = step1xml.replace('latticeforleads__Last_Score_Date__c', self.getScoreDateField())
    step1xml = step1xml.replace('__DeploymentID__', tenantName)
    lgm.setLoadGroupFunctionality('LoadScoredLeads_Step1', step1xml)

    lgm.createLoadGroup('LoadScoredLeads_Step2', 'OperationalProcess\Standard', 'LoadScoredLeads_Step2', True, False)
    step2xml = ''
    if type == 'MKTO':
      step2xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_LoadScoredLeads" ' \
                 'queryAlias="Q_Timestamp_LoadScoredLeads" sw="Workspace" schemaName="Timestamp_LoadScoredLeads" ' \
                 'at="False" ucm="True"><schemas /><specs /><cms><cm qcn="MKTO_LeadRecord_ID" ' \
                 'itcn="MKTO_LeadRecord_ID" /><cm qcn="Time_OfCompletion_LoadScoredLeads" ' \
                 'itcn="Time_OfCompletion_LoadScoredLeads" /></cms></extractQuery><extractQuery qw="Workspace" ' \
                 'queryName="Bard_LeadScoreStage_ExtractForHistory" ' \
                 'queryAlias="Bard_LeadScoreStage_ExtractForHistory" sw="Workspace" ' \
                 'schemaName="Bard_LeadScoreHistory" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="LeadID" ' \
                 'itcn="LeadID" /><cm qcn="Score" itcn="Score" /><cm qcn="Bucket_Display_Name" ' \
                 'itcn="Bucket_Display_Name" /><cm qcn="Play_Display_Name" itcn="Play_Display_Name" /><cm ' \
                 'qcn="RawScore" itcn="RawScore" /><cm qcn="Probability" itcn="Probability" /><cm qcn="Lift" ' \
                 'itcn="Lift" /><cm qcn="Percentile" itcn="Percentile" /><cm qcn="ScoreDate" itcn="ScoreDate" ' \
                 '/></cms></extractQuery></extractQueries>'
    elif type == 'ELQ':
      step2xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_LoadScoredLeads" ' \
                 'queryAlias="Q_Timestamp_LoadScoredLeads" sw="Workspace" schemaName="Timestamp_LoadScoredLeads" ' \
                 'at="False" ucm="True"><schemas /><specs /><cms><cm qcn="ELQ_Contact_ContactID" ' \
                 'itcn="ELQ_Contact_ContactID" /><cm qcn="Time_OfCompletion_LoadScoredLeads" ' \
                 'itcn="Time_OfCompletion_LoadScoredLeads" /></cms></extractQuery><extractQuery qw="Workspace" ' \
                 'queryName="Bard_LeadScoreStage_ExtractForHistory" ' \
                 'queryAlias="Bard_LeadScoreStage_ExtractForHistory" sw="Workspace" ' \
                 'schemaName="Bard_LeadScoreHistory" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="LeadID" ' \
                 'itcn="LeadID" /><cm qcn="Score" itcn="Score" /><cm qcn="Bucket_Display_Name" ' \
                 'itcn="Bucket_Display_Name" /><cm qcn="Play_Display_Name" itcn="Play_Display_Name" /><cm ' \
                 'qcn="RawScore" itcn="RawScore" /><cm qcn="Probability" itcn="Probability" /><cm qcn="Lift" ' \
                 'itcn="Lift" /><cm qcn="Percentile" itcn="Percentile" /><cm qcn="ScoreDate" itcn="ScoreDate" ' \
                 '/></cms></extractQuery></extractQueries>'
    else:
      step2xml = '<extractQueries><extractQuery qw="Workspace" queryName="Bard_LeadScoreStage_ExtractForHistory" ' \
                 'queryAlias="Bard_LeadScoreStage_ExtractForHistory" sw="Workspace" ' \
                 'schemaName="Bard_LeadScoreHistory" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="LeadID" ' \
                 'itcn="LeadID" /><cm qcn="Score" itcn="Score" /><cm qcn="Bucket_Display_Name" ' \
                 'itcn="Bucket_Display_Name" /><cm qcn="Play_Display_Name" itcn="Play_Display_Name" /><cm ' \
                 'qcn="RawScore" itcn="RawScore" /><cm qcn="Probability" itcn="Probability" /><cm qcn="Lift" ' \
                 'itcn="Lift" /><cm qcn="Percentile" itcn="Percentile" /><cm qcn="ScoreDate" itcn="ScoreDate" ' \
                 '/></cms></extractQuery><extractQuery qw="Workspace" queryName="Q_Timestamp_LoadScoredLeads" ' \
                 'queryAlias="Q_Timestamp_LoadScoredLeads" sw="Workspace" schemaName="Timestamp_LoadScoredLeads" ' \
                 'at="False" ucm="True"><schemas /><specs /><cms><cm qcn="SFDC_Lead_Contact_ID" ' \
                 'itcn="SFDC_Lead_Contact_ID" /><cm qcn="Time_OfCompletion_LoadScoredLeads" ' \
                 'itcn="Time_OfCompletion_LoadScoredLeads" /></cms></extractQuery></extractQueries>'
    lgm.setLoadGroupFunctionality('LoadScoredLeads_Step2', step2xml)

    lgm.createLoadGroup('PushDataToDante_Hourly', 'OperationalProcess\Standard', 'PushDataToDante_Hourly', True, False)
    step3xml = ''
    if type == 'MKTO':
      step3xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PushToDante_Stage" ' \
                 'queryAlias="Q_Timestamp_PushToDante_Stage" sw="Workspace" schemaName="Timestamp_PushToDante_Stage" ' \
                 'at="False" ucm="True"><schemas /><specs /><cms><cm qcn="MKTO_LeadRecord_ID" ' \
                 'itcn="MKTO_LeadRecord_ID" /><cm qcn="SFDC_Contact_ID" itcn="SFDC_Contact_ID" /><cm ' \
                 'qcn="SFDC_Lead_ID" itcn="SFDC_Lead_ID" /><cm qcn="Time_OfSubmission_PushToDante" ' \
                 'itcn="Time_OfSubmission_PushToDante" /><cm qcn="ScoreDate" itcn="ScoreDate" ' \
                 '/></cms></extractQuery></extractQueries>'
    elif type == 'ELQ':
      step3xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PushToDante_Stage" ' \
                 'queryAlias="Q_Timestamp_PushToDante_Stage" sw="Workspace" schemaName="Timestamp_PushToDante_Stage" ' \
                 'at="False" ucm="True"><schemas /><specs /><cms><cm qcn="ELQ_Contact_ContactID" ' \
                 'itcn="ELQ_Contact_ContactID" /><cm qcn="ScoreDate" itcn="ScoreDate" /><cm qcn="SFDC_Contact_ID" ' \
                 'itcn="SFDC_Contact_ID" /><cm qcn="SFDC_Lead_ID" itcn="SFDC_Lead_ID" /><cm ' \
                 'qcn="Time_OfSubmission_PushToDante" itcn="Time_OfSubmission_PushToDante" ' \
                 '/></cms></extractQuery></extractQueries>'
    else:
      step3xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PushToDante_Stage" ' \
                 'queryAlias="Q_Timestamp_PushToDante_Stage" sw="Workspace" schemaName="Timestamp_PushToDante_Stage" ' \
                 'at="False" ucm="True"><schemas /><specs /><cms><cm qcn="SFDC_Lead_Contact_ID" ' \
                 'itcn="SFDC_Lead_Contact_ID" /><cm qcn="ScoreDate" itcn="ScoreDate" /><cm ' \
                 'qcn="Time_OfSubmission_PushToDante" itcn="Time_OfSubmission_PushToDante" /><cm ' \
                 'qcn="SFDC_Contact_ID" itcn="SFDC_Contact_ID" /><cm qcn="SFDC_Lead_ID" itcn="SFDC_Lead_ID" ' \
                 '/></cms></extractQuery></extractQueries>'
    lgm.setLoadGroupFunctionality('PushDataToDante_Hourly', step3xml)

    lgm.createLoadGroup('PushLeadsInDanteToDestination', 'OperationalProcess\Standard', 'PushLeadsInDanteToDestination',
                        True, False)
    step4xml = ''
    if type == 'MKTO':
      step4xml = '<targetQueries><targetQuery w="Workspace" t="2" name="Q_LeadScoreForLeadDestination_Query" ' \
                 'alias="Q_LeadScoreForLeadDestination_Query" isc="False" threshold="10000" fsTableName="LeadRecord" ' \
                 'sourceType="1" jobType="20" ignoreOptionsValue="0" exportToDestDirectly="True" exportRule="4" ' \
                 'fileExt="bcp" rowTerminator="\\0\\r" columnTerminator="\\0" edts="False" destType="MARKETO" ' \
                 'destDataProvider="Marketo_DataProvider" cto="0"><schemas /><specs ' \
                 '/><fsColumnMappings><fsColumnMapping queryColumnName="MKTO_LeadRecord_ID" fsColumnName="Id" ' \
                 'formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" ' \
                 '/><fsColumnMapping queryColumnName="Score_Date_Time" ' \
                 'fsColumnName="latticeforleads__Last_Score_Date__c" formatter="" type="0" ignoreType="0" ' \
                 'ignoreOptions="0" formatColumnName="False" charsToFormat="" /><fsColumnMapping ' \
                 'queryColumnName="Score" fsColumnName="latticeforleads__Score__c" formatter="" type="0" ' \
                 'ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" ' \
                 '/></fsColumnMappings><excludeColumns><ec c="MKTO_LeadRecord_ID" /><ec c="Score_Date_Time" /><ec ' \
                 'c="Score" /></excludeColumns><validationQueries /><constantRows /><kcs><kc n="Id" /></kcs><fut ' \
                 'dn="" d="" n="" iet="False" iets="False" t="1" /></targetQuery></targetQueries>'
    elif type == 'ELQ':
      step4xml = '<targetQueries><targetQuery w="Workspace" t="2" name="Q_ELQ_Contact_Score" ' \
                 'alias="Q_ELQ_Contact_Score" isc="False" threshold="10000" fsTableName="Contact" sourceType="1" ' \
                 'jobType="20" ignoreOptionsValue="0" exportToDestDirectly="True" exportRule="4" fileExt="bcp" ' \
                 'rowTerminator="\\0\\r" columnTerminator="\\0" edts="False" destType="ELOQUA BULK" ' \
                 'destDataProvider="Eloqua_Bulk_DataProvider" cto="0"><schemas /><specs ' \
                 '/><fsColumnMappings><fsColumnMapping queryColumnName="ContactID" fsColumnName="ContactID" ' \
                 'formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" ' \
                 '/><fsColumnMapping queryColumnName="C_Lattice_LastScoreDate1" ' \
                 'fsColumnName="latticeforleads__Last_Score_Date__c" formatter="" type="0" ignoreType="0" ignoreOptions="0" ' \
                 'formatColumnName="False" charsToFormat="" /><fsColumnMapping ' \
                 'queryColumnName="C_Lattice_Predictive_Score1" fsColumnName="latticeforleads__Score__c" ' \
                 'formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" ' \
                 '/></fsColumnMappings><excludeColumns /><validationQueries /><constantRows /><kcs><kc n="ContactID" ' \
                 '/></kcs><fut dn="" d="" n="" iet="False" iets="False" t="1" /></targetQuery></targetQueries>'
    else:
      step4xml = '<targetQueries><targetQuery w="Workspace" t="2" name="Q_SFDC_Contact_Score" ' \
                 'alias="Q_SFDC_Contact_Score" isc="False" threshold="10000" fsTableName="Contact" sourceType="1" ' \
                 'jobType="20" ignoreOptionsValue="0" exportToDestDirectly="True" exportRule="4" fileExt="bcp" ' \
                 'rowTerminator="\\0\\r" columnTerminator="\\0" edts="False" destType="SFDC" ' \
                 'destDataProvider="SFDC_DataProvider" cto="0"><schemas /><specs /><fsColumnMappings><fsColumnMapping ' \
                 'queryColumnName="C_Lattice_Predictive_Score1" fsColumnName="latticeforleads__Score__c" formatter="" ' \
                 'type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" ' \
                 '/><fsColumnMapping queryColumnName="C_Lattice_LastScoreDate1" ' \
                 'fsColumnName="latticeforleads__Last_Score_Date__c" formatter="" type="0" ignoreType="0" ' \
                 'ignoreOptions="0" formatColumnName="False" charsToFormat="" /><fsColumnMapping ' \
                 'queryColumnName="SFDC_Lead_Contact_ID" fsColumnName="Id" formatter="" type="0" ignoreType="0" ' \
                 'ignoreOptions="0" formatColumnName="False" charsToFormat="" /></fsColumnMappings><excludeColumns ' \
                 '/><validationQueries /><constantRows /><kcs><kc n="Id" /></kcs><fut dn="" d="" n="" iet="False" ' \
                 'iets="False" t="1" /></targetQuery><targetQuery w="Workspace" t="2" name="Q_SFDC_Lead_Score" ' \
                 'alias="Q_SFDC_Lead_Score" isc="False" threshold="10000" fsTableName="Lead" sourceType="1" ' \
                 'jobType="20" ignoreOptionsValue="0" exportToDestDirectly="True" exportRule="4" fileExt="bcp" ' \
                 'rowTerminator="\\0\\r" columnTerminator="\\0" edts="False" destType="SFDC" ' \
                 'destDataProvider="SFDC_DataProvider" cto="0"><schemas /><specs /><fsColumnMappings><fsColumnMapping ' \
                 'queryColumnName="C_Lattice_Predictive_Score1" fsColumnName="latticeforleads__Score__c" formatter="" ' \
                 'type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" ' \
                 '/><fsColumnMapping queryColumnName="C_Lattice_LastScoreDate1" ' \
                 'fsColumnName="latticeforleads__Last_Score_Date__c" formatter="" type="0" ignoreType="0" ' \
                 'ignoreOptions="0" formatColumnName="False" charsToFormat="" /><fsColumnMapping ' \
                 'queryColumnName="SFDC_Lead_Contact_ID" fsColumnName="Id" formatter="" type="0" ignoreType="0" ' \
                 'ignoreOptions="0" formatColumnName="False" charsToFormat="" /></fsColumnMappings><excludeColumns ' \
                 '/><validationQueries /><constantRows /><kcs><kc n="Id" /></kcs><fut dn="" d="" n="" iet="False" ' \
                 'iets="False" t="1" /></targetQuery></targetQueries>'

    step4xml = step4xml.replace('latticeforleads__Score__c', self.getScoreField())
    step4xml = step4xml.replace('latticeforleads__Last_Score_Date__c', self.getScoreDateField())
    lgm.setLoadGroupFunctionality('PushLeadsInDanteToDestination', step4xml)

    step5xml = ''
    if type == 'MKTO':
      step5xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PushToDestination" ' \
                 'queryAlias="Q_Timestamp_PushToDestination" sw="Workspace" schemaName="Timestamp_PushToDestination" ' \
                 'at="False" ucm="True"><schemas /><specs /><cms><cm qcn="MKTO_LeadRecord_ID" ' \
                 'itcn="MKTO_LeadRecord_ID" /><cm qcn="Time_OfCompletion_PushToDestination" ' \
                 'itcn="Time_OfCompletion_PushToDestination" /></cms></extractQuery></extractQueries>'
    elif type == 'ELQ':
      step5xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PushToDestination" ' \
                 'queryAlias="Q_Timestamp_PushToDestination" sw="Workspace" schemaName="Timestamp_PushToDestination" ' \
                 'at="False" ucm="True"><schemas /><specs /><cms><cm qcn="ELQ_Contact_ContactID" ' \
                 'itcn="ELQ_Contact_ContactID" /><cm qcn="Time_OfCompletion_PushToDestination" ' \
                 'itcn="Time_OfCompletion_PushToDestination" /></cms></extractQuery></extractQueries>'
    else:
      step5xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PushToDestination" ' \
                 'queryAlias="Q_Timestamp_PushToDestination" sw="Workspace" schemaName="Timestamp_PushToDestination" ' \
                 'at="False" ucm="True"><schemas /><specs /><cms><cm qcn="SFDC_Lead_Contact_ID" ' \
                 'itcn="SFDC_Lead_Contact_ID" /><cm qcn="Time_OfCompletion_PushToDestination" ' \
                 'itcn="Time_OfCompletion_PushToDestination" /></cms></extractQuery></extractQueries>'
    lgm.setLoadGroupFunctionality('PushLeadsInDanteToDestination', step5xml)

    ptld = etree.fromstring(lgm.getLoadGroup('PushToLeadDestination').encode('ascii', 'xmlcharrefreplace'))

    ptld.set('ng', 'True')
    lgm.setLoadGroup(etree.tostring(ptld))
    ngsxml = '<ngs><ng n="LoadScoredLeads_Step1"/><ng n="LoadScoredLeads_Step2"/><ng n="PushDataToDante_Hourly"/><ng ' \
             'n="InsightsAllSteps"/><ng n="PushLeadsInDanteToDestination"/></ngs>'
    lgm.setLoadGroupFunctionality('PushToLeadDestination', ngsxml)

    return True
