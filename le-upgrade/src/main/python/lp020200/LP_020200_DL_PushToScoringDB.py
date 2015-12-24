#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase


class LP_020200_DL_PushToScoringDB(StepBase):
  name = 'LP_020200_DL_PushToScoringDB'
  description = 'Separate into 2 LGs fro the Load Group:PushToScoringDB'
  version = '$Rev: 70934 $'

  def __init__(self, forceApply=False):
    super(LP_020200_DL_PushToScoringDB, self).__init__(forceApply)

  def getApplicability(self, appseq):

    lgm = appseq.getLoadGroupMgr()
    if not lgm.hasLoadGroup('PushToScoringDB_Step1') \
      and not lgm.hasLoadGroup('PushToScoringDB_Step2') \
      and not lgm.hasLoadGroup('PushToScoringDB'):

      return Applicability.cannotApplyPass
    return Applicability.canApply

  def apply(self, appseq):

    success = False

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText('template_type')

    lgm.createLoadGroup('PushToScoringDB_Step1', 'OperationalProcess\Standard', 'PushToScoringDB_Step1', True, False)
    step1xml = ''
    if type == 'MKTO':
      step1xml = '''
    <group name="PushToScoringDB_Step1" alias="PushToScoringDB_Step1" w="Workspace" type="1" scheduleType="0" allowUserChangeScheduleType="False" visibleForEndUser="True" threshold="10000" launchExpiredDays="7" createdBy="richard.liu@lattice-engines.com" path="OperationalProcess\Standard" autoGenerated="False" validationValidMinutes="120" autoClearOnFailure="False" mergeRulesSaved="True" ng="False">
      <schemas />
      <visiDBConfigurationWithMacros />
      <targetQueries />
      <targetQuerySequences />
      <rdss />
      <validationExtracts />
      <ces />
      <extractQueries />
      <extractQuerySequences />
      <leafExtracts />
      <launchExtracts />
      <jobs />
      <pdmatches />
      <leadscroings />
      <lssbardins>
        <lssbardin n="PushToScoringDB_LSS_Bard_In" w="Workspace" mn="Tahoe" qn="Q_PLS_Scoring_Incremental" dp="SQL_LSSBard" eac="True">
          <cms />
        </lssbardin>
      </lssbardins>
      <lssbardouts />
      <lds />
      <ecs />
      <gCs />
    </group>'''

    elif type == 'ELQ':
      step1xml = '''
    <group name="PushToScoringDB_Step1" alias="PushToScoringDB_Step1" w="Workspace" type="1" scheduleType="0" allowUserChangeScheduleType="False" visibleForEndUser="True" threshold="10000" launchExpiredDays="7" createdBy="VZhao@Lattice-Engines.com" path="OperationalProcess\Standard" autoGenerated="False" validationValidMinutes="120" autoClearOnFailure="False" mergeRulesSaved="True" ng="False">
      <schemas />
      <visiDBConfigurationWithMacros />
      <targetQueries />
      <targetQuerySequences />
      <rdss />
      <validationExtracts />
      <ces />
      <extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Summary_DownloadsBeforeScoring" queryAlias="Q_Summary_DownloadsBeforeScoring" sw="Workspace" schemaName="Summary_DownloadsBeforeScoring" at="False" ucm="True">
          <schemas />
          <specs />
          <cms>
            <cm qcn="ID" itcn="ID" />
            <cm qcn="Time_OfSubmission_PushToScoring" itcn="Time_OfSubmission_PushToScoring" />
            <cm qcn="Count_NumberOfNewLeads" itcn="Count_NumberOfNewLeads" />
            <cm qcn="Count_NumberOfModifiedLeads" itcn="Count_NumberOfModifiedLeads" />
            <cm qcn="Thrttl_LastDownloadAtMax" itcn="Thrttl_LastDownloadAtMax" />
            <cm qcn="Thrttl_LastDownloadMinDateModified" itcn="Thrttl_LastDownloadMinDateModified" />
            <cm qcn="Thrttl_LastDownloadMaxDateModified" itcn="Thrttl_LastDownloadMaxDateModified" />
            <cm qcn="Thrttl_LastDownloadMaxContactID" itcn="Thrttl_LastDownloadMaxContactID" />
            <cm qcn="Thrttl_LastScoreMaxContactID" itcn="Thrttl_LastScoreMaxContactID" />
            <cm qcn="Thrttl_LastDownloadUpperLimitDateModified" itcn="Thrttl_LastDownloadUpperLimitDateModified" />
          </cms>
        </extractQuery>
      </extractQueries>
      <extractQuerySequences />
      <leafExtracts />
      <launchExtracts />
      <jobs />
      <pdmatches />
      <leadscroings />
      <lssbardins>
        <lssbardin n="PushToScoringDB_LSS_Bard_In" w="Workspace" mn="Tahoe" qn="Q_PLS_Scoring_Incremental" dp="SQL_LSSBard" eac="True">
          <cms />
        </lssbardin>
      </lssbardins>
      <lssbardouts />
      <lds />
      <ecs />
      <gCs />
    </group>'''

    else:
      step1xml = '''
    <group name="PushToScoringDB_Step1" alias="PushToScoringDB_Step1" w="Workspace" type="1" scheduleType="0" allowUserChangeScheduleType="False" visibleForEndUser="True" threshold="10000" launchExpiredDays="7" createdBy="VZhao@Lattice-Engines.com" path="OperationalProcess\Standard" autoGenerated="False" validationValidMinutes="120" autoClearOnFailure="False" mergeRulesSaved="True" ng="False">
      <schemas />
      <visiDBConfigurationWithMacros />
      <targetQueries />
      <targetQuerySequences />
      <rdss />
      <validationExtracts />
      <ces />
      <extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Summary_DownloadsBeforeScoring" queryAlias="Q_Summary_DownloadsBeforeScoring" sw="Workspace" schemaName="Summary_DownloadsBeforeScoring" at="False" ucm="True">
          <schemas />
          <specs />
          <cms>
            <cm qcn="ID" itcn="ID" />
            <cm qcn="Time_OfSubmission_PushToScoring" itcn="Time_OfSubmission_PushToScoring" />
            <cm qcn="Count_NumberOfContacts" itcn="Count_NumberOfContacts" />
            <cm qcn="Count_NumberOfModifiedContacts" itcn="Count_NumberOfModifiedContacts" />
            <cm qcn="Thrttl_LowerLimit_ID_ForNewContacts" itcn="Thrttl_LowerLimit_ID_ForNewContacts" />
            <cm qcn="Thrttl_LowerLimit_CreatedDate_Contact" itcn="Thrttl_LowerLimit_CreatedDate_Contact" />
            <cm qcn="Thrttl_LowerLimit_ID_ForModifiedContacts" itcn="Thrttl_LowerLimit_ID_ForModifiedContacts" />
            <cm qcn="Thrttl_LowerLimit_LastModifiedDate_ForModifiedContacts" itcn="Thrttl_LowerLimit_LastModifiedDate_ForModifiedContacts" />
            <cm qcn="Count_NumberOfLeads" itcn="Count_NumberOfLeads" />
            <cm qcn="Count_NumberOfModifiedLeads" itcn="Count_NumberOfModifiedLeads" />
            <cm qcn="Thrttl_LowerLimit_ID_ForNewLeads" itcn="Thrttl_LowerLimit_ID_ForNewLeads" />
            <cm qcn="Thrttl_LowerLimit_CreatedDate_Lead" itcn="Thrttl_LowerLimit_CreatedDate_Lead" />
            <cm qcn="Thrttl_LowerLimit_ID_ForModifiedLeads" itcn="Thrttl_LowerLimit_ID_ForModifiedLeads" />
            <cm qcn="Thrttl_LowerLimit_LastModifiedDate_ForModifiedLeads" itcn="Thrttl_LowerLimit_LastModifiedDate_ForModifiedLeads" />
          </cms>
        </extractQuery>
      </extractQueries>
      <extractQuerySequences />
      <leafExtracts />
      <launchExtracts />
      <jobs />
      <pdmatches />
      <leadscroings />
      <lssbardins>
        <lssbardin n="PushToScoringDB_LSS_Bard_In" w="Workspace" mn="Tahoe" qn="Q_PLS_Scoring_Incremental" dp="SQL_LSSBard" eac="True">
          <cms />
        </lssbardin>
      </lssbardins>
      <lssbardouts />
      <lds />
      <ecs />
      <gCs />
    </group>'''
    lgm.setLoadGroup(step1xml)

    lgm.createLoadGroup('PushToScoringDB_Step2', 'OperationalProcess\Standard', 'PushToScoringDB_Step2', True, False)

    step2xml = ''
    if type == 'MKTO':
      step2xml = '''<extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_PushToScoringIncr" queryAlias="Q_Timestamp_PushToScoringIncr" sw="Workspace" schemaName="Timestamp_PushToScoring" at="False" ucm="True">
          <schemas />
          <specs />
          <cms>
            <cm qcn="MKTO_LeadRecord_ID" itcn="MKTO_LeadRecord_ID" />
            <cm qcn="Time_OfSubmission_PushToScoring" itcn="Time_OfSubmission_PushToScoring" />
          </cms>
        </extractQuery>
      </extractQueries>'''

    elif type == 'ELQ':
      step2xml = '''<extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_PushToScoringIncr" queryAlias="Q_Timestamp_PushToScoringIncr" sw="Workspace" schemaName="Timestamp_PushToScoring" at="False" ucm="True">
          <schemas />
          <specs />
          <cms>
            <cm qcn="ELQ_Contact_ContactID" itcn="ELQ_Contact_ContactID" />
            <cm qcn="Time_OfSubmission_PushToScoring" itcn="Time_OfSubmission_PushToScoring" />
          </cms>
        </extractQuery>
      </extractQueries>'''

    else:
      step2xml = '''<extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_PushToScoringIncr" queryAlias="Q_Timestamp_PushToScoringIncr" sw="Workspace" schemaName="Timestamp_PushToScoring" at="False" ucm="True">
          <schemas />
          <specs />
          <cms>
            <cm qcn="SFDC_Lead_Contact_ID" itcn="SFDC_Lead_Contact_ID" />
            <cm qcn="Time_OfSubmission_PushToScoring" itcn="Time_OfSubmission_PushToScoring" />
          </cms>
        </extractQuery>
      </extractQueries>'''

    lgm.setLoadGroupFunctionality('PushToScoringDB_Step2', step2xml)

    ptld = etree.fromstring(lgm.getLoadGroup('PushToScoringDB').encode('ascii', 'xmlcharrefreplace'))

    ptld.set('ng', 'True')
    lgm.setLoadGroup(etree.tostring(ptld))
    ngsxml = '<ngs><ng n="PushToScoringDB_Step1"/><ng n="PushToScoringDB_Step2"/></ngs>'
    lgm.setLoadGroupFunctionality('PushToScoringDB', ngsxml)

    success = True

    return success
