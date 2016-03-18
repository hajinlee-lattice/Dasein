#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import appsequence
import liaison


class LP_020400_LG_UploadLeadFile(StepBase):
  name = 'LP_020400_LG_UploadLeadFile'
  description = 'Set an New LG BulkScoring_PushToScoringDB_UserLeadFile'
  version = '$Rev: 70934 $'

  def __init__(self, forceApply=False):
    super(LP_020400_LG_UploadLeadFile, self).__init__(forceApply)

  def getApplicability(self, appseq):

    lgm = appseq.getLoadGroupMgr()
    conn_mgr = appseq.getConnectionMgr()

    if lgm.hasLoadGroup('BulkScoring_PushToScoringDB_UserLeadFile'):
      bsptulf_xml = lgm.getLoadGroupFunctionality( 'BulkScoring_PushToScoringDB_UserLeadFile', 'ngs' )
      hasModPTLD = bsptulf_xml != '<ngs />'
      hasStep1 = lgm.hasLoadGroup( 'BulkScoring_PushToScoringDB_UserLeadFile_Step1' )
      hasStep2 = lgm.hasLoadGroup( 'BulkScoring_PushToScoringDB_UserLeadFile_Step2' )
      if hasModPTLD and hasStep1 and hasStep2:
        return Applicability.alreadyAppliedPass
      else:
        return Applicability.canApply
    return Applicability.canApply

  def apply(self, appseq):

    success = False
    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText( 'template_type' )
    bsptulf_xml_step1 = ''
    bsptulf_xml_step2 = ''

    lgm.createLoadGroup('BulkScoring_PushToScoringDB_UserLeadFile_Step1', 'BulkScoring\Standard',
                        'BulkScoring_PushToScoringDB_UserLeadFile_Step1', True, False)
    bsptulf_xml_step1= etree.fromstring(lgm.getLoadGroup('BulkScoring_PushToScoringDB_UserLeadFile_Step1').encode('ascii', 'xmlcharrefreplace'))

    lgm.createLoadGroup('BulkScoring_PushToScoringDB_UserLeadFile_Step2', 'BulkScoring\Standard',
                        'BulkScoring_PushToScoringDB_UserLeadFile_Step2', True, False)
    bsptulf_xml_step1= etree.fromstring(lgm.getLoadGroup('BulkScoring_PushToScoringDB_UserLeadFile_Step2').encode('ascii', 'xmlcharrefreplace'))

    if type == 'MKTO':
      bsptulf_xml_step1 = '''
      <group name="BulkScoring_PushToScoringDB_UserLeadFile_Step1" alias="BulkScoring_PushToScoringDB_UserLeadFile_Step1" w="Workspace" type="1" scheduleType="0" allowUserChangeScheduleType="False" visibleForEndUser="True" threshold="10000" launchExpiredDays="7" createdBy="YTian@Lattice-Engines.com" path="BulkScoring\Standard" autoGenerated="False" validationValidMinutes="120" autoClearOnFailure="False" mergeRulesSaved="True" ng="False">
      <schemas/>
      <visiDBConfigurationWithMacros/>
      <targetQueries/>
      <targetQuerySequences/>
      <gvqs/>
      <rdss/>
      <validationExtracts/>
      <ces/>
      <extractQueries/>
      <extractQuerySequences/>
      <leafExtracts/>
      <launchExtracts/>
      <jobs/>
      <pdmatches/>
      <leadscroings/>
      <lssbardins>
        <lssbardin n="PushToScoringDB_UserLeadFile_LSS_Bard_In" w="Workspace" mn="Tahoe" qn="Q_PLS_Scoring_Bulk_LeadFile" dp="SQL_LSSBard" eac="True">
          <cms/>
        </lssbardin>
      </lssbardins>
      <lssbardouts/>
      <lds/>
      <ecs/>
      <gCs/>
    </group>'''

      bsptulf_xml_step2 = '''
      <group name="BulkScoring_PushToScoringDB_UserLeadFile_Step2" alias="BulkScoring_PushToScoringDB_UserLeadFile_Step2" w="Workspace" type="1" scheduleType="0" allowUserChangeScheduleType="False" visibleForEndUser="True" threshold="10000" launchExpiredDays="7" createdBy="YTian@Lattice-Engines.com" path="BulkScoring\Standard" autoGenerated="False" validationValidMinutes="120" autoClearOnFailure="False" mergeRulesSaved="True" ng="False">
      <schemas/>
      <visiDBConfigurationWithMacros/>
      <targetQueries/>
      <targetQuerySequences/>
      <gvqs/>
      <rdss/>
      <validationExtracts/>
      <ces/>
      <extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_PushToScoring_Bulk_LeadFile" queryAlias="Q_Timestamp_PushToScoring_Bulk_LeadFile" sw="Workspace" schemaName="Timestamp_PushToScoring" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms/>
        </extractQuery>
      </extractQueries>
      <extractQuerySequences/>
      <leafExtracts/>
      <launchExtracts/>
      <jobs/>
      <pdmatches/>
      <leadscroings/>
      <lssbardins/>
      <lssbardouts/>
      <lds/>
      <ecs/>
      <gCs/>
    </group>'''
    elif type =='ELQ':
      bsptulf_xml_step1 = '''
      <group name="BulkScoring_PushToScoringDB_UserLeadFile_Step1" alias="BulkScoring_PushToScoringDB_UserLeadFile_Step1" w="Workspace" type="1" scheduleType="0" allowUserChangeScheduleType="False" visibleForEndUser="True" threshold="10000" launchExpiredDays="7" createdBy="YTian@Lattice-Engines.com" path="BulkScoring\Standard" autoGenerated="False" validationValidMinutes="120" autoClearOnFailure="False" mergeRulesSaved="True" ng="False">
      <schemas/>
      <visiDBConfigurationWithMacros/>
      <targetQueries/>
      <targetQuerySequences/>
      <gvqs/>
      <rdss/>
      <validationExtracts/>
      <ces/>
      <extractQueries/>
      <extractQuerySequences/>
      <leafExtracts/>
      <launchExtracts/>
      <jobs/>
      <pdmatches/>
      <leadscroings/>
      <lssbardins>
        <lssbardin n="PushToScoringDB_UserLeadFile_LSS_Bard_In" w="Workspace" mn="Tahoe" qn="Q_PLS_Scoring_Bulk_LeadFile" dp="SQL_LSSBard" eac="True">
          <cms/>
        </lssbardin>
      </lssbardins>
      <lssbardouts/>
      <lds/>
      <ecs/>
      <gCs/>
    </group>'''
      bsptulf_xml_step2 = '''
      <group name="BulkScoring_PushToScoringDB_UserLeadFile_Step2" alias="BulkScoring_PushToScoringDB_UserLeadFile_Step2" w="Workspace" type="1" scheduleType="0" allowUserChangeScheduleType="False" visibleForEndUser="True" threshold="10000" launchExpiredDays="7" createdBy="YTian@Lattice-Engines.com" path="BulkScoring\Standard" autoGenerated="False" validationValidMinutes="120" autoClearOnFailure="False" mergeRulesSaved="True" ng="False">
      <schemas/>
      <visiDBConfigurationWithMacros/>
      <targetQueries/>
      <targetQuerySequences/>
      <gvqs/>
      <rdss/>
      <validationExtracts/>
      <ces/>
      <extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_PushToScoring_Bulk_LeadFile" queryAlias="Q_Timestamp_PushToScoring_Bulk_LeadFile" sw="Workspace" schemaName="Timestamp_PushToScoring" at="False" ucm="True">
          <schemas/>
          <specs/>
          <cms/>
        </extractQuery>
      </extractQueries>
      <extractQuerySequences/>
      <leafExtracts/>
      <launchExtracts/>
      <jobs/>
      <pdmatches/>
      <leadscroings/>
      <lssbardins/>
      <lssbardouts/>
      <lds/>
      <ecs/>
      <gCs/>
    </group>'''
    else:
      bsptulf_xml_step1 = '''
      <group name="BulkScoring_PushToScoringDB_UserLeadFile_Step1" alias="BulkScoring_PushToScoringDB_UserLeadFile_Step1" w="Workspace" type="1" scheduleType="0" allowUserChangeScheduleType="False" visibleForEndUser="True" threshold="10000" launchExpiredDays="7" createdBy="YTian@Lattice-Engines.com" path="BulkScoring\Standard" autoGenerated="False" validationValidMinutes="120" autoClearOnFailure="False" mergeRulesSaved="True" ng="False">
      <schemas/>
      <visiDBConfigurationWithMacros/>
      <targetQueries/>
      <targetQuerySequences/>
      <gvqs/>
      <rdss/>
      <validationExtracts/>
      <ces/>
      <extractQueries/>
      <extractQuerySequences/>
      <leafExtracts/>
      <launchExtracts/>
      <jobs/>
      <pdmatches/>
      <leadscroings/>
      <lssbardins>
        <lssbardin n="PushToScoringDB_UserLeadFile_LSS_Bard_In" w="Workspace" mn="Tahoe" qn="Q_PLS_Scoring_Bulk_LeadFile" dp="SQL_LSSBard" eac="True">
          <cms/>
        </lssbardin>
      </lssbardins>
      <lssbardouts/>
      <lds/>
      <ecs/>
      <gCs/>
    </group>'''
      bsptulf_xml_step2 = '''
      <group name="BulkScoring_PushToScoringDB_UserLeadFile_Step2" alias="BulkScoring_PushToScoringDB_UserLeadFile_Step2" w="Workspace" type="1" scheduleType="0" allowUserChangeScheduleType="False" visibleForEndUser="True" threshold="10000" launchExpiredDays="7" createdBy="YTian@Lattice-Engines.com" path="BulkScoring\Standard" autoGenerated="False" validationValidMinutes="120" autoClearOnFailure="False" mergeRulesSaved="True" ng="False">
      <schemas/>
      <visiDBConfigurationWithMacros/>
      <targetQueries/>
      <targetQuerySequences/>
      <gvqs/>
      <rdss/>
      <validationExtracts/>
      <ces/>
      <extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_PushToScoring_Bulk_LeadFile" queryAlias="Q_Timestamp_PushToScoring_Bulk_LeadFile" sw="Workspace" schemaName="Timestamp_PushToScoring" at="False" ucm="False">
          <schemas/>
          <specs/>
          <cms/>
        </extractQuery>
      </extractQueries>
      <extractQuerySequences/>
      <leafExtracts/>
      <launchExtracts/>
      <jobs/>
      <pdmatches/>
      <leadscroings/>
      <lssbardins/>
      <lssbardouts/>
      <lds/>
      <ecs/>
      <gCs/>
    </group>'''
    lgm.setLoadGroup(bsptulf_xml_step1)
    lgm.setLoadGroup(bsptulf_xml_step2)

    lgm.createLoadGroup('BulkScoring_PushToScoringDB_UserLeadFile', 'BulkScoring\Standard', 'BulkScoring_PushToScoringDB_UserLeadFile', True, True)
    bsptulf = etree.fromstring(lgm.getLoadGroup('BulkScoring_PushToScoringDB_UserLeadFile').encode('ascii', 'xmlcharrefreplace'))
    lgm.setLoadGroup(etree.tostring(bsptulf))
    ngsxml_bsptulf = '<ngs><ng n="BulkScoring_PushToScoringDB_UserLeadFile_Step1"/><ng n="BulkScoring_PushToScoringDB_UserLeadFile_Step2"/></ngs>'
    lgm.setLoadGroupFunctionality('BulkScoring_PushToScoringDB_UserLeadFile', ngsxml_bsptulf)

    success = True

    return success
