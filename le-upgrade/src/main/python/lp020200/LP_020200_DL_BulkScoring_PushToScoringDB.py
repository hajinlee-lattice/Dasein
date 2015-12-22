
#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase

class LP_020200_DL_BulkScoring_PushToScoringDB( StepBase ):
  
  name        = 'LP_020200_DL_BulkScoring_PushToScoringDB'
  description = 'Separate into 2 LGs'
  version     = '$Rev: 70934 $'

  def __init__( self, forceApply = False ):
    super( LP_020200_DL_BulkScoring_PushToScoringDB, self ).__init__( forceApply )


  def getApplicability( self, appseq ):

    lgm = appseq.getLoadGroupMgr()
    if lgm.hasLoadGroup( 'BulkScoring_PushToScoringDB_Step1' ) and lgm.hasLoadGroup( 'BulkScoring_PushToScoringDB_Step2' ) :
      return Applicability.alreadyAppliedPass
    return Applicability.canApply


  def apply( self, appseq ):
    
    success = False

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText( 'template_type' )

    lgm.createLoadGroup( 'BulkScoring_PushToScoringDB_Step1', 'BulkScoring\Standard', 'BulkScoring_PushToScoringDB_Step1', True, False )
    step1xml = ''
    if type == 'MKTO':
      step1xml = '''
    <group name="BulkScoring_PushToScoringDB_Step1" alias="BulkScoring_PushToScoringDB_Step1" w="Workspace" type="1" scheduleType="0" allowUserChangeScheduleType="False" visibleForEndUser="True" threshold="10000" launchExpiredDays="7" createdBy="VZhao@Lattice-Engines.com" path="BulkScoring\Standard" autoGenerated="False" validationValidMinutes="120" autoClearOnFailure="False" mergeRulesSaved="True" ng="False">
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
        <lssbardin n="PushToScoringDB_LSS_Bard_In" w="Workspace" mn="Tahoe" qn="Q_PLS_Scoring_Bulk" dp="SQL_LSSBard" eac="True">
          <cms />
        </lssbardin>
      </lssbardins>
      <lssbardouts />
      <lds />
      <ecs />
      <gCs />
    </group>'''

    elif type =='ELQ':
      step1xml = '''
    <group name="BulkScoring_PushToScoringDB_Step1" alias="BulkScoring_PushToScoringDB_Step1" w="Workspace" type="1" scheduleType="0" allowUserChangeScheduleType="False" visibleForEndUser="True" threshold="10000" launchExpiredDays="7" createdBy="VZhao@Lattice-Engines.com" path="BulkScoring\Standard" autoGenerated="False" validationValidMinutes="120" autoClearOnFailure="False" mergeRulesSaved="True" ng="False">
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
        <lssbardin n="PushToScoringDB_LSS_Bard_In" w="Workspace" mn="Tahoe" qn="Q_PLS_Scoring_Bulk" dp="SQL_LSSBard" eac="True">
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
    <group name="BulkScoring_PushToScoringDB_Step1" alias="BulkScoring_PushToScoringDB_Step1" w="Workspace" type="1" scheduleType="0" allowUserChangeScheduleType="False" visibleForEndUser="True" threshold="10000" launchExpiredDays="7" createdBy="VZhao@Lattice-Engines.com" path="BulkScoring\Standard" autoGenerated="False" validationValidMinutes="120" autoClearOnFailure="False" mergeRulesSaved="True" ng="False">
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
        <lssbardin n="PushToScoringDB_LSS_Bard_In" w="Workspace" mn="Tahoe" qn="Q_PLS_Scoring_Bulk" dp="SQL_LSSBard" eac="True">
          <cms />
        </lssbardin>
      </lssbardins>
      <lssbardouts />
      <lds />
      <ecs />
      <gCs />
    </group>'''
    lgm.setLoadGroup(step1xml)

    lgm.createLoadGroup( 'BulkScoring_PushToScoringDB_Step2', 'BulkScoring\Standard', 'BulkScoring_PushToScoringDB_Step2', True, False )

    step2xml = ''
    if type == 'MKTO':
      step2xml = '''<extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_PushToScoringBulk" queryAlias="Q_Timestamp_PushToScoringBulk" sw="Workspace" schemaName="Timestamp_PushToScoring" at="False" ucm="True">
          <schemas />
          <specs />
          <cms>
            <cm qcn="MKTO_LeadRecord_ID" itcn="MKTO_LeadRecord_ID" />
            <cm qcn="Time_OfSubmission_PushToScoring" itcn="Time_OfSubmission_PushToScoring" />
          </cms>
        </extractQuery>
      </extractQueries>'''

    elif type =='ELQ':
      step2xml = '''<extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Timestamp_PushToScoringBulk" queryAlias="Q_Timestamp_PushToScoringBulk" sw="Workspace" schemaName="Timestamp_PushToScoring" at="False" ucm="True">
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
        <extractQuery qw="Workspace" queryName="Q_Timestamp_PushToScoringBulk" queryAlias="Q_Timestamp_PushToScoringBulk" sw="Workspace" schemaName="Timestamp_PushToScoring" at="False" ucm="True">
          <schemas />
          <specs />
          <cms>
            <cm qcn="SFDC_Lead_Contact_ID" itcn="SFDC_Lead_Contact_ID" />
            <cm qcn="Time_OfSubmission_PushToScoring" itcn="Time_OfSubmission_PushToScoring" />
          </cms>
        </extractQuery>
      </extractQueries>'''

    lgm.setLoadGroupFunctionality( 'BulkScoring_PushToScoringDB_Step2', step2xml )

    ptld  = etree.fromstring( lgm.getLoadGroup('BulkScoring_PushToScoringDB').encode('ascii', 'xmlcharrefreplace') )

    ptld.set( 'ng', 'True' )
    lgm.setLoadGroup( etree.tostring(ptld) )
    ngsxml = '<ngs><ng n="BulkScoring_PushToScoringDB_Step1"/><ng n="BulkScoring_PushToScoringDB_Step2"/></ngs>'
    lgm.setLoadGroupFunctionality( 'BulkScoring_PushToScoringDB', ngsxml )

    success = True

    return success

