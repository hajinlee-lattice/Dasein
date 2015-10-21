
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from lxml import etree
from appsequence import Applicability, StepBase

class LP_020001_Rescoring( StepBase ):
  
  name        = 'LP_020001_Rescoring'
  description = 'Prevent Scoring Leads Multiple Times (SCNG-807)'
  version     = '$Rev$'

  def __init__( self, forceApply = False ):
    super( LP_020001_Rescoring, self ).__init__( forceApply )


  def getApplicability( self, appseq ):

    lgm = appseq.getLoadGroupMgr()
    
    if not lgm.hasLoadGroup( 'PushToScoringDB' ):
      return Applicability.cannotApplyFail

    psdbxml = lgm.getLoadGroupFunctionality( 'PushToScoringDB', 'ngs' )
    hasModPSDB = psdbxml != '<ngs />'
    hasStep1 = lgm.hasLoadGroup( 'PushToScoringDB_Step1' )
    hasStep2 = lgm.hasLoadGroup( 'PushToScoringDB_Step2' )

    if not hasModPSDB and not hasStep1 and not hasStep2:
      return Applicability.canApply
    elif hasModPSDB and hasStep1 and hasStep2:
      return Applicability.alreadyAppliedPass

    return Applicability.cannotApplyPass


  def apply( self, appseq ):
    
    success = False

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText( 'template_type' )

    lgm.createLoadGroup( 'PushToScoringDB_Step1', 'OperationalProcess\Standard', 'PushToScoringDB_Step1', True, False )
    step1xml = ''
    if type == 'MKTO':
      step1xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PreScoringIncr" queryAlias="Q_Timestamp_PreScoringIncr" sw="Workspace" schemaName="Timestamp_PushToScoring" at="False" ucm="True"><schemas/><specs/><cms><cm qcn="MKTO_LeadRecord_ID" itcn="MKTO_LeadRecord_ID"/><cm qcn="Time_OfSubmission_PushToScoring" itcn="Time_OfSubmission_PushToScoring"/></cms></extractQuery></extractQueries>'
    elif type =='ELQ':
      step1xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PreScoringIncr" queryAlias="Q_Timestamp_PreScoringIncr" sw="Workspace" schemaName="Timestamp_PushToScoring" at="False" ucm="True"><schemas/><specs/><cms><cm qcn="ELQ_Contact_ContactID" itcn="ELQ_Contact_ContactID"/><cm qcn="Time_OfSubmission_PushToScoring" itcn="Time_OfSubmission_PushToScoring"/></cms></extractQuery></extractQueries>'
    else:
      step1xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_PreScoringIncr" queryAlias="Q_Timestamp_PreScoringIncr" sw="Workspace" schemaName="Timestamp_PushToScoring" at="False" ucm="True"><schemas/><specs/><cms><cm qcn="SFDC_Lead_Contact_ID" itcn="SFDC_Lead_Contact_ID"/><cm qcn="Time_OfSubmission_PushToScoring" itcn="Time_OfSubmission_PushToScoring"/></cms></extractQuery></extractQueries>'
    lgm.setLoadGroupFunctionality( 'PushToScoringDB_Step1', step1xml )

    psdb  = etree.fromstring( lgm.getLoadGroup('PushToScoringDB').encode('ascii', 'xmlcharrefreplace') )
    step2 = etree.fromstring( lgm.getLoadGroup('PushToScoringDB').encode('ascii', 'xmlcharrefreplace') )

    step2.set( 'name', 'PushToScoringDB_Step2' )
    step2.set( 'alias', 'PushToScoringDB_Step2' )
    lgm.setLoadGroup( etree.tostring(step2) )

    psdb.set( 'ng', 'True' )
    lgm.setLoadGroup( etree.tostring(psdb) )
    ngsxml = '<ngs><ng n="PushToScoringDB_Step1"/><ng n="PushToScoringDB_Step2"/></ngs>'
    lgm.setLoadGroupFunctionality( 'PushToScoringDB', ngsxml )

    lgm.commit()

    success = True

    return success

