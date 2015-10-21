
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from lxml import etree
from appsequence import Applicability, StepBase

class LP_020001_RescoringDisable( StepBase ):
  
  name        = 'LP_020001_RescoringDisable'
  description = 'Disable the fix for SCNG-807'
  version     = '$Rev$'

  def __init__( self, forceApply = False ):
    super( LP_020001_RescoringDisable, self ).__init__( forceApply )


  def getApplicability( self, appseq ):

    lgm = appseq.getLoadGroupMgr()
    
    if not lgm.hasLoadGroup( 'PushToScoringDB' ):
      return Applicability.cannotApplyFail

    psdb_ngs_xml = lgm.getLoadGroupFunctionality( 'PushToScoringDB', 'ngs' )
    psdb_ngs = etree.fromstring( psdb_ngs_xml )
    for ng in psdb_ngs:
      if ng.get('n') == 'PushToScoringDB_Step1':
        return Applicability.canApply

    hasStep1 = lgm.hasLoadGroup( 'PushToScoringDB_Step1' )
    hasStep2 = lgm.hasLoadGroup( 'PushToScoringDB_Step2' )

    if hasStep1 and hasStep2:
      return Applicability.alreadyAppliedPass

    return Applicability.cannotApplyPass


  def apply( self, appseq ):
    
    lgm = appseq.getLoadGroupMgr()
    
    psdb_ngs_xml = lgm.getLoadGroupFunctionality( 'PushToScoringDB', 'ngs' )
    psdb_ngs = etree.fromstring( psdb_ngs_xml )
    for ng in psdb_ngs:
      if ng.get('n') == 'PushToScoringDB_Step1':
        psdb_ngs.remove( ng )

    lgm.setLoadGroupFunctionality( 'PushToScoringDB', etree.tostring(psdb_ngs) )

    lgm.commit()

    return True
