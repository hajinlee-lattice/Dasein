
#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase

class LP_020200_DL_RemoveEQInPushLeadsInDanteToDestination( StepBase ):

  name        = 'LP_020200_DL_Test'
  description = 'Remove EQ In LG : PushLeadsInDanteToDestination'
  version     = '$Rev: 70934 $'

  def __init__( self, forceApply = False ):
    super( LP_020200_DL_RemoveEQInPushLeadsInDanteToDestination, self ).__init__( forceApply )


  def getApplicability( self, appseq ):
    lgm = appseq.getLoadGroupMgr()
    if lgm.hasLoadGroup('PushLeadsInDanteToDestination'):
      return Applicability.cannotApplyPass
    return Applicability.canApply


  def apply( self, appseq ):
    
    success = False
    lgm = appseq.getLoadGroupMgr()

    pltd_eq_xml = lgm.getLoadGroupFunctionality('PushLeadsInDanteToDestination','extractQueries')
    pltd_eq = etree.fromstring( pltd_eq_xml )
    for eq in pltd_eq:
      if eq.get('queryAlias') == 'Q_Timestamp_PushToDestination':
        pltd_eq.remove( eq )

    lgm.setLoadGroupFunctionality( 'PushLeadsInDanteToDestination', etree.tostring(pltd_eq) )

    success = True

    return success

