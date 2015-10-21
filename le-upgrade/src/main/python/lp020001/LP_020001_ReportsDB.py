
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from lxml import etree
from appsequence import Applicability, StepBase

class LP_020001_ReportsDB( StepBase ):
  
  name        = 'LP_020001_ReportsDB'
  description = 'Cannot Write Profile_SummaryCounts to Reports DB'
  version     = '$Rev$'

  def __init__( self, forceApply = False ):
    super( LP_020001_ReportsDB, self ).__init__( forceApply )


  def getApplicability( self, appseq ):
    
    lgm = appseq.getLoadGroupMgr()
    
    if not lgm.hasLoadGroup( 'PushToReportsDB' ):
      return Applicability.cannotApplyPass

    prdb_tqs_xml = lgm.getLoadGroupFunctionality( 'PushToReportsDB', 'targetQueries' )
    prdb_tqs = etree.fromstring( prdb_tqs_xml )
    for tq in prdb_tqs:
      if tq.get('name') == 'Profile_SummaryCounts':
        return Applicability.canApply

    return Applicability.cannotApplyPass


  def apply( self, appseq ):

    lgm = appseq.getLoadGroupMgr()

    prdb_tqs_xml = lgm.getLoadGroupFunctionality( 'PushToReportsDB', 'targetQueries' )
    prdb_tqss_xml = lgm.getLoadGroupFunctionality( 'PushToReportsDB', 'targetQuerySequences' )
    prdb_tqs = etree.fromstring( prdb_tqs_xml )
    prdb_tqss = etree.fromstring( prdb_tqss_xml )
    
    for tq in prdb_tqs:
      if tq.get('name') == 'Profile_SummaryCounts':
        prdb_tqs.remove( tq )
        break
    
    for seq in prdb_tqss:
      if seq.get('queryName') == 'Profile_SummaryCounts':
        prdb_tqss.remove( seq )
        break

    lgm.setLoadGroupFunctionality( 'PushToReportsDB', etree.tostring(prdb_tqs) )
    lgm.setLoadGroupFunctionality( 'PushToReportsDB', etree.tostring(prdb_tqss) )

    lgm.commit()

    return True
