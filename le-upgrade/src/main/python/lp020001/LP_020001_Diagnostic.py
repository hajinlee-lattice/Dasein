
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from lxml import etree
from appsequence import Applicability, StepBase

class LP_020001_Diagnostic( StepBase ):
  
  name        = 'LP_020001_Diagnostic'
  description = 'Diagnostic Queries Download Too Much Data'
  version     = '$Rev$'

  def __init__( self, forceApply = False ):
    super( LP_020001_Diagnostic, self ).__init__( forceApply )


  def getApplicability( self, appseq ):

    lgm = appseq.getLoadGroupMgr()
    
    if not lgm.hasLoadGroup( 'FinalDailyTasks' ):
      return Applicability.cannotApplyPass

    fdt_ngs_xml = lgm.getLoadGroupFunctionality( 'FinalDailyTasks', 'ngs' )
    fdt_ngs = etree.fromstring( fdt_ngs_xml )
    for ng in fdt_ngs:
      if ng.get('n') == 'Diagnostic_LoadLeads' or ng.get('n') == 'Diagnostic_PushToDestination':
        return Applicability.canApply

    return Applicability.cannotApplyPass


  def apply( self, appseq ):

    lgm = appseq.getLoadGroupMgr()
    
    fdt_ngs_xml = lgm.getLoadGroupFunctionality( 'FinalDailyTasks', 'ngs' )
    fdt_ngs = etree.fromstring( fdt_ngs_xml )
    for ng in fdt_ngs:
      if ng.get('n') == 'Diagnostic_LoadLeads' or ng.get('n') == 'Diagnostic_PushToDestination':
        fdt_ngs.remove( ng )

    lgm.setLoadGroupFunctionality( 'FinalDailyTasks', etree.tostring(fdt_ngs) )

    lgm.commit()

    return True
