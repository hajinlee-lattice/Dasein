
#
# $LastChangedBy: lyan $
# $LastChangedDate: 2015-11-20 01:50:33 +0800 (Fri, 20 Nov 2015) $
# $Rev: 71049 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import liaison

class LP_020600_DL_SetMissingLeadsForFinalDailyJobs( StepBase ):
  
  name        = 'LP_020600_DL_SetMissingLeadsForFinalDailyJobs'
  description = 'Upgrade Modified Specs from 2.5.1 to 2.6.0:LP_020600_DL_SetMissingLeadsForFinalDailyJobs'
  version     = '$Rev: 71049 $'
  def __init__( self, forceApply = False ):
    super( LP_020600_DL_SetMissingLeadsForFinalDailyJobs, self ).__init__( forceApply )

  def getApplicability( self, appseq ):
    lgm = appseq.getLoadGroupMgr()
    if not lgm.hasLoadGroup('FinalDailyTasks')\
        and lgm.hasLoadGroup('Diagnostic_PreMissingLeadsReport')\
        and lgm.hasLoadGroup('Diagnostic_LoadLeads')\
        and lgm.hasLoadGroup('Diagnostic_PushToDestination'):
        return Applicability.cannotApplyPass
    return Applicability.canApply


  def apply( self, appseq ):

    success = False
    lgm = appseq.getLoadGroupMgr()
#1.Set a new LG Diagnostic_MissingLeads_DailyJobs
    lgm.createLoadGroup('Diagnostic_MissingLeads_DailyJobs', 'Diagnostic', 'Diagnostic_MissingLeads_DailyJobs', True, True)
    step1xml = ''
    step1xml = '''<ngs>
                <ng n="Diagnostic_PreMissingLeadsReport"/>
                <ng n="Diagnostic_LoadLeads"/>
                <ng n="Diagnostic_PushToDestination"/>
                </ngs>'''
    lgm.setLoadGroupFunctionality('Diagnostic_MissingLeads_DailyJobs', step1xml)

#2.Reset LG FinalDailyTasks
    lgm.createLoadGroup('FinalDailyTasks', '', 'FinalDailyTasks', True, True)
    step21xml = ''
    step2xml = '''<ngs>
                <ng n="Diagnostic_MissingLeads_DailyJobs"/>
                </ngs>'''
    lgm.setLoadGroupFunctionality('FinalDailyTasks', step2xml)
    success = True

    return success