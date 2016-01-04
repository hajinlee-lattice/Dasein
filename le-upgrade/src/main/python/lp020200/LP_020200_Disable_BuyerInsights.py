#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import re


class LP_020200_Disable_BuyerInsights(StepBase):
  name = 'LP_020200_Disable_BuyerInsights'
  description = 'Disable the buyer insights feature'
  version = '$Rev: 71901 $'

  def __init__(self, forceApply=False):
    super(LP_020200_Disable_BuyerInsights, self).__init__(forceApply)

  def getApplicability(self, appseq):

    lgm = appseq.getLoadGroupMgr()
    if not (lgm.hasLoadGroup('PushDataToDante_Hourly') or lgm.hasLoadGroup(
      'PushLeadsLastScoredToDestination ') or lgm.hasLoadGroup('PushToLeadDestination_TimeStamp ') or lgm.hasLoadGroup(
      'LoadScoredLeads_Step1') or lgm.hasLoadGroup('LoadScoredLeads_Step2') or lgm.hasLoadGroup(
      'PushToLeadDestination_Validation') or lgm.hasLoadGroup('InsightsAllSteps')):
      return Applicability.cannotApplyPass
    return Applicability.canApply

  def apply(self, appseq):

    success = False

    lgm = appseq.getLoadGroupMgr()

    # Configure the nested load groups under FinalDailyTasks
    fdt_ngs_xml = lgm.getLoadGroupFunctionality('FinalDailyTasks', "ngs")
    fdt_ngs = etree.fromstring(fdt_ngs_xml)
    # Remove PushDataToDante_Hourly/InsightsAllSteps from FinalDailyTasks anyway.
    for ng in fdt_ngs:
      if ng.get('n') in ('PushDataToDante_Hourly', 'InsightsAllSteps'):
        fdt_ngs.remove(ng)

    lgm.setLoadGroupFunctionality('FinalDailyTasks', etree.tostring(fdt_ngs))

    # Configure the nested load groups under pushToLeadDestination
    ptld = etree.fromstring(lgm.getLoadGroup('PushToLeadDestination').encode('ascii', 'xmlcharrefreplace'))

    ptld.set('ng', 'True')
    lgm.setLoadGroup(etree.tostring(ptld))
    ngsxml = '<ngs><ng n="LoadScoredLeads_Step1"/><ng n="LoadScoredLeads_Step2"/><ng n="PushLeadsLastScoredToDestination"/><ng n="PushToLeadDestination_TimeStamp"/><ng n="PushToLeadDestination_Validation"/></ngs>'
    lgm.setLoadGroupFunctionality('PushToLeadDestination', ngsxml)

    success = True

    return success
