#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase


class PM_070500_DL_UpdateLeadMetadata(StepBase):
  name = 'PM_070500_DL_UpdateLeadMetadata'
  description = 'In playmaker template, the load group "UpdateLeadMetadata" is using DanteAccount as Notion, where it should use DanteLead'
  version = '$Rev: 70934 $'

  def __init__(self, forceApply=False):
    super(PM_070500_DL_UpdateLeadMetadata, self).__init__(forceApply)

  def getApplicability(self, appseq):

    lgm = appseq.getLoadGroupMgr()
    if not lgm.hasLoadGroup('UpdateLeadMetadata'):
      return Applicability.cannotApplyPass
    return Applicability.canApply

  def apply(self, appseq):

    success = False

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText('template_type')

    if type == 'MAKER':
      pltd = lgm.getLoadGroup('UpdateLeadMetadata')
      pltd = pltd.replace('DanteAccount', 'DanteLead')
      step1 = etree.fromstring(pltd.encode('ascii', 'xmlcharrefreplace'))

      step1.set('name', 'UpdateLeadMetadata')
      step1.set('alias', 'UpdateLeadMetadata')
      lgm.setLoadGroup(etree.tostring(step1))

    success = True

    return success
