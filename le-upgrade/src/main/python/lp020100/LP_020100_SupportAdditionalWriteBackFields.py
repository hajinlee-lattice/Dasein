#
# $LastChangedBy: lyan $
# $LastChangedDate: 2015-11-20 01:50:33 +0800 (Fri, 20 Nov 2015) $
# $Rev: 71049 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import re


class LP_020100_SupportAdditionalWriteBackFields(StepBase):
  name = 'LP_020100_SupportAdditionalWriteBackFields'
  description = 'Support the additional write-back fields.'
  version = '$Rev: 71049 $'


  def __init__(self, forceApply=False):
    super(LP_020100_SupportAdditionalWriteBackFields, self).__init__(forceApply)

  def getApplicability(self, appseq):

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText('template_type')

    print "Type:" + type

    if type in ("ELQ", "MKTO"):

      if not lgm.hasLoadGroup('PushToLeadDestination_Step1'):
        return Applicability.cannotApplyFail

      ptldxml = lgm.getLoadGroupFunctionality('PushToLeadDestination_Step1', 'lssbardouts')
      psdb  = etree.fromstring( ptldxml.encode('ascii', 'xmlcharrefreplace') )
      cmLength = len(psdb.findall('./lssbardout/cms/cm'))
      if (cmLength > 3):
        print "Additional write back fields"
        return Applicability.canApply
    elif type == 'SFDC':
      if not lgm.hasLoadGroup('PushToLeadDestination_Step2'):
        return Applicability.cannotApplyFail
      ptldxml = lgm.getLoadGroupFunctionality('PushToLeadDestination_Step2', 'targetQueries')
      psdb  = etree.fromstring( ptldxml.encode('ascii', 'xmlcharrefreplace') )
      cmLength = len(psdb.findall("./targetQuery/fsColumnMappings/fsColumnMapping"))
      if (cmLength > 6):
        print "Additional write back fields"
        return Applicability.canApply
    return Applicability.cannotApplyFail

  def apply(self, appseq):
    #Just add the validation function here, the apply function need to be added, too.
    success = False

    return True
