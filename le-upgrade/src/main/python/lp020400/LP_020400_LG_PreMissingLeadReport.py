#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import appsequence
import liaison


class LP_020400_LG_PreMissingLeadReport(StepBase):
  name = 'LP_020400_LG_PreMissingLeadReport'
  description = 'Set an New LG LP_020400_LG_MissingLeadReport'
  version = '$Rev: 70934 $'

  def __init__(self, forceApply=False):
    super(LP_020400_LG_PreMissingLeadReport, self).__init__(forceApply)

  def getApplicability(self, appseq):

    lgm = appseq.getLoadGroupMgr()
    conn_mgr = appseq.getConnectionMgr()

    if lgm.hasLoadGroup('Diagnostic_PreMissingLeadsReport'):
      return Applicability.alreadyAppliedPass
    return Applicability.canApply

  def apply(self, appseq):

    success = False
    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText( 'template_type' )
    customerId = appseq.getText('customer_id')
    mlr_xml = ''

    lgm.createLoadGroup('Diagnostic_PreMissingLeadsReport', 'Diagnostic',
                        'Diagnostic_PreMissingLeadsReport', True, False)
    mlr_xml= etree.fromstring(lgm.getLoadGroup('Diagnostic_PreMissingLeadsReport').encode('ascii', 'xmlcharrefreplace'))
    if type == 'MKTO':
      mlr_xml = '''<ecs>
        <ec n="Excute_Command_PreMissingLeadsReport" s="10.51.1.131" c="C:\Python27\Python D:\MissingLeadReport\le-upgrade\src\main\python\LPMissingLeadReport.py --missingLead __CUSTOMER_NAME__ MissingResults.csv" e="True"/>
      </ecs>'''
    elif type =='ELQ':
      mlr_xml = '''<ecs>
        <ec n="Excute_Command_PreMissingLeadsReport" s="10.51.1.131" c="C:\Python27\Python D:\MissingLeadReport\le-upgrade\src\main\python\LPMissingLeadReport.py --missingLead __CUSTOMER_NAME__  MissingResults.csv" e="True"/>
      </ecs>'''
    else:
      mlr_xml = '''<ecs>
        <ec n="Excute_Command_PreMissingLeadsReport" s="10.51.1.131" c="C:\Python27\Python D:\MissingLeadReport\le-upgrade\src\main\python\LPMissingLeadReport.py --missingLead __CUSTOMER_NAME__  MissingResults.csv" e="True"/>
      </ecs>'''
    mlr_xml = mlr_xml.replace('__CUSTOMER_NAME__', customerId)
    lgm.setLoadGroupFunctionality('Diagnostic_PreMissingLeadsReport', mlr_xml)
    #lgm.setLoadGroup(mlr_xml)

    success = True

    return success
