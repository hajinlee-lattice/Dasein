#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import re


class LP_020200_DL_BulkDiagnostic(StepBase):
  name = 'LP_020200_DL_BulkDiagnostic'
  description = 'Enable to run BulkDiagnostic'
  version = '$Rev: 70934 $'

  def __init__(self, forceApply=False):
    super(LP_020200_DL_BulkDiagnostic, self).__init__(forceApply)

  def getApplicability(self, appseq):

    lgm = appseq.getLoadGroupMgr()
    if not lgm.hasLoadGroup('Diagnostic_PushToDestination') and not lgm.hasLoadGroup('Diagnostic_LoadLeads'):
      return Applicability.cannotApplyPass
    return Applicability.canApply

  def apply(self, appseq):

    success = False

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText('template_type')
    scoreField = appseq.getText('score_field')
    scoreDateField = appseq.getText('score_date_field')
    sfdcLeadScoreField = appseq.getText('sfdc_lead_score_field')
    sfdcContactScoreField = appseq.getText('sfdc_contact_score_field')
    sfdcLeadScoreDateField = appseq.getText('sfdc_lead_score_date_field')
    sfdcContactScoreDateField = appseq.getText('sfdc_contact_score_date_field')

    step1 = etree.fromstring(lgm.getLoadGroup('Diagnostic_LoadLeads').encode('ascii', 'xmlcharrefreplace'))

    step1.set('name', 'Diagnostic_LoadLeads_Bulk')
    step1.set('alias', 'Diagnostic_LoadLeads_Bulk')

    lgm.setLoadGroup(etree.tostring(step1))

    diag_rdss_xml = lgm.getLoadGroupFunctionality('Diagnostic_LoadLeads_Bulk', "rdss")

    if type == 'MKTO':

      diag_rdss_xml = re.sub(r' f=\"@.*?\"', ' f="@datediff(now, month(6)) AND&#xD;&#xA;activityType in (\'New Lead\','
                                             '\'Click Link\',\'Visit Webpage\',\'Interesting Moment\',\'Open Email\','
                                             '\'Email Bounced Soft\',\'Fill Out Form\',\'Unsubscribe Email\','
                                             '\'Click Email\')"',
                             diag_rdss_xml)
    elif type == 'ELQ':
      diag_rdss_xml = re.sub(r' f=\"@.*?\"',
                             ' f="@datediff(now, month(6)) AND (__SCORE_DATE_FIELD__ &lt; '
                             '#Diagnostic_RescoreThreshold OR __SCORE_FIELD__ = 0 )"',
                             diag_rdss_xml)
      diag_rdss_xml.replace('__SCORE_DATE_FIELD__', scoreDateField)
      diag_rdss_xml.replace('__SCORE_FIELD__', scoreField)

    elif type == 'SFDC':
      diag_rdss_xml = re.sub(r'<rds n=\"SFDC_Contact_Validation\".*f=\"@.*?\"',
                             ' f="@datediff(now, month(6)) AND ( __CONTACT_SCORE_DATE_FIELD__ &lt; '
                             '#Diagnostic_RescoreThreshold_Contact OR __CONTACT_SCORE_FIELD__ = null )"',
                             diag_rdss_xml)
      diag_rdss_xml.replace('__CONTACT_SCORE_DATE_FIELD__', sfdcContactScoreDateField)
      diag_rdss_xml.replace('__CONTACT_SCORE_FIELD__', sfdcContactScoreField)
      diag_rdss_xml = re.sub(r'<rds n=\"SFDC_Lead_Validation\".*f=\"@.*?\"',
                             ' f="@datediff(now, month(6)) AND ( __LEAD_SCORE_DATE_FIELD__ &lt; '
                             '#Diagnostic_RescoreThreshold_Contact OR __LEAD_SCORE_FIELD__ = null )"',
                             diag_rdss_xml)
      diag_rdss_xml.replace('__LEAD_SCORE_DATE_FIELD__', sfdcLeadScoreDateField)
      diag_rdss_xml.replace('__LEAD_SCORE_FIELD__', sfdcLeadScoreField)

    lgm.setLoadGroupFunctionality('Diagnostic_LoadLeads_Bulk', diag_rdss_xml)

    lgm.createLoadGroup('Bulk_Diagnostic', 'Diagnostic', 'Bulk_Diagnostic', True, True)

    ngsxml = '<ngs><ng n="Diagnostic_LoadLeads_Bulk"/><ng n="Diagnostic_PushToDestination"/></ngs>'
    lgm.setLoadGroupFunctionality('Bulk_Diagnostic', ngsxml)

    success = True

    return success
