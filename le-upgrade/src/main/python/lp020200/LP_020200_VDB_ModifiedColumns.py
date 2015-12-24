#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import appsequence
import liaison


class LP_020200_VDB_ModifiedColumns(StepBase):
  name = 'LP_020200_VDB_ModifiedColumns'
  description = 'Upgrade Modified Specs from 2.1.2 to 2.2.0'
  version = '$Rev: 70934 $'

  def __init__(self, forceApply=False):
    super(LP_020200_VDB_ModifiedColumns, self).__init__(forceApply)

  def getApplicability(self, appseq):

    conn_mgr = appseq.getConnectionMgr()
    conn_mgr.getQuery("Q_Diagnostic_DownloadedUnscoredLeads")
    conn_mgr.getQuery("Q_Diagnostic_MissingDownloadedLeads")

    if not conn_mgr.getQuery("Q_Diagnostic_DownloadedUnscoredLeads") and not conn_mgr.getQuery(
      "Q_Diagnostic_MissingDownloadedLeads"):
      return Applicability.cannotApplyPass

    return Applicability.canApply

  def apply(self, appseq):

    success = False

    conn_mgr = appseq.getConnectionMgr()
    type = appseq.getText('template_type')

    if type in ('MKTO', 'ELQ', 'SFDC'):
      # Modify the Column in Q_Diagnostic_DownloadedUnscoredLeads
      query1 = conn_mgr.getQuery("Q_Diagnostic_DownloadedUnscoredLeads")
      exp1_1 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("SFDC_Id"))')
      col1_1 = liaison.QueryColumnVDBImpl('SFDC_Id', exp1_1)

      query1.appendColumn(col1_1)
      conn_mgr.setQuery(query1)

      # Modify the Column in Q_Diagnostic_MissingDownloadedLeads
      query2 = conn_mgr.getQuery("Q_Diagnostic_MissingDownloadedLeads")
      exp2_1 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("SFDC_Id"))')
      col2_1 = liaison.QueryColumnVDBImpl('SFDC_Id', exp2_1)

      query2.appendColumn(col2_1)
      conn_mgr.setQuery(query2)

    success = True

    return success
