
#
# $LastChangedBy: lyan $
# $LastChangedDate: 2015-11-20 01:50:33 +0800 (Fri, 20 Nov 2015) $
# $Rev: 71049 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import liaison

class LP_020600_VDB_EliminateUnusedColumnsinMKTO_LeadRecord( StepBase ):
  
  name        = 'LP_020600_VDB_EliminateUnusedColumnsinMKTO_LeadRecord'
  description = 'Upgrade Modified Specs from 2.5.1 to 2.6.0:LP_020600_VDB_EliminateUnusedColumnsinMKTO_LeadRecord'
  version     = '$Rev: 71049 $'
  def __init__( self, forceApply = False ):
    super( LP_020600_VDB_EliminateUnusedColumnsinMKTO_LeadRecord, self ).__init__( forceApply )

  def getApplicability(self, appseq):
    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText('template_type')
    if type =='MKTO':
      return Applicability.canApply
    else:
      return Applicability.cannotApplyPass

  def apply(self, appseq):

    success = False
    conn_mgr = appseq.getConnectionMgr()
    type = appseq.getText('template_type')

    if type == 'MKTO':
      #Remove the columns from the query Q_Unpivot_MKTO_LeadRecord
      query = conn_mgr.getQuery("Q_Unpivot_MKTO_LeadRecord")
      exp_1 = liaison.ExpressionVDBImplGeneric('ContainerElementName("MarketoSocialLinkedInReach"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("MarketoSocialLinkedInReach")))')
      col_1 = liaison.QueryColumnVDBImpl('MarketoSocialLinkedInReach', exp_1)

      exp_2 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("MarketoSocialTwitterReach"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("MarketoSocialTwitterReach")))')
      col_2 = liaison.QueryColumnVDBImpl('MarketoSocialTwitterReach', exp_2)

      exp_3 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("MarketoSocialYahooReach"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("MarketoSocialYahooReach")))')
      col_3 = liaison.QueryColumnVDBImpl('MarketoSocialYahooReach', exp_3)

      exp_4 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("SAP_CRM_Person_Type"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("SAP_CRM_Person_Type")))')
      col_4 = liaison.QueryColumnVDBImpl('SAP_CRM_Person_Type', exp_4)

      exp_5 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("SAP_CRM_Qualification"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("SAP_CRM_Qualification")))')
      col_5 = liaison.QueryColumnVDBImpl('SAP_CRM_Qualification', exp_5)

      exp_6 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("DoNotCallReason"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("DoNotCallReason")))')
      col_6 = liaison.QueryColumnVDBImpl('DoNotCallReason', exp_6)

      exp_7 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("LeadRole"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("LeadRole")))')
      col_7 = liaison.QueryColumnVDBImpl('LeadRole', exp_7)

      exp_8 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("MarketoSocialFacebookReferredEnrollments"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("MarketoSocialFacebookReferredEnrollments")))')
      col_8 = liaison.QueryColumnVDBImpl('MarketoSocialFacebookReferredEnrollments', exp_8)

      exp_9 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("MarketoSocialFacebookReferredVisits"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("MarketoSocialFacebookReferredVisits")))')
      col_9 = liaison.QueryColumnVDBImpl('MarketoSocialFacebookReferredVisits', exp_9)

      exp_10 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("MarketoSocialLinkedInReferredEnrollments"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("MarketoSocialLinkedInReferredEnrollments")))')
      col_10 = liaison.QueryColumnVDBImpl('MarketoSocialLinkedInReferredEnrollments', exp_10)

      exp_11 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("MarketoSocialLinkedInReferredVisits"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("MarketoSocialLinkedInReferredVisits")))')
      col_11 = liaison.QueryColumnVDBImpl('MarketoSocialLinkedInReferredVisits', exp_11)

      exp_12 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("MarketoSocialTotalReferredEnrollments"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("MarketoSocialTotalReferredEnrollments")))')
      col_12 = liaison.QueryColumnVDBImpl('MarketoSocialTotalReferredEnrollments', exp_12)

      exp_13 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("MarketoSocialTotalReferredVisits"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("MarketoSocialTotalReferredVisits")))')
      col_13 = liaison.QueryColumnVDBImpl('MarketoSocialTotalReferredVisits', exp_13)

      exp_14 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("MarketoSocialTwitterReferredEnrollments"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("MarketoSocialTwitterReferredEnrollments")))')
      col_14 = liaison.QueryColumnVDBImpl('MarketoSocialTwitterReferredEnrollments', exp_14)

      exp_15 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("MarketoSocialTwitterReferredVisits"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("MarketoSocialTwitterReferredVisits")))')
      col_15 = liaison.QueryColumnVDBImpl('MarketoSocialTwitterReferredVisits', exp_15)

      exp_16 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("MarketoSocialYahooReferredEnrollments"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("MarketoSocialYahooReferredEnrollments")))')
      col_16 = liaison.QueryColumnVDBImpl('MarketoSocialYahooReferredEnrollments', exp_16)

      exp_17 = liaison.ExpressionVDBImplGeneric(
        'ContainerElementName("MarketoSocialYahooReferredVisits"),LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")),ContainerElementName("MarketoSocialYahooReferredVisits")))')
      col_17 = liaison.QueryColumnVDBImpl('MarketoSocialYahooReferredVisits', exp_17)

      query.removeColumn('MarketoSocialLinkedInReach')
      query.removeColumn('MarketoSocialTwitterReach')
      query.removeColumn('MarketoSocialYahooReach')
      query.removeColumn('SAP_CRM_Person_Type')
      query.removeColumn('SAP_CRM_Qualification')
      query.removeColumn('DoNotCallReason')
      query.removeColumn('LeadRole')
      query.removeColumn('MarketoSocialFacebookReferredEnrollments')
      query.removeColumn('MarketoSocialFacebookReferredVisits')
      query.removeColumn('MarketoSocialLinkedInReferredEnrollments')
      query.removeColumn('MarketoSocialLinkedInReferredVisits')
      query.removeColumn('MarketoSocialTotalReferredEnrollments')
      query.removeColumn('MarketoSocialTotalReferredVisits')
      query.removeColumn('MarketoSocialTwitterReferredEnrollments')
      query.removeColumn('MarketoSocialTwitterReferredVisits')
      query.removeColumn('MarketoSocialYahooReferredEnrollments')
      query.removeColumn('MarketoSocialYahooReferredVisits')
      conn_mgr.setQuery(query)

      # Modify the Column in MKTO_LeadRecord
      table1 = conn_mgr.getTable('MKTO_LeadRecord')
      table1.removeColumn('MarketoSocialLinkedInReach')
      table1.removeColumn('MarketoSocialTwitterReach')
      table1.removeColumn('MarketoSocialYahooReach')
      table1.removeColumn('SAP_CRM_Person_Type')
      table1.removeColumn('SAP_CRM_Qualification')
      table1.removeColumn('DoNotCallReason')
      table1.removeColumn('LeadRole')
      table1.removeColumn('MarketoSocialFacebookReferredEnrollments')
      table1.removeColumn('MarketoSocialFacebookReferredVisits')
      table1.removeColumn('MarketoSocialLinkedInReferredEnrollments')
      table1.removeColumn('MarketoSocialLinkedInReferredVisits')
      table1.removeColumn('MarketoSocialTotalReferredEnrollments')
      table1.removeColumn('MarketoSocialTotalReferredVisits')
      table1.removeColumn('MarketoSocialTwitterReferredEnrollments')
      table1.removeColumn('MarketoSocialTwitterReferredVisits')
      table1.removeColumn('MarketoSocialYahooReferredEnrollments')
      table1.removeColumn('MarketoSocialYahooReferredVisits')
      conn_mgr.setTable(table1)

      # Modify the Column in MKTO_LeadRecord
      table2 = conn_mgr.getTable('MKTO_LeadRecord_Diagnostic')
      table2.removeColumn('MarketoSocialLinkedInReach')
      table2.removeColumn('MarketoSocialTwitterReach')
      table2.removeColumn('MarketoSocialYahooReach')
      table2.removeColumn('SAP_CRM_Person_Type')
      table2.removeColumn('SAP_CRM_Qualification')
      table2.removeColumn('DoNotCallReason')
      table2.removeColumn('LeadRole')
      table2.removeColumn('MarketoSocialFacebookReferredEnrollments')
      table2.removeColumn('MarketoSocialFacebookReferredVisits')
      table2.removeColumn('MarketoSocialLinkedInReferredEnrollments')
      table2.removeColumn('MarketoSocialLinkedInReferredVisits')
      table2.removeColumn('MarketoSocialTotalReferredEnrollments')
      table2.removeColumn('MarketoSocialTotalReferredVisits')
      table2.removeColumn('MarketoSocialTwitterReferredEnrollments')
      table2.removeColumn('MarketoSocialTwitterReferredVisits')
      table2.removeColumn('MarketoSocialYahooReferredEnrollments')
      table2.removeColumn('MarketoSocialYahooReferredVisits')
      conn_mgr.setTable(table2)
    else:
      pass
    success = True

    return success
