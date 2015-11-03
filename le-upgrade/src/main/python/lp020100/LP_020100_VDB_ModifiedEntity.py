
#
# $LastChangedBy: YeTian $
# $LastChangedDate: 2015-10-21 14:33:11 +0800 (Wed, 21 Oct 2015) $
# $Rev: 70508 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import appsequence
import liaison

class LP_020100_VDB_ModifiedEntity( StepBase ):
  
  name        = 'LP_020100_VDB_ModifiedEntity'
  description = 'Upgrade Modified Specs from 2.0.1 to 2.1.0'
  version     = '$Rev: 70508 $'
  def __init__( self, forceApply = False ):
    super( LP_020100_VDB_ModifiedEntity, self ).__init__( forceApply )

  def getApplicability( self, appseq ):

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )
      if type == 'MKTO' or type == 'ELQ':
          conn_mgr.getSpec("Q_Dante_ContactSourceTable")
          conn_mgr.getSpec("Q_Dante_LeadSourceTable")

          if not conn_mgr.getSpec("Q_Dante_LeadSourceTable") and conn_mgr.getSpec("Q_Dante_ContactSourceTable"):
              return Applicability.cannotApplyFail

      else:
          conn_mgr.getSpec("Q_Dante_LeadSourceTable")

          if not conn_mgr.getSpec("Q_Dante_LeadSourceTable"):
              return Applicability.cannotApplyFail

      return  Applicability.canApply

  def apply( self, appseq ):

      success = False

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )

      entities_new = []

      if type == 'MKTO':
          #Modify the Entity in Q_Dante_ContactSourceTable
          Q_Dante_ContactSourceTable    = conn_mgr.getQuery("Q_Dante_ContactSourceTable")
          entity1 = liaison.ExpressionVDBImplGeneric('LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("SFDC_Contact_ID")))')
          entity2 = liaison.ExpressionVDBImplGeneric('LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("MKTO_LeadRecord_ID")))')
          entities_new.append(entity1)
          entities_new.append(entity2)
          Q_Dante_ContactSourceTable.entities_ = entities_new

          entityColumn1 = liaison.ExpressionVDBImplGeneric('SpecQueryNamedFunctionExpression(ContainerElementName("MKTO_LeadRecord_ID"),LatticeFunctionIdentifierAddressAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("MKTO_LeadRecord_ID"))))')
          Q_Dante_ContactSourceTable.columns_.insert(1,entityColumn1)

          conn_mgr.setSpec('Q_Dante_ContactSourceTable',Q_Dante_ContactSourceTable.SpecLatticeNamedElements())

          #Modify the Entity in Q_Dante_LeadSourceTable
          entities_new = []
          Q_Dante_LeadSourceTable    = conn_mgr.getQuery("Q_Dante_LeadSourceTable")
          entity3 = liaison.ExpressionVDBImplGeneric('LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("SFDC_Lead_ID")))')
          entity4 = liaison.ExpressionVDBImplGeneric('LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("MKTO_LeadRecord_ID")))')
          entities_new.append(entity3)
          entities_new.append(entity4)
          Q_Dante_LeadSourceTable.entities_ = entities_new

          entityColumn2 = liaison.ExpressionVDBImplGeneric('SpecQueryNamedFunctionExpression(ContainerElementName("MKTO_LeadRecord_ID"),LatticeFunctionIdentifierAddressAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("MKTO_LeadRecord_ID"))))')
          Q_Dante_LeadSourceTable.columns_.insert(1,entityColumn2)

          conn_mgr.setSpec('Q_Dante_LeadSourceTable',Q_Dante_LeadSourceTable.SpecLatticeNamedElements())

      elif type == 'ELQ':
           #Modify the Entity in Q_Dante_ContactSourceTable
          entities_new = []
          Q_Dante_ContactSourceTable    = conn_mgr.getQuery("Q_Dante_ContactSourceTable")
          entity1 = liaison.ExpressionVDBImplGeneric('LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("SFDC_Contact_ID")))')
          entity2 = liaison.ExpressionVDBImplGeneric('LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("ELQ_Contact_ContactID")))')
          entities_new.append(entity1)
          entities_new.append(entity2)
          Q_Dante_ContactSourceTable.entities_ = entities_new

          entityColumn2 = liaison.ExpressionVDBImplGeneric('SpecQueryNamedFunctionExpression(ContainerElementName("ELQ_Contact_ContactID"),LatticeFunctionIdentifierAddressAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("ELQ_Contact_ContactID"))))')
          Q_Dante_ContactSourceTable.columns_.insert(1,entityColumn2)

          conn_mgr.setSpec('Q_Dante_ContactSourceTable',Q_Dante_ContactSourceTable.SpecLatticeNamedElements())

          #Modify the Entity in Q_Dante_LeadSourceTable
          entities_new = []
          Q_Dante_LeadSourceTable    = conn_mgr.getQuery("Q_Dante_LeadSourceTable")
          entity3 = liaison.ExpressionVDBImplGeneric('LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("SFDC_Lead_ID")))')
          entity4 = liaison.ExpressionVDBImplGeneric('LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("ELQ_Contact_ContactID")))')
          entities_new.append(entity3)
          entities_new.append(entity4)
          Q_Dante_LeadSourceTable.entities_ = entities_new

          entityColumn2 = liaison.ExpressionVDBImplGeneric('SpecQueryNamedFunctionExpression(ContainerElementName("ELQ_Contact_ContactID"),LatticeFunctionIdentifierAddressAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("ELQ_Contact_ContactID"))))')
          Q_Dante_LeadSourceTable.columns_.insert(1,entityColumn2)

          conn_mgr.setSpec('Q_Dante_LeadSourceTable',Q_Dante_LeadSourceTable.SpecLatticeNamedElements())
      else:
           #Modify the Entity in Q_Dante_LeadSourceTable
          entities_new = []
          Q_Dante_LeadSourceTable    = conn_mgr.getQuery("Q_Dante_LeadSourceTable")
          entity1 = liaison.ExpressionVDBImplGeneric('LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("SFDC_Lead_Contact_ID")))')
          entities_new.append(entity1)
          Q_Dante_LeadSourceTable.entities_ = entities_new

          entityColumn2 = liaison.ExpressionVDBImplGeneric('SpecQueryNamedFunctionExpression(ContainerElementName("SFDC_Lead_Contact_ID"),LatticeFunctionIdentifierAddressAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("SFDC_Lead_Contact_ID"))))')
          Q_Dante_LeadSourceTable.columns_.insert(1,entityColumn2)
          Q_Dante_LeadSourceTable.removeColumn('SFDCLeadID')

          conn_mgr.setSpec('Q_Dante_LeadSourceTable',Q_Dante_LeadSourceTable.SpecLatticeNamedElements())



      success = True

      return success






