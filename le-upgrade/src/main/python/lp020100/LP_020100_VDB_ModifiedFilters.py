
#
# $LastChangedBy: YeTian $
# $LastChangedDate: 2015-10-21 14:33:11 +0800 (Wed, 21 Oct 2015) $
# $Rev: 70508 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import appsequence
import liaison

class LP_020100_VDB_ModifiedFilters( StepBase ):
  
  name        = 'LP_020100_VDB_ModifiedFilters'
  description = 'Upgrade Modified Specs from 2.0.1 to 2.1.0'
  version     = '$Rev: 70508 $'
  def __init__( self, forceApply = False ):
    super( LP_020100_VDB_ModifiedFilters, self ).__init__( forceApply )

  def getApplicability( self, appseq ):

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )
      if type == 'MKTO':
          conn_mgr.getSpec("Q_Dante_LeadSourceTable")
          conn_mgr.getSpec("Q_Dante_ContactSourceTable")
          conn_mgr.getSpec("Q_LeadScoreForLeadDestination_Query")
          conn_mgr.getSpec("Q_Timestamp_PushToDante")
          conn_mgr.getSpec("Q_Timestamp_PushToDestination")
          conn_mgr.getSpec("Q_Diagnostic_GetLeadRecordID")

          if not conn_mgr.getSpec("Q_Dante_LeadSourceTable") and not conn_mgr.getSpec("Q_Dante_ContactSourceTable") and not conn_mgr.getSpec("Q_LeadScoreForLeadDestination_Query")and not conn_mgr.getSpec("Q_Timestamp_PushToDante")and not conn_mgr.getSpec("Q_Timestamp_PushToDestination"):
              return Applicability.cannotApplyFail

      elif type == 'ELQ':
          conn_mgr.getSpec("Q_Dante_LeadSourceTable")
          conn_mgr.getSpec("Q_Dante_ContactSourceTable")
          conn_mgr.getSpec("Q_ELQ_Contact_Score")
          conn_mgr.getSpec("Q_Timestamp_PushToDante")
          conn_mgr.getSpec("Q_Timestamp_PushToDestination")

          if not conn_mgr.getSpec("Q_Dante_LeadSourceTable") and not conn_mgr.getSpec("Q_Dante_ContactSourceTable") and not conn_mgr.getSpec("Q_ELQ_Contact_Score")and not conn_mgr.getSpec("Q_Timestamp_PushToDante")and not conn_mgr.getSpec("Q_Timestamp_PushToDestination"):
              return Applicability.cannotApplyFail
      else:
          conn_mgr.getSpec("Q_Dante_LeadSourceTable")
          conn_mgr.getSpec("Q_SFDC_Contact_Score")
          conn_mgr.getSpec("Q_SFDC_Lead_Score")
          conn_mgr.getSpec("Q_Timestamp_PushToDante")
          conn_mgr.getSpec("Q_Timestamp_PushToDestination")
          if not conn_mgr.getSpec("Q_Dante_LeadSourceTable") and not conn_mgr.getSpec("Q_SFDC_Contact_Score") and not conn_mgr.getSpec("Q_SFDC_Lead_Score")and not conn_mgr.getSpec("Q_Timestamp_PushToDante")and not conn_mgr.getSpec("Q_Timestamp_PushToDestination"):
              return Applicability.cannotApplyFail

      return  Applicability.canApply

  def apply( self, appseq ):

      success = False

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )

      filters_new1 = []
      filters_new2 = []
      filters_new3 = []
      filters_new4 = []
      filters_new5 = []
      filters_new6 = []

      if type == 'MKTO':
          #Modify the Filter in Q_Dante_LeadSourceTable
          Q_Dante_LeadSourceTable    = conn_mgr.getQuery("Q_Dante_LeadSourceTable")
          filter1 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetFcn(LatticeFunctionIdentifier(ContainerElementName("Dante_Stage_IsSelectedForDanteLead")),LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante_Stage")))))')
          filters_new1.append(filter1)
          Q_Dante_LeadSourceTable.filters_ = filters_new1

          #print Q_Dante_LeadSourceTable.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_Dante_LeadSourceTable',Q_Dante_LeadSourceTable.SpecLatticeNamedElements())

          #Modify the Filter in Q_Dante_ContactSourceTable
          Q_Dante_ContactSourceTable = conn_mgr.getQuery("Q_Dante_ContactSourceTable")
          filter2 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetFcn(LatticeFunctionIdentifier(ContainerElementName("Dante_Stage_IsSelectedForDanteContact")),LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante_Stage")))))')
          filters_new2.append(filter2)
          Q_Dante_ContactSourceTable.filters_ = filters_new2

          #print Q_Dante_ContactSourceTable.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_Dante_ContactSourceTable',Q_Dante_ContactSourceTable.SpecLatticeNamedElements())

          #Modify the Filter in Q_LeadScoreForLeadDestination_Query
          Q_LeadScoreForLeadDestination_Query = conn_mgr.getQuery("Q_LeadScoreForLeadDestination_Query")
          filter3 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetFcn(LatticeFunctionIdentifier(ContainerElementName("MKTO_LeadRecord_ID_IsSelectedForPushToDestination")),LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante")))))')
          filters_new3.append(filter3)
          Q_LeadScoreForLeadDestination_Query.filters_ = filters_new3

          #print Q_LeadScoreForLeadDestination_Query.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_LeadScoreForLeadDestination_Query',Q_LeadScoreForLeadDestination_Query.SpecLatticeNamedElements())

          #Modify the Filter in Q_Timestamp_PushToDante
          Q_Timestamp_PushToDante = conn_mgr.getQuery("Q_Timestamp_PushToDante")
          filter4 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante_Stage"))))')
          filters_new4.append(filter4)
          Q_Timestamp_PushToDante.filters_ = filters_new4

          #print Q_Timestamp_PushToDante.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_Timestamp_PushToDante',Q_Timestamp_PushToDante.SpecLatticeNamedElements())

          #Modify the Filter in Q_Timestamp_PushToDestination
          Q_Timestamp_PushToDestination = conn_mgr.getQuery("Q_Timestamp_PushToDestination")
          filter5 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetFcn(LatticeFunctionIdentifier(ContainerElementName("MKTO_LeadRecord_ID_IsSelectedForPushToDestination")),LatticeAddressSetSourceTable(LatticeSourceTableIdentifier(ContainerElementName("Timestamp_PushToDante")),LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante")))))')
          filters_new5.append(filter5)
          Q_Timestamp_PushToDestination.filters_ = filters_new5
          #print Q_Timestamp_PushToDestination.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_Timestamp_PushToDestination',Q_Timestamp_PushToDestination.SpecLatticeNamedElements())

          #Modify the Filter in Q_Diagnostic_GetLeadRecordID
          Q_Diagnostic_GetLeadRecordID = conn_mgr.getQuery("Q_Diagnostic_GetLeadRecordID")
          filter6 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetIdentifier(ContainerElementName("Diagnostic_Filter_ExtractedLeadRecords"))')
          filters_new6.append(filter6)
          Q_Diagnostic_GetLeadRecordID.filters_ = filters_new6

          #print Q_Diagnostic_GetLeadRecordID.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_Diagnostic_GetLeadRecordID',Q_Diagnostic_GetLeadRecordID.SpecLatticeNamedElements())

      elif type =='ELQ':
          #Modify the Filter in Q_Dante_LeadSourceTable
          Q_Dante_LeadSourceTable    = conn_mgr.getQuery("Q_Dante_LeadSourceTable")
          filter1 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetFcn(LatticeFunctionIdentifier(ContainerElementName("Dante_Stage_IsSelectedForDanteLead")),LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante_Stage")))))')
          filters_new1.append(filter1)
          Q_Dante_LeadSourceTable.filters_ = filters_new1

          #print Q_Dante_LeadSourceTable.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_Dante_LeadSourceTable',Q_Dante_LeadSourceTable.SpecLatticeNamedElements())

          #Modify the Filter in Q_Dante_ContactSourceTable
          Q_Dante_ContactSourceTable = conn_mgr.getQuery("Q_Dante_ContactSourceTable")
          filter2 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetFcn(LatticeFunctionIdentifier(ContainerElementName("Dante_Stage_IsSelectedForDanteContact")),LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante_Stage")))))')
          filters_new2.append(filter2)
          Q_Dante_ContactSourceTable.filters_ = filters_new2

          #print Q_Dante_ContactSourceTable.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_Dante_ContactSourceTable',Q_Dante_ContactSourceTable.SpecLatticeNamedElements())

          #Modify the Filter in Q_ELQ_Contact_Score
          Q_ELQ_Contact_Score = conn_mgr.getQuery("Q_ELQ_Contact_Score")
          filter3_1 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetFcn(LatticeFunctionIdentifier(ContainerElementName("ELQ_Contact_ContactID_IsSelectedForPushToDestination"))')
          filter3_2 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetSourceTable(LatticeSourceTableIdentifier(ContainerElementName("Timestamp_PushToDante")),LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante")))))')
          filters_new3.append(filter3_1)
          filters_new3.append(filter3_2)
          Q_ELQ_Contact_Score.filters_ = filters_new3

          #print Q_ELQ_Contact_Score.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_LeadScoreForLeadDestination_Query',Q_ELQ_Contact_Score.SpecLatticeNamedElements())

          #Modify the Filter in Q_Timestamp_PushToDante
          Q_Timestamp_PushToDante = conn_mgr.getQuery("Q_Timestamp_PushToDante")
          filter4 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante_Stage"))))')
          filters_new4.append(filter4)
          Q_Timestamp_PushToDante.filters_ = filters_new4

          #print Q_Timestamp_PushToDante.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_Timestamp_PushToDante',Q_Timestamp_PushToDante.SpecLatticeNamedElements())

          #Modify the Filter in Q_Timestamp_PushToDestination
          Q_Timestamp_PushToDestination = conn_mgr.getQuery("Q_Timestamp_PushToDestination")
          filter5 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetFcn(LatticeFunctionIdentifier(ContainerElementName("ELQ_Contact_ContactID_IsSelectedForPushToDestination")),LatticeAddressSetSourceTable(LatticeSourceTableIdentifier(ContainerElementName("Timestamp_PushToDante")),LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante")))))')
          filters_new5.append(filter5)
          Q_Timestamp_PushToDestination.filters_ = filters_new5
          #print Q_Timestamp_PushToDestination.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_Timestamp_PushToDestination',Q_Timestamp_PushToDestination.SpecLatticeNamedElements())

      else:
          #Modify the Filter in Q_Dante_LeadSourceTable
          Q_Dante_LeadSourceTable    = conn_mgr.getQuery("Q_Dante_LeadSourceTable")
          filter1 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante_Stage"))))')
          filters_new1.append(filter1)
          Q_Dante_LeadSourceTable.filters_ = filters_new1

          #print Q_Dante_LeadSourceTable.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_Dante_LeadSourceTable',Q_Dante_LeadSourceTable.SpecLatticeNamedElements())

          #Modify the Filter in Q_SFDC_Contact_Score
          Q_SFDC_Contact_Score = conn_mgr.getQuery("Q_SFDC_Contact_Score")
          filter2 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetIdentifier(ContainerElementName("SelectedForPushingToDestination_Contact"))')
          filters_new2.append(filter2)
          Q_SFDC_Contact_Score.filters_ = filters_new2
          #print Q_SFDC_Contact_Score.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_Dante_ContactSourceTable',Q_SFDC_Contact_Score.SpecLatticeNamedElements())

          #Modify the Filter in Q_SFDC_Lead_Score
          Q_SFDC_Lead_Score = conn_mgr.getQuery("Q_SFDC_Lead_Score")
          filter3 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetIdentifier(ContainerElementName("SelectedForPushingToDestination_Lead"))')
          filters_new3.append(filter3)
          Q_SFDC_Lead_Score.filters_ = filters_new3

          #print Q_SFDC_Lead_Score.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_SFDC_Lead_Score',Q_SFDC_Lead_Score.SpecLatticeNamedElements())

          #Modify the Filter in Q_Timestamp_PushToDante
          Q_Timestamp_PushToDante = conn_mgr.getQuery("Q_Timestamp_PushToDante")
          filter4 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetSourceTable(LatticeSourceTableIdentifier(ContainerElementName("Timestamp_PushToDante_Stage")),LatticeAddressExpressionAtomic(LatticeAddressAtomicNoKeys(ContainerElementName("Timestamp_PushToDante_Stage"))))')
          filters_new4.append(filter4)
          Q_Timestamp_PushToDante.filters_ = filters_new4

          #print Q_Timestamp_PushToDante.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_Timestamp_PushToDante',Q_Timestamp_PushToDante.SpecLatticeNamedElements())

          #Modify the Filter in Q_Timestamp_PushToDestination
          Q_Timestamp_PushToDestination = conn_mgr.getQuery("Q_Timestamp_PushToDestination")
          filter5 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetFcn(LatticeFunctionIdentifier(ContainerElementName("SFDC_Lead_Contact_ID_IsSelectedForPushToDestination")),LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante")))))')
          filters_new5.append(filter5)
          Q_Timestamp_PushToDestination.filters_ = filters_new5

          #print Q_Timestamp_PushToDestination.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_Timestamp_PushToDestination',Q_Timestamp_PushToDestination.SpecLatticeNamedElements())

      success = True

      return success






