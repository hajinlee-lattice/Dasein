#
# $LastChangedBy: lyan $
# $LastChangedDate: 2015-11-20 01:50:33 +0800 (Fri, 20 Nov 2015) $
# $Rev: 71049 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import liaison


class LP_020200_VDB_ModifiedSpec(StepBase):
  name = 'LP_020200_VDB_ModifiedSpec'
  description = 'Upgrade Modified Specs from 2.1.2 to 2.2.0'
  version = '$Rev: 71049 $'

  def __init__(self, forceApply=False):
    super(LP_020200_VDB_ModifiedSpec, self).__init__(forceApply)

    ##Check SelectedForDante existig in Spec

  def getApplicability(self, appseq):

    conn_mgr = appseq.getConnectionMgr()
    conn_mgr.getSpec("Const_DaysOfDataForScoringBulk")
    conn_mgr.getSpec("HAVING_DownloadedLeadNotMatched_PD")
    conn_mgr.getSpec("SelectedForScoringIncr")
    type = appseq.getText('template_type')

    if type == 'MKTO':
      conn_mgr.getSpec("MKTO_LeadRecord_ID_ToBeScored")
      conn_mgr.getSpec("SelectedForPushToDestination_MKTO")

      if not conn_mgr.getSpec("Const_DaysOfDataForScoringBulk") \
        and not conn_mgr.getSpec("HAVING_DownloadedLeadNotMatched_PD") \
        and not conn_mgr.getSpec("SelectedForScoringIncr") \
        and not conn_mgr.getSpec("MKTO_LeadRecord_ID_ToBeScored") \
        and not conn_mgr.getSpec("SelectedForPushToDestination_MKTO") \
        and not conn_mgr.getSpec("Dante_Stage_ExistScoreNewSFDC_Contacts") \
        and not conn_mgr.getSpec("Dante_Stage_ExistScoreNewSFDC_Leads"):
        return Applicability.cannotApplyFail

    elif type == 'ELQ':
      conn_mgr.getSpec("SelectedForPushToDestination_ELQ")
      conn_mgr.getSpec("SelectedForScoringIncr_Timestamp")
      if not conn_mgr.getSpec("Const_DaysOfDataForScoringBulk") \
        and not conn_mgr.getSpec("HAVING_DownloadedLeadNotMatched_PD") \
        and not conn_mgr.getSpec("SelectedForScoringIncr") \
        and not conn_mgr.getSpec("SelectedForPushToDestination_ELQ") \
        and not conn_mgr.getSpec("SelectedForScoringIncr_Timestamp") \
        and not conn_mgr.getSpec("Dante_Stage_ExistScoreNewSFDC_Contacts") \
        and not conn_mgr.getSpec("Dante_Stage_ExistScoreNewSFDC_Leads"):
        return Applicability.cannotApplyFail

    else:
      conn_mgr.getSpec("SelectedForPushingToDestination_Contact")
      conn_mgr.getSpec("SelectedForPushingToDestination_Lead")
      conn_mgr.getSpec("SelectedForScoringIncr_Timestamp")
      if not conn_mgr.getSpec("Const_DaysOfDataForScoringBulk") \
        and not conn_mgr.getSpec("HAVING_DownloadedLeadNotMatched_PD") \
        and not conn_mgr.getSpec("SelectedForScoringIncr") \
        and not conn_mgr.getSpec("SelectedForScoringIncr_Timestamp") \
        and not conn_mgr.getSpec("SelectedForPushingToDestination_Contact") \
        and not conn_mgr.getSpec("SelectedForPushingToDestination_Lead"):
        return Applicability.cannotApplyFail

    return Applicability.canApply

    ##Check Time_OfMostRecentPushToDante existig in Spec

  def getApplicability(self, appseq):

    conn_mgr = appseq.getConnectionMgr()
    conn_mgr.getSpec("Time_OfMostRecentPushToDante")

    if not conn_mgr.getSpec("Time_OfMostRecentPushToDante"):
      return Applicability.cannotApplyFail

    return Applicability.canApply

  def apply(self, appseq):

    success = False

    conn_mgr = appseq.getConnectionMgr()
    type = appseq.getText('template_type')

    if type == 'MKTO':
      # Spec1: Modify Const_DaysOfDataForScoringBulk
      spec1 = 'SpecLatticeNamedElements(SpecLatticeNamedElement(' \
              '	SpecLatticeFunction(' \
              '		LatticeFunctionExpressionConstantScalar(' \
              '			"90",' \
              '			DataTypeInt' \
              '		),' \
              '		DataTypeUnknown,' \
              '		SpecFunctionTypeMetric,' \
              '		SpecFunctionSourceTypeCalculation,' \
              '		SpecDefaultValueNull,' \
              '		SpecDescription("")' \
              '	),' \
              '	ContainerElementName("Const_DaysOfDataForScoringBulk")' \
              '))'

      Const_DaysOfDataForScoringBulk = liaison.ExpressionVDBImplGeneric(spec1)
      conn_mgr.setSpec('Const_DaysOfDataForScoringBulk', Const_DaysOfDataForScoringBulk.definition())

      # Spec2: Modify HAVING_DownloadedLeadNotMatched_PD
      spec2 = 'SpecLatticeNamedElements(SpecLatticeNamedElement(' \
              '	SpecLatticeAliasDeclaration(' \
              '		LatticeAddressSetIdentifier(' \
              '			ContainerElementName("HAVING_DownloadedLeadNotMatched_Timestamp")' \
              '		)' \
              '	),' \
              '	ContainerElementName("HAVING_DownloadedLeadNotMatched_PD")' \
              '))'

      HAVING_DownloadedLeadNotMatched_PD = liaison.ExpressionVDBImplGeneric(spec2)
      conn_mgr.setSpec('HAVING_DownloadedLeadNotMatched_PD', HAVING_DownloadedLeadNotMatched_PD.definition())

      # Spec3: Modify SelectedForScoringIncr
      spec3 = 'SpecLatticeNamedElements(SpecLatticeNamedElement(' \
              '	SpecLatticeAliasDeclaration(' \
              '		LatticeAddressSetIdentifier(' \
              '			ContainerElementName("SelectedForScoringIncr_Timestamp")' \
              '		)' \
              '	),' \
              '	ContainerElementName("SelectedForScoringIncr")' \
              '))'

      SelectedForScoringIncr = liaison.ExpressionVDBImplGeneric(spec3)
      conn_mgr.setSpec('SelectedForScoringIncr', SelectedForScoringIncr.definition())

      # Spec4: Modify MKTO_LeadRecord_ID_ToBeScored
      spec4 = 'SpecLatticeNamedElements(' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeFunction(' \
              '		LatticeFunctionExpressionTransform(' \
              '			LatticeFunctionExpression(' \
              '				LatticeFunctionOperatorIdentifier("OR"),' \
              '				LatticeFunctionExpression(' \
              '					LatticeFunctionOperatorIdentifier("Equal"),' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementNameTableQualifiedName(' \
              '							LatticeSourceTableIdentifier(' \
              '								ContainerElementName("Timestamp_DownloadedLeads")' \
              '							),' \
              '							ContainerElementName("MKTO_LeadRecord_ID")' \
              '						)' \
              '					),' \
              '					LatticeFunctionExpressionConstant(' \
              '						"-111111.",' \
              '						DataTypeFloat' \
              '					)' \
              '				),' \
              '				LatticeFunctionExpression(' \
              '					LatticeFunctionOperatorIdentifier("IF"),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("Equal"),' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("Option_OlderLeads_NeedToBeScored")' \
              '						),' \
              '						LatticeFunctionExpressionConstant(' \
              '							"1",' \
              '							DataTypeInt' \
              '						)' \
              '					),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("OR"),' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("MKTO_LeadRecord_ID_ToBeScored_Incr")' \
              '						),' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("MKTO_LeadRecord_ID_ToBeScored_OlderLeads")' \
              '						)' \
              '					),' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementName("MKTO_LeadRecord_ID_ToBeScored_Incr")' \
              '					)' \
              '				)' \
              '			),' \
              '			LatticeAddressSetPi(' \
              '				LatticeAddressExpressionAtomic(' \
              '					LatticeAddressAtomicIdentifier(' \
              '						ContainerElementName("MKTO_LeadRecord_ID")' \
              '					)' \
              '				)' \
              '			),' \
              '			FunctionAggregationOperator("Max")' \
              '		),' \
              '		DataTypeUnknown,' \
              '		SpecFunctionTypeMetric,' \
              '		SpecFunctionSourceTypeCalculation,' \
              '		SpecDefaultValueNull,' \
              '		SpecDescription("")' \
              '	),' \
              '	ContainerElementName("MKTO_LeadRecord_ID_ToBeScored")' \
              '))'

      MKTO_LeadRecord_ID_ToBeScored = liaison.ExpressionVDBImplGeneric(spec4)
      conn_mgr.setSpec('MKTO_LeadRecord_ID_ToBeScored', MKTO_LeadRecord_ID_ToBeScored.definition())

      # Spec5: Modify SelectedForPushToDestination_MKTO
      spec5 = 'SpecLatticeNamedElements(' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeAliasDeclaration(' \
              '		LatticeAddressSetFcn(' \
              '			LatticeFunctionExpression(' \
              '				LatticeFunctionOperatorIdentifier("Greater"),' \
              '				LatticeFunctionIdentifier(' \
              '					ContainerElementNameTableQualifiedName(' \
              '						LatticeSourceTableIdentifier(' \
              '							ContainerElementName("Timestamp_LoadScoredLeads")' \
              '						),' \
              '						ContainerElementName("Time_OfCompletion_LoadScoredLeads")' \
              '					)' \
              '				),' \
              '				LatticeFunctionIdentifier(' \
              '					ContainerElementName("Time_OfMostRecentPushToDestination")' \
              '				)' \
              '			),' \
              '			LatticeAddressSetIdentifier(' \
              '				ContainerElementName("Alias_AllLeadTable")' \
              '			)' \
              '		)' \
              '	),' \
              '	ContainerElementName("SelectedForPushToDestination_MKTO")' \
              '))'

      SelectedForPushToDestination_MKTO = liaison.ExpressionVDBImplGeneric(spec5)
      conn_mgr.setSpec('SelectedForPushToDestination_MKTO', SelectedForPushToDestination_MKTO.definition())

      # Spec6: Modify Dante_Stage_ExistScoreNewSFDC_Contacts
      spec6 = 'SpecLatticeNamedElements(' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeFunction(' \
              '		LatticeFunctionExpressionTransform(' \
              '			LatticeFunctionExpression(' \
              '				LatticeFunctionOperatorIdentifier("IsNull"),' \
              '				LatticeFunctionExpressionTransform(' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("AND"),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("LessOrEqual"),' \
              '							LatticeFunctionIdentifier(' \
              '								ContainerElementNameTableQualifiedName(' \
              '									LatticeSourceTableIdentifier(' \
              '										ContainerElementName("Timestamp_PushToDante")' \
              '									),' \
              '									ContainerElementName("Time_OfCompletion_PushToDante")' \
              '								)' \
              '							),' \
              '							LatticeFunctionIdentifier(' \
              '								ContainerElementName("Time_OfMostRecentPushToDante")' \
              '							)' \
              '						),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("AND"),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("Equal"),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementNameTableQualifiedName(' \
              '										LatticeSourceTableIdentifier(' \
              '											ContainerElementName("SFDC_Contact")' \
              '										),' \
              '										ContainerElementName("CreatedDate")' \
              '									)' \
              '								),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementNameTableQualifiedName(' \
              '										LatticeSourceTableIdentifier(' \
              '											ContainerElementName("SFDC_Contact")' \
              '										),' \
              '										ContainerElementName("LastModifiedDate")' \
              '									)' \
              '								)' \
              '							),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("Greater"),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementNameTableQualifiedName(' \
              '										LatticeSourceTableIdentifier(' \
              '											ContainerElementName("Timestamp_DownloadedSFDC_Contacts")' \
              '										),' \
              '										ContainerElementName("Time_OfComepletion_DownloadedSFDC_Contacts")' \
              '									)' \
              '								),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementName("Time_OfMostRecentPushToDante")' \
              '								)' \
              '							)' \
              '						)' \
              '					),' \
              '					LatticeAddressSetPi(' \
              '						LatticeAddressExpressionAtomic(' \
              '							LatticeAddressAtomicIdentifier(' \
              '								ContainerElementName("SFDC_Contact_ID")' \
              '							)' \
              '						)' \
              '					),' \
              '					FunctionAggregationOperator("Max")' \
              '				),' \
              '				LatticeFunctionExpressionConstantScalar(' \
              '					"FALSE",' \
              '					DataTypeBit' \
              '				)' \
              '			),' \
              '			LatticeAddressSetPi(' \
              '				LatticeAddressExpressionAtomic(' \
              '					LatticeAddressAtomicIdentifier(' \
              '						ContainerElementName("MKTO_LeadRecord_ID")' \
              '					)' \
              '				)' \
              '			),' \
              '			FunctionAggregationOperator("None")' \
              '		),' \
              '		DataTypeUnknown,' \
              '		SpecFunctionTypeMetric,' \
              '		SpecFunctionSourceTypeCalculation,' \
              '		SpecDefaultValueNull,' \
              '		SpecDescription("")' \
              '	),' \
              '	ContainerElementName("Dante_Stage_ExistScoreNewSFDC_Contacts")' \
              '))'

      Dante_Stage_ExistScoreNewSFDC_Contacts = liaison.ExpressionVDBImplGeneric(spec6)
      conn_mgr.setSpec('Dante_Stage_ExistScoreNewSFDC_Contacts', Dante_Stage_ExistScoreNewSFDC_Contacts.definition())

      # Spec7: Modify Dante_Stage_ExistScoreNewSFDC_Lead
      spec7 = 'SpecLatticeNamedElements(SpecLatticeNamedElement(' \
              '	SpecLatticeFunction(' \
              '		LatticeFunctionExpressionTransform(' \
              '			LatticeFunctionExpression(' \
              '				LatticeFunctionOperatorIdentifier("IsNull"),' \
              '				LatticeFunctionExpressionTransform(' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("AND"),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("LessOrEqual"),' \
              '							LatticeFunctionIdentifier(' \
              '								ContainerElementNameTableQualifiedName(' \
              '									LatticeSourceTableIdentifier(' \
              '										ContainerElementName("Timestamp_PushToDante")' \
              '									),' \
              '									ContainerElementName("Time_OfCompletion_PushToDante")' \
              '								)' \
              '							),' \
              '							LatticeFunctionIdentifier(' \
              '								ContainerElementName("Time_OfMostRecentPushToDante")' \
              '							)' \
              '						),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("AND"),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("Equal"),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementNameTableQualifiedName(' \
              '										LatticeSourceTableIdentifier(' \
              '											ContainerElementName("SFDC_Lead")' \
              '										),' \
              '										ContainerElementName("CreatedDate")' \
              '									)' \
              '								),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementNameTableQualifiedName(' \
              '										LatticeSourceTableIdentifier(' \
              '											ContainerElementName("SFDC_Lead")' \
              '										),' \
              '										ContainerElementName("LastModifiedDate")' \
              '									)' \
              '								)' \
              '							),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("Greater"),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementNameTableQualifiedName(' \
              '										LatticeSourceTableIdentifier(' \
              '											ContainerElementName("Timestamp_DownloadedSFDC_Leads")' \
              '										),' \
              '										ContainerElementName("Time_OfComepletion_DownloadedSFDC_Leads")' \
              '									)' \
              '								),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementName("Time_OfMostRecentPushToDante")' \
              '								)' \
              '							)' \
              '						)' \
              '					),' \
              '					LatticeAddressSetPi(' \
              '						LatticeAddressExpressionAtomic(' \
              '							LatticeAddressAtomicIdentifier(' \
              '								ContainerElementName("SFDC_Lead_ID")' \
              '							)' \
              '						)' \
              '					),' \
              '					FunctionAggregationOperator("Max")' \
              '				),' \
              '				LatticeFunctionExpressionConstantScalar(' \
              '					"FALSE",' \
              '					DataTypeBit' \
              '				)' \
              '			),' \
              '			LatticeAddressSetPi(' \
              '				LatticeAddressExpressionAtomic(' \
              '					LatticeAddressAtomicIdentifier(' \
              '						ContainerElementName("MKTO_LeadRecord_ID")' \
              '					)' \
              '				)' \
              '			),' \
              '			FunctionAggregationOperator("None")' \
              '		),' \
              '		DataTypeUnknown,' \
              '		SpecFunctionTypeMetric,' \
              '		SpecFunctionSourceTypeCalculation,' \
              '		SpecDefaultValueNull,' \
              '		SpecDescription("")' \
              '	),' \
              '	ContainerElementName("Dante_Stage_ExistScoreNewSFDC_Leads")' \
              '))'

      Dante_Stage_ExistScoreNewSFDC_Lead = liaison.ExpressionVDBImplGeneric(spec7)
      conn_mgr.setSpec('Dante_Stage_ExistScoreNewSFDC_Lead', Dante_Stage_ExistScoreNewSFDC_Lead.definition())

      # Spec8: Modify Dante_Stage_ExistScoreNewSFDC_Lead
      spec8 = 'SpecLatticeNamedElements(' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeFunction(' \
              '		LatticeFunctionExpressionTransform(' \
              '			LatticeFunctionExpression(' \
              '				LatticeFunctionOperatorIdentifier("AND"),' \
              '				LatticeFunctionExpression(' \
              '					LatticeFunctionOperatorIdentifier("Greater"),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("AddDay"),' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("Alias_ModifiedDate_Lead")' \
              '						),' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("Const_DaysOfDataForModeling")' \
              '						)' \
              '					),' \
              '					LatticeFunctionExpressionTransform(' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("Alias_ModifiedDate_Lead")' \
              '						),' \
              '						LatticeAddressSetPi(' \
              '							LatticeAddressExpressionAtomic(LatticeAddressAtomicAll)' \
              '						),' \
              '						FunctionAggregationOperator("None")' \
              '					)' \
              '				),' \
              '				LatticeFunctionExpression(' \
              '					LatticeFunctionOperatorIdentifier("Greater"),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("Minus"),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("ConvertToInt"),' \
              '							LatticeFunctionExpressionConstant(' \
              '								"Now",' \
              '								DataTypeDateTime' \
              '							)' \
              '						),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("IsNull"),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("ConvertToInt"),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementName("Alias_CreatedDate_Lead")' \
              '								)' \
              '							),' \
              '							LatticeFunctionExpressionConstantScalar(' \
              '								"0",' \
              '								DataTypeInt' \
              '							)' \
              '						)' \
              '					),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("Multiply"),' \
              '						LatticeFunctionExpressionConstant(' \
              '							"86400",' \
              '							DataTypeInt' \
              '						),' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("Const_DaysFromLeadCreationDate")' \
              '						)' \
              '					)' \
              '				)' \
              '			),' \
              '			LatticeAddressSetIdentifier(' \
              '				ContainerElementName("Alias_AllLeadTable")' \
              '			),' \
              '			FunctionAggregationOperator("Max")' \
              '		),' \
              '		DataTypeUnknown,' \
              '		SpecFunctionTypeMetric,' \
              '		SpecFunctionSourceTypeCalculation,' \
              '		SpecDefaultValueNull,' \
              '		SpecDescription("")' \
              '	),' \
              '	ContainerElementName("Lead_InDateRangeForModeling")' \
              '))'

      Lead_InDateRangeForModeling = liaison.ExpressionVDBImplGeneric(spec8)
      conn_mgr.setSpec('Lead_InDateRangeForModeling', Lead_InDateRangeForModeling.definition())

    elif type == 'ELQ':
      spec1 = 'SpecLatticeNamedElements(SpecLatticeNamedElement(' \
              '	SpecLatticeFunction(' \
              '		LatticeFunctionExpressionConstantScalar(' \
              '			"90",' \
              '			DataTypeInt' \
              '		),' \
              '		DataTypeUnknown,' \
              '		SpecFunctionTypeMetric,' \
              '		SpecFunctionSourceTypeCalculation,' \
              '		SpecDefaultValueNull,' \
              '		SpecDescription("")' \
              '	),' \
              '	ContainerElementName("Const_DaysOfDataForScoringBulk")' \
              '))'
      Const_DaysOfDataForScoringBulk = liaison.ExpressionVDBImplGeneric(spec1)
      conn_mgr.setSpec('Const_DaysOfDataForScoringBulk', Const_DaysOfDataForScoringBulk.definition())

      # Spec2: Modify HAVING_DownloadedLeadNotMatched_PD
      spec2 = 'SpecLatticeNamedElements(SpecLatticeNamedElement(' \
              '	SpecLatticeAliasDeclaration(' \
              '		LatticeAddressSetIdentifier(' \
              '			ContainerElementName("HAVING_DownloadedLeadNotMatched_Timestamp")' \
              '		)' \
              '	),' \
              '	ContainerElementName("HAVING_DownloadedLeadNotMatched_PD")' \
              '))'

      HAVING_DownloadedLeadNotMatched_PD = liaison.ExpressionVDBImplGeneric(spec2)
      conn_mgr.setSpec('HAVING_DownloadedLeadNotMatched_PD', HAVING_DownloadedLeadNotMatched_PD.definition())

      # Spec3: Modify SelectedForScoringIncr
      spec3 = 'SpecLatticeNamedElements(SpecLatticeNamedElement(' \
              '	SpecLatticeAliasDeclaration(' \
              '		LatticeAddressSetIdentifier(' \
              '			ContainerElementName("SelectedForScoringIncr_Timestamp")' \
              '		)' \
              '	),' \
              '	ContainerElementName("SelectedForScoringIncr")' \
              '))'

      SelectedForScoringIncr = liaison.ExpressionVDBImplGeneric(spec3)
      conn_mgr.setSpec('SelectedForScoringIncr', SelectedForScoringIncr.definition())

      # Spec4: Modify SelectedForScoringIncr_Timestamp
      spec4 = 'SpecLatticeNamedElements(' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeAliasDeclaration(' \
              '		LatticeAddressSetFcn(' \
              '			LatticeFunctionExpression(' \
              '				LatticeFunctionOperatorIdentifier("IF"),' \
              '				LatticeFunctionExpression(' \
              '					LatticeFunctionOperatorIdentifier("Equal"),' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementName("Option_OlderLeads_NeedToBeScored")' \
              '					),' \
              '					LatticeFunctionExpressionConstant(' \
              '						"1",' \
              '						DataTypeInt' \
              '					)' \
              '				),' \
              '				LatticeFunctionExpression(' \
              '					LatticeFunctionOperatorIdentifier("OR"),' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementName("ELQ_Contact_ContactID_ToBeScored_Incr")' \
              '					),' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementName("ELQ_Contact_ContactID_ToBeScored_OlderLeads")' \
              '					)' \
              '				),' \
              '				LatticeFunctionIdentifier(' \
              '					ContainerElementName("ELQ_Contact_ContactID_ToBeScored_Incr")' \
              '				)' \
              '			),' \
              '			LatticeAddressSetIdentifier(' \
              '				ContainerElementName("Alias_AllLeadID")' \
              '			)' \
              '		)' \
              '	),' \
              '	ContainerElementName("SelectedForScoringIncr_Timestamp")' \
              '))'

      SelectedForScoringIncr_Timestamp = liaison.ExpressionVDBImplGeneric(spec4)
      conn_mgr.setSpec('SelectedForScoringIncr_Timestamp', SelectedForScoringIncr_Timestamp.definition())

      # Spec5: Modify SelectedForPushToDestination_ELQ
      spec5 = 'SpecLatticeNamedElements(' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeAliasDeclaration(' \
              '		LatticeAddressSetFcn(' \
              '			LatticeFunctionExpression(' \
              '				LatticeFunctionOperatorIdentifier("Greater"),' \
              '				LatticeFunctionIdentifier(' \
              '					ContainerElementNameTableQualifiedName(' \
              '						LatticeSourceTableIdentifier(' \
              '							ContainerElementName("Timestamp_LoadScoredLeads")' \
              '						),' \
              '						ContainerElementName("Time_OfCompletion_LoadScoredLeads")' \
              '					)' \
              '				),' \
              '				LatticeFunctionIdentifier(' \
              '					ContainerElementName("Time_OfMostRecentPushToDestination")' \
              '				)' \
              '			),' \
              '			LatticeAddressSetIdentifier(' \
              '				ContainerElementName("Alias_AllLeadTable")' \
              '			)' \
              '		)' \
              '	),' \
              '	ContainerElementName("SelectedForPushToDestination_ELQ")' \
              '))'
      SelectedForPushToDestination_ELQ = liaison.ExpressionVDBImplGeneric(spec5)
      conn_mgr.setSpec('SelectedForPushToDestination_ELQ', SelectedForPushToDestination_ELQ.definition())

      # Spec6: Modify Dante_Stage_ExistScoreNewSFDC_Contacts
      spec6 = 'SpecLatticeNamedElements(' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeFunction(' \
              '		LatticeFunctionExpressionTransform(' \
              '			LatticeFunctionExpression(' \
              '				LatticeFunctionOperatorIdentifier("IsNull"),' \
              '				LatticeFunctionExpressionTransform(' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("AND"),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("LessOrEqual"),' \
              '							LatticeFunctionIdentifier(' \
              '								ContainerElementNameTableQualifiedName(' \
              '									LatticeSourceTableIdentifier(' \
              '										ContainerElementName("Timestamp_PushToDante")' \
              '									),' \
              '									ContainerElementName("Time_OfCompletion_PushToDante")' \
              '								)' \
              '							),' \
              '							LatticeFunctionIdentifier(' \
              '								ContainerElementName("Time_OfMostRecentPushToDante")' \
              '							)' \
              '						),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("AND"),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("Equal"),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementNameTableQualifiedName(' \
              '										LatticeSourceTableIdentifier(' \
              '											ContainerElementName("SFDC_Contact")' \
              '										),' \
              '										ContainerElementName("CreatedDate")' \
              '									)' \
              '								),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementNameTableQualifiedName(' \
              '										LatticeSourceTableIdentifier(' \
              '											ContainerElementName("SFDC_Contact")' \
              '										),' \
              '										ContainerElementName("LastModifiedDate")' \
              '									)' \
              '								)' \
              '							),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("Greater"),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementNameTableQualifiedName(' \
              '										LatticeSourceTableIdentifier(' \
              '											ContainerElementName("Timestamp_DownloadedSFDC_Contacts")' \
              '										),' \
              '										ContainerElementName("Time_OfComepletion_DownloadedSFDC_Contacts")' \
              '									)' \
              '								),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementName("Time_OfMostRecentPushToDante")' \
              '								)' \
              '							)' \
              '						)' \
              '					),' \
              '					LatticeAddressSetPi(' \
              '						LatticeAddressExpressionAtomic(' \
              '							LatticeAddressAtomicIdentifier(' \
              '								ContainerElementName("SFDC_Contact_ID")' \
              '							)' \
              '						)' \
              '					),' \
              '					FunctionAggregationOperator("Max")' \
              '				),' \
              '				LatticeFunctionExpressionConstantScalar(' \
              '					"FALSE",' \
              '					DataTypeBit' \
              '				)' \
              '			),' \
              '			LatticeAddressSetPi(' \
              '				LatticeAddressExpressionAtomic(' \
              '					LatticeAddressAtomicIdentifier(' \
              '						ContainerElementName("ELQ_Contact_ContactID")' \
              '					)' \
              '				)' \
              '			),' \
              '			FunctionAggregationOperator("None")' \
              '		),' \
              '		DataTypeUnknown,' \
              '		SpecFunctionTypeMetric,' \
              '		SpecFunctionSourceTypeCalculation,' \
              '		SpecDefaultValueNull,' \
              '		SpecDescription("")' \
              '	),' \
              '	ContainerElementName("Dante_Stage_ExistScoreNewSFDC_Contacts")' \
              '))'

      Dante_Stage_ExistScoreNewSFDC_Contacts = liaison.ExpressionVDBImplGeneric(spec6)
      conn_mgr.setSpec('Dante_Stage_ExistScoreNewSFDC_Contacts', Dante_Stage_ExistScoreNewSFDC_Contacts.definition())

      # Spec7: Modify Dante_Stage_ExistScoreNewSFDC_Lead
      spec7 = 'SpecLatticeNamedElements(' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeFunction(' \
              '		LatticeFunctionExpressionTransform(' \
              '			LatticeFunctionExpression(' \
              '				LatticeFunctionOperatorIdentifier("IsNull"),' \
              '				LatticeFunctionExpressionTransform(' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("AND"),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("LessOrEqual"),' \
              '							LatticeFunctionIdentifier(' \
              '								ContainerElementNameTableQualifiedName(' \
              '									LatticeSourceTableIdentifier(' \
              '										ContainerElementName("Timestamp_PushToDante")' \
              '									),' \
              '									ContainerElementName("Time_OfCompletion_PushToDante")' \
              '								)' \
              '							),' \
              '							LatticeFunctionIdentifier(' \
              '								ContainerElementName("Time_OfMostRecentPushToDante")' \
              '							)' \
              '						),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("AND"),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("Equal"),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementNameTableQualifiedName(' \
              '										LatticeSourceTableIdentifier(' \
              '											ContainerElementName("SFDC_Lead")' \
              '										),' \
              '										ContainerElementName("CreatedDate")' \
              '									)' \
              '								),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementNameTableQualifiedName(' \
              '										LatticeSourceTableIdentifier(' \
              '											ContainerElementName("SFDC_Lead")' \
              '										),' \
              '										ContainerElementName("LastModifiedDate")' \
              '									)' \
              '								)' \
              '							),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("Greater"),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementNameTableQualifiedName(' \
              '										LatticeSourceTableIdentifier(' \
              '											ContainerElementName("Timestamp_DownloadedSFDC_Leads")' \
              '										),' \
              '										ContainerElementName("Time_OfComepletion_DownloadedSFDC_Leads")' \
              '									)' \
              '								),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementName("Time_OfMostRecentPushToDante")' \
              '								)' \
              '							)' \
              '						)' \
              '					),' \
              '					LatticeAddressSetPi(' \
              '						LatticeAddressExpressionAtomic(' \
              '							LatticeAddressAtomicIdentifier(' \
              '								ContainerElementName("SFDC_Lead_ID")' \
              '							)' \
              '						)' \
              '					),' \
              '					FunctionAggregationOperator("Max")' \
              '				),' \
              '				LatticeFunctionExpressionConstantScalar(' \
              '					"FALSE",' \
              '					DataTypeBit' \
              '				)' \
              '			),' \
              '			LatticeAddressSetPi(' \
              '				LatticeAddressExpressionAtomic(' \
              '					LatticeAddressAtomicIdentifier(' \
              '						ContainerElementName("ELQ_Contact_ContactID")' \
              '					)' \
              '				)' \
              '			),' \
              '			FunctionAggregationOperator("None")' \
              '		),' \
              '		DataTypeUnknown,' \
              '		SpecFunctionTypeMetric,' \
              '		SpecFunctionSourceTypeCalculation,' \
              '		SpecDefaultValueNull,' \
              '		SpecDescription("")' \
              '	),' \
              '	ContainerElementName("Dante_Stage_ExistScoreNewSFDC_Leads")' \
              '))'

      Dante_Stage_ExistScoreNewSFDC_Lead = liaison.ExpressionVDBImplGeneric(spec7)
      conn_mgr.setSpec('Dante_Stage_ExistScoreNewSFDC_Lead', Dante_Stage_ExistScoreNewSFDC_Lead.definition())

      # Spec8: Modify Dante_Stage_ExistScoreNewSFDC_Lead
      spec8 = 'SpecLatticeNamedElements(' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeFunction(' \
              '		LatticeFunctionExpressionTransform(' \
              '			LatticeFunctionExpression(' \
              '				LatticeFunctionOperatorIdentifier("AND"),' \
              '				LatticeFunctionExpression(' \
              '					LatticeFunctionOperatorIdentifier("Greater"),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("AddDay"),' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("Alias_ModifiedDate_Lead")' \
              '						),' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("Const_DaysOfDataForModeling")' \
              '						)' \
              '					),' \
              '					LatticeFunctionExpressionTransform(' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("Alias_ModifiedDate_Lead")' \
              '						),' \
              '						LatticeAddressSetPi(' \
              '							LatticeAddressExpressionAtomic(LatticeAddressAtomicAll)' \
              '						),' \
              '						FunctionAggregationOperator("None")' \
              '					)' \
              '				),' \
              '				LatticeFunctionExpression(' \
              '					LatticeFunctionOperatorIdentifier("Greater"),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("Minus"),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("ConvertToInt"),' \
              '							LatticeFunctionExpressionConstant(' \
              '								"Now",' \
              '								DataTypeDateTime' \
              '							)' \
              '						),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("IsNull"),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("ConvertToInt"),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementNameTableQualifiedName(' \
              '										LatticeSourceTableIdentifier(' \
              '											ContainerElementName("ELQ_Contact")' \
              '										),' \
              '										ContainerElementName("C_DateCreated")' \
              '									)' \
              '								)' \
              '							),' \
              '							LatticeFunctionExpressionConstantScalar(' \
              '								"0",' \
              '								DataTypeInt' \
              '							)' \
              '						)' \
              '					),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("Multiply"),' \
              '						LatticeFunctionExpressionConstant(' \
              '							"86400",' \
              '							DataTypeInt' \
              '						),' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("Const_DaysFromLeadCreationDate")' \
              '						)' \
              '					)' \
              '				)' \
              '			),' \
              '			LatticeAddressSetIdentifier(' \
              '				ContainerElementName("Alias_AllLeadTable")' \
              '			),' \
              '			FunctionAggregationOperator("Max")' \
              '		),' \
              '		DataTypeUnknown,' \
              '		SpecFunctionTypeMetric,' \
              '		SpecFunctionSourceTypeCalculation,' \
              '		SpecDefaultValueNull,' \
              '		SpecDescription("")' \
              '	),' \
              '	ContainerElementName("Lead_InDateRangeForModeling")' \
              '))'

      Lead_InDateRangeForModeling = liaison.ExpressionVDBImplGeneric(spec8)
      conn_mgr.setSpec('Lead_InDateRangeForModeling', Lead_InDateRangeForModeling.definition())

    else:
      spec1 = 'SpecLatticeNamedElements(SpecLatticeNamedElement(' \
              '	SpecLatticeFunction(' \
              '		LatticeFunctionExpressionConstantScalar(' \
              '			"90",' \
              '			DataTypeInt' \
              '		),' \
              '		DataTypeUnknown,' \
              '		SpecFunctionTypeMetric,' \
              '		SpecFunctionSourceTypeCalculation,' \
              '		SpecDefaultValueNull,' \
              '		SpecDescription("")' \
              '	),' \
              '	ContainerElementName("Const_DaysOfDataForScoringBulk")' \
              '))'

      Const_DaysOfDataForScoringBulk = liaison.ExpressionVDBImplGeneric(spec1)
      conn_mgr.setSpec('Const_DaysOfDataForScoringBulk', Const_DaysOfDataForScoringBulk.definition())

      # Spec2: Modify HAVING_DownloadedLeadNotMatched_PD
      spec2 = 'SpecLatticeNamedElements(SpecLatticeNamedElement(' \
              '	SpecLatticeAliasDeclaration(' \
              '		LatticeAddressSetIdentifier(' \
              '			ContainerElementName("HAVING_DownloadedLeadNotMatched_Timestamp")' \
              '		)' \
              '	),' \
              '	ContainerElementName("HAVING_DownloadedLeadNotMatched_PD")' \
              '))'

      HAVING_DownloadedLeadNotMatched_PD = liaison.ExpressionVDBImplGeneric(spec2)
      conn_mgr.setSpec('HAVING_DownloadedLeadNotMatched_PD', HAVING_DownloadedLeadNotMatched_PD.definition())

      # Spec3: Modify SelectedForScoringIncr
      spec3 = 'SpecLatticeNamedElements(SpecLatticeNamedElement(' \
              '	SpecLatticeAliasDeclaration(' \
              '		LatticeAddressSetIdentifier(' \
              '			ContainerElementName("SelectedForScoringIncr_Timestamp")' \
              '		)' \
              '	),' \
              '	ContainerElementName("SelectedForScoringIncr")' \
              '))'

      SelectedForScoringIncr = liaison.ExpressionVDBImplGeneric(spec3)
      conn_mgr.setSpec('SelectedForScoringIncr', SelectedForScoringIncr.definition())

      # Spec4: Modify SelectedForScoringIncr_Timestamp
      spec4 = 'SpecLatticeNamedElements(' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeAliasDeclaration(' \
              '		LatticeAddressSetFcn(' \
              '			LatticeFunctionExpression(' \
              '				LatticeFunctionOperatorIdentifier("IF"),' \
              '				LatticeFunctionIdentifier(' \
              '					ContainerElementName("Is_Contact")' \
              '				),' \
              '				LatticeFunctionExpression(' \
              '					LatticeFunctionOperatorIdentifier("IF"),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("Equal"),' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("Option_OlderContacts_NeedToBeScored")' \
              '						),' \
              '						LatticeFunctionExpressionConstant(' \
              '							"1",' \
              '							DataTypeInt' \
              '						)' \
              '					),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("OR"),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("IsNull"),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("Less"),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementName("SFDC_Contact_ID_RankForScoring_Contact")' \
              '								),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementName("Const_MaxNumberOfContactsToScore")' \
              '								)' \
              '							),' \
              '							LatticeFunctionExpressionConstant(' \
              '								"FALSE",' \
              '								DataTypeBit' \
              '							)' \
              '						),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("IsNull"),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("Less"),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementName("SFDC_Contact_ID_RankForScoring_OlderContact")' \
              '								),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementName("Const_NumberOfOlderContactsForScoring")' \
              '								)' \
              '							),' \
              '							LatticeFunctionExpressionConstant(' \
              '								"FALSE",' \
              '								DataTypeBit' \
              '							)' \
              '						)' \
              '					),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("IsNull"),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("Less"),' \
              '							LatticeFunctionIdentifier(' \
              '								ContainerElementName("SFDC_Contact_ID_RankForScoring_OlderContact")' \
              '							),' \
              '							LatticeFunctionIdentifier(' \
              '								ContainerElementName("Const_NumberOfOlderContactsForScoring")' \
              '							)' \
              '						),' \
              '						LatticeFunctionExpressionConstant(' \
              '							"FALSE",' \
              '							DataTypeBit' \
              '						)' \
              '					)' \
              '				),' \
              '				LatticeFunctionExpression(' \
              '					LatticeFunctionOperatorIdentifier("IF"),' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementName("Is_Lead")' \
              '					),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("IF"),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("Equal"),' \
              '							LatticeFunctionIdentifier(' \
              '								ContainerElementName("Option_OlderLeads_NeedToBeScored")' \
              '							),' \
              '							LatticeFunctionExpressionConstant(' \
              '								"1",' \
              '								DataTypeInt' \
              '							)' \
              '						),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("OR"),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("IsNull"),' \
              '								LatticeFunctionExpression(' \
              '									LatticeFunctionOperatorIdentifier("Less"),' \
              '									LatticeFunctionIdentifier(' \
              '										ContainerElementName("SFDC_Lead_ID_RankForScoring_OlderLead")' \
              '									),' \
              '									LatticeFunctionIdentifier(' \
              '										ContainerElementName("Const_NumberOfOlderLeadsForScoring")' \
              '									)' \
              '								),' \
              '								LatticeFunctionExpressionConstant(' \
              '									"FALSE",' \
              '									DataTypeBit' \
              '								)' \
              '							),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("IsNull"),' \
              '								LatticeFunctionExpression(' \
              '									LatticeFunctionOperatorIdentifier("Less"),' \
              '									LatticeFunctionIdentifier(' \
              '										ContainerElementName("SFDC_Lead_ID_RankForScoring_Lead")' \
              '									),' \
              '									LatticeFunctionIdentifier(' \
              '										ContainerElementName("Const_MaxNumberOfLeadsToScore")' \
              '									)' \
              '								),' \
              '								LatticeFunctionExpressionConstant(' \
              '									"FALSE",' \
              '									DataTypeBit' \
              '								)' \
              '							)' \
              '						),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("IsNull"),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("Less"),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementName("SFDC_Lead_ID_RankForScoring_Lead")' \
              '								),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementName("Const_MaxNumberOfLeadsToScore")' \
              '								)' \
              '							),' \
              '							LatticeFunctionExpressionConstant(' \
              '								"FALSE",' \
              '								DataTypeBit' \
              '							)' \
              '						)' \
              '					),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("NOT"),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("IsNullValue"),' \
              '							LatticeFunctionIdentifier(' \
              '								ContainerElementNameTableQualifiedName(' \
              '									LatticeSourceTableIdentifier(' \
              '										ContainerElementName("Sys_LatticeSystemID")' \
              '									),' \
              '									ContainerElementName("Lattice_System_ID")' \
              '								)' \
              '							)' \
              '						)' \
              '					)' \
              '				)' \
              '			),' \
              '			LatticeAddressSetPi(' \
              '				LatticeAddressExpressionAtomic(' \
              '					LatticeAddressAtomicIdentifier(' \
              '						ContainerElementName("SFDC_Lead_Contact_ID")' \
              '					)' \
              '				)' \
              '			)' \
              '		)' \
              '	),' \
              '	ContainerElementName("SelectedForScoringIncr_Timestamp")' \
              '))'

      SelectedForScoringIncr_Timestamp = liaison.ExpressionVDBImplGeneric(spec4)
      conn_mgr.setSpec('SelectedForScoringIncr_Timestamp', SelectedForScoringIncr_Timestamp.definition())

      # Spec5: Modify SelectedForPushingToDestination_Contact
      spec5 = 'SpecLatticeNamedElements(' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeAliasDeclaration(' \
              '		LatticeAddressSetFcn(' \
              '			LatticeFunctionExpression(' \
              '				LatticeFunctionOperatorIdentifier("AND"),' \
              '				LatticeFunctionIdentifier(' \
              '					ContainerElementName("Is_Contact")' \
              '				),' \
              '				LatticeFunctionExpression(' \
              '					LatticeFunctionOperatorIdentifier("Greater"),' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementNameTableQualifiedName(' \
              '							LatticeSourceTableIdentifier(' \
              '								ContainerElementName("Timestamp_LoadScoredLeads")' \
              '							),' \
              '							ContainerElementName("Time_OfCompletion_LoadScoredLeads")' \
              '						)' \
              '					),' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementName("Time_OfMostRecentPushToDestination")' \
              '					)' \
              '				)' \
              '			),' \
              '			LatticeAddressSetIdentifier(' \
              '				ContainerElementName("Alias_AllLeadTable")' \
              '			)' \
              '		)' \
              '	),' \
              '	ContainerElementName("SelectedForPushingToDestination_Contact")' \
              '))'

      SelectedForPushingToDestination_Contact = liaison.ExpressionVDBImplGeneric(spec5)
      conn_mgr.setSpec('SelectedForPushingToDestination_Contact', SelectedForPushingToDestination_Contact.definition())

      # Spec6: Modify SelectedForPushingToDestination_Lead
      spec6 = 'SpecLatticeNamedElements(' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeAliasDeclaration(' \
              '		LatticeAddressSetFcn(' \
              '			LatticeFunctionExpression(' \
              '				LatticeFunctionOperatorIdentifier("AND"),' \
              '				LatticeFunctionIdentifier(' \
              '					ContainerElementName("Is_Lead")' \
              '				),' \
              '				LatticeFunctionExpression(' \
              '					LatticeFunctionOperatorIdentifier("Greater"),' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementNameTableQualifiedName(' \
              '							LatticeSourceTableIdentifier(' \
              '								ContainerElementName("Timestamp_LoadScoredLeads")' \
              '							),' \
              '							ContainerElementName("Time_OfCompletion_LoadScoredLeads")' \
              '						)' \
              '					),' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementName("Time_OfMostRecentPushToDestination")' \
              '					)' \
              '				)' \
              '			),' \
              '			LatticeAddressSetIdentifier(' \
              '				ContainerElementName("Alias_AllLeadTable")' \
              '			)' \
              '		)' \
              '	),' \
              '	ContainerElementName("SelectedForPushingToDestination_Lead")' \
              '))'

      SelectedForPushingToDestination_Contact = liaison.ExpressionVDBImplGeneric(spec6)
      conn_mgr.setSpec('SelectedForPushingToDestination_Contact', SelectedForPushingToDestination_Contact.definition())

      # Spec6: Modify Dante_Stage_ExistScoreNewSFDC_Lead
      spec6 = 'SpecLatticeNamedElements(SpecLatticeNamedElement(' \
              '	SpecLatticeFunction(' \
              '		LatticeFunctionExpressionTransform(' \
              '			LatticeFunctionExpression(' \
              '				LatticeFunctionOperatorIdentifier("AND"),' \
              '				LatticeFunctionExpression(' \
              '					LatticeFunctionOperatorIdentifier("Greater"),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("AddDay"),' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("Alias_ModifiedDate_Lead")' \
              '						),' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("Const_DaysOfDataForModeling")' \
              '						)' \
              '					),' \
              '					LatticeFunctionExpressionTransform(' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("Alias_ModifiedDate_Lead")' \
              '						),' \
              '						LatticeAddressSetPi(' \
              '							LatticeAddressExpressionAtomic(LatticeAddressAtomicAll)' \
              '						),' \
              '						FunctionAggregationOperator("None")' \
              '					)' \
              '				),' \
              '				LatticeFunctionExpression(' \
              '					LatticeFunctionOperatorIdentifier("Greater"),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("Minus"),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("ConvertToInt"),' \
              '							LatticeFunctionExpressionConstant(' \
              '								"Now",' \
              '								DataTypeDateTime' \
              '							)' \
              '						),' \
              '						LatticeFunctionExpression(' \
              '							LatticeFunctionOperatorIdentifier("IsNull"),' \
              '							LatticeFunctionExpression(' \
              '								LatticeFunctionOperatorIdentifier("ConvertToInt"),' \
              '								LatticeFunctionIdentifier(' \
              '									ContainerElementName("Alias_CreatedDate_Lead")' \
              '								)' \
              '							),' \
              '							LatticeFunctionExpressionConstantScalar(' \
              '								"0",' \
              '								DataTypeInt' \
              '							)' \
              '						)' \
              '					),' \
              '					LatticeFunctionExpression(' \
              '						LatticeFunctionOperatorIdentifier("Multiply"),' \
              '						LatticeFunctionExpressionConstant(' \
              '							"86400",' \
              '							DataTypeInt' \
              '						),' \
              '						LatticeFunctionIdentifier(' \
              '							ContainerElementName("Const_DaysFromLeadCreationDate")' \
              '						)' \
              '					)' \
              '				)' \
              '			),' \
              '			LatticeAddressSetIdentifier(' \
              '				ContainerElementName("Alias_AllLeadTable")' \
              '			),' \
              '			FunctionAggregationOperator("Max")' \
              '		),' \
              '		DataTypeUnknown,' \
              '		SpecFunctionTypeMetric,' \
              '		SpecFunctionSourceTypeCalculation,' \
              '		SpecDefaultValueNull,' \
              '		SpecDescription("")' \
              '	),' \
              '	ContainerElementName("Lead_InDateRangeForModeling")' \
              '))'
      Lead_InDateRangeForModeling = liaison.ExpressionVDBImplGeneric(spec6)
      conn_mgr.setSpec('Lead_InDateRangeForModeling', Lead_InDateRangeForModeling.definition())

    success = True

    return success
