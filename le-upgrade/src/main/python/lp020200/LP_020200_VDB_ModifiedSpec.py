
#
# $LastChangedBy: lyan $
# $LastChangedDate: 2015-11-20 01:50:33 +0800 (Fri, 20 Nov 2015) $
# $Rev: 71049 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import liaison

class LP_020200_VDB_ModifiedSpec( StepBase ):
  
  name        = 'LP_020200_VDB_ModifiedSpec'
  description = 'Upgrade Modified Specs from 2.1.2 to 2.2.0'
  version     = '$Rev: 71049 $'
  def __init__( self, forceApply = False ):
    super( LP_020200_VDB_ModifiedSpec, self ).__init__( forceApply )

 ##Check SelectedForDante existig in Spec
  def getApplicability( self, appseq ):

      conn_mgr = appseq.getConnectionMgr()
      conn_mgr.getSpec("Const_DaysOfDataForScoringBulk")
      conn_mgr.getSpec("HAVING_DownloadedLeadNotMatched_PD")
      conn_mgr.getSpec("SelectedForScoringIncr")
      type = appseq.getText( 'template_type' )

      if type == 'MKTO':
        conn_mgr.getSpec("MKTO_LeadRecord_ID_ToBeScored")
        conn_mgr.getSpec("SelectedForPushToDestination_MKTO")

        if not conn_mgr.getSpec("Const_DaysOfDataForScoringBulk") \
           and conn_mgr.getSpec("HAVING_DownloadedLeadNotMatched_PD") \
           and conn_mgr.getSpec("SelectedForScoringIncr") \
           and conn_mgr.getSpec("MKTO_LeadRecord_ID_ToBeScored") \
           and conn_mgr.getSpec("SelectedForPushToDestination_MKTO"):
          return Applicability.cannotApplyFail

      elif type == 'ELQ':
        conn_mgr.getSpec("SelectedForPushToDestination_ELQ")
        conn_mgr.getSpec("SelectedForScoringIncr_Timestamp")
        if not conn_mgr.getSpec("Const_DaysOfDataForScoringBulk") \
           and conn_mgr.getSpec("HAVING_DownloadedLeadNotMatched_PD") \
           and conn_mgr.getSpec("SelectedForScoringIncr") \
           and conn_mgr.getSpec("SelectedForPushToDestination_ELQ")\
           and conn_mgr.getSpec("SelectedForScoringIncr_Timestamp"):
          return Applicability.cannotApplyFail

      else:
        conn_mgr.getSpec("SelectedForPushingToDestination_Contact")
        conn_mgr.getSpec("SelectedForPushingToDestination_Lead")
        conn_mgr.getSpec("SelectedForScoringIncr_Timestamp")
        if not conn_mgr.getSpec("Const_DaysOfDataForScoringBulk") \
           and conn_mgr.getSpec("HAVING_DownloadedLeadNotMatched_PD") \
           and conn_mgr.getSpec("SelectedForScoringIncr") \
           and conn_mgr.getSpec("SelectedForScoringIncr_Timestamp")\
           and conn_mgr.getSpec("SelectedForPushingToDestination_Contact")\
           and conn_mgr.getSpec("SelectedForPushingToDestination_Lead"):
          return Applicability.cannotApplyFail

      return  Applicability.canApply

   ##Check Time_OfMostRecentPushToDante existig in Spec
  def getApplicability( self, appseq ):

      conn_mgr = appseq.getConnectionMgr()
      conn_mgr.getSpec("Time_OfMostRecentPushToDante")

      if not conn_mgr.getSpec("Time_OfMostRecentPushToDante"):
        return Applicability.cannotApplyFail

      return  Applicability.canApply

  def apply( self, appseq ):

      success = False

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )

      if type== 'MKTO':
      # Spec1: Modify Const_DaysOfDataForScoringBulk
       spec1= 'SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '	SpecLatticeFunction('\
              '		LatticeFunctionExpressionConstantScalar('\
              '			"90",'\
              '			DataTypeInt'\
              '		),'\
              '		DataTypeUnknown,'\
              '		SpecFunctionTypeMetric,'\
              '		SpecFunctionSourceTypeCalculation,'\
              '		SpecDefaultValueNull,'\
              '		SpecDescription("")'\
              '	),'\
              '	ContainerElementName("Const_DaysOfDataForScoringBulk")'\
              '))'

       Const_DaysOfDataForScoringBulk=liaison.ExpressionVDBImplGeneric(spec1)
       conn_mgr.setSpec('Const_DaysOfDataForScoringBulk',Const_DaysOfDataForScoringBulk.definition())

      # Spec2: Modify HAVING_DownloadedLeadNotMatched_PD
       spec2= 'SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '	SpecLatticeAliasDeclaration('\
              '		LatticeAddressSetIdentifier('\
              '			ContainerElementName("HAVING_DownloadedLeadNotMatched_Timestamp")'\
              '		)'\
              '	),'\
              '	ContainerElementName("HAVING_DownloadedLeadNotMatched_PD")'\
              '))'

       HAVING_DownloadedLeadNotMatched_PD=liaison.ExpressionVDBImplGeneric(spec2)
       conn_mgr.setSpec('HAVING_DownloadedLeadNotMatched_PD',HAVING_DownloadedLeadNotMatched_PD.definition())

      # Spec3: Modify SelectedForScoringIncr
       spec3= 'SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '	SpecLatticeAliasDeclaration('\
              '		LatticeAddressSetIdentifier('\
              '			ContainerElementName("SelectedForScoringIncr_Timestamp")'\
              '		)'\
              '	),'\
              '	ContainerElementName("SelectedForScoringIncr")'\
              '))'

       SelectedForScoringIncr=liaison.ExpressionVDBImplGeneric(spec3)
       conn_mgr.setSpec('SelectedForScoringIncr',SelectedForScoringIncr.definition())


       # Spec4: Modify SelectedForScoringIncr
       spec4= 'SpecLatticeNamedElements('\
              'SpecLatticeNamedElement('\
              '	SpecLatticeFunction('\
              '		LatticeFunctionExpressionTransform('\
              '			LatticeFunctionExpression('\
              '				LatticeFunctionOperatorIdentifier("OR"),'\
              '				LatticeFunctionExpression('\
              '					LatticeFunctionOperatorIdentifier("Equal"),'\
              '					LatticeFunctionIdentifier('\
              '						ContainerElementNameTableQualifiedName('\
              '							LatticeSourceTableIdentifier('\
              '								ContainerElementName("Timestamp_DownloadedLeads")'\
              '							),'\
              '							ContainerElementName("MKTO_LeadRecord_ID")'\
              '						)'\
              '					),'\
              '					LatticeFunctionExpressionConstant('\
              '						"-111111.",'\
              '						DataTypeFloat'\
              '					)'\
              '				),'\
              '				LatticeFunctionExpression('\
              '					LatticeFunctionOperatorIdentifier("IF"),'\
              '					LatticeFunctionExpression('\
              '						LatticeFunctionOperatorIdentifier("Equal"),'\
              '						LatticeFunctionIdentifier('\
              '							ContainerElementName("Option_OlderLeads_NeedToBeScored")'\
              '						),'\
              '						LatticeFunctionExpressionConstant('\
              '							"1",'\
              '							DataTypeInt'\
              '						)'\
              '					),'\
              '					LatticeFunctionExpression('\
              '						LatticeFunctionOperatorIdentifier("OR"),'\
              '						LatticeFunctionIdentifier('\
              '							ContainerElementName("MKTO_LeadRecord_ID_ToBeScored_Incr")'\
              '						),'\
              '						LatticeFunctionIdentifier('\
              '							ContainerElementName("MKTO_LeadRecord_ID_ToBeScored_OlderLeads")'\
              '						)'\
              '					),'\
              '					LatticeFunctionIdentifier('\
              '						ContainerElementName("MKTO_LeadRecord_ID_ToBeScored_Incr")'\
              '					)'\
              '				)'\
              '			),'\
              '			LatticeAddressSetPi('\
              '				LatticeAddressExpressionAtomic('\
              '					LatticeAddressAtomicIdentifier('\
              '						ContainerElementName("MKTO_LeadRecord_ID")'\
              '					)'\
              '				)'\
              '			),'\
              '			FunctionAggregationOperator("Max")'\
              '		),'\
              '		DataTypeUnknown,'\
              '		SpecFunctionTypeMetric,'\
              '		SpecFunctionSourceTypeCalculation,'\
              '		SpecDefaultValueNull,'\
              '		SpecDescription("")'\
              '	),'\
              '	ContainerElementName("MKTO_LeadRecord_ID_ToBeScored")'\
              '))'

       MKTO_LeadRecord_ID_ToBeScored=liaison.ExpressionVDBImplGeneric(spec4)
       conn_mgr.setSpec('MKTO_LeadRecord_ID_ToBeScored',MKTO_LeadRecord_ID_ToBeScored.definition())

       # Spec5: Modify SelectedForPushToDestination_MKTO
       spec5='SpecLatticeNamedElements('\
              'SpecLatticeNamedElement('\
              '	SpecLatticeAliasDeclaration('\
              '		LatticeAddressSetFcn('\
              '			LatticeFunctionExpression('\
              '				LatticeFunctionOperatorIdentifier("Greater"),'\
              '				LatticeFunctionIdentifier('\
              '					ContainerElementNameTableQualifiedName('\
              '						LatticeSourceTableIdentifier('\
              '							ContainerElementName("Timestamp_LoadScoredLeads")'\
              '						),'\
              '						ContainerElementName("Time_OfCompletion_LoadScoredLeads")'\
              '					)'\
              '				),'\
              '				LatticeFunctionIdentifier('\
              '					ContainerElementName("Time_OfMostRecentPushToDestination")'\
              '				)'\
              '			),'\
              '			LatticeAddressSetIdentifier('\
              '				ContainerElementName("Alias_AllLeadTable")'\
              '			)'\
              '		)'\
              '	),'\
              '	ContainerElementName("SelectedForPushToDestination_MKTO")'\
              '))'

       SelectedForPushToDestination_MKTO=liaison.ExpressionVDBImplGeneric(spec5)
       conn_mgr.setSpec('SelectedForPushToDestination_MKTO',SelectedForPushToDestination_MKTO.definition())

      if type== 'ELQ':
        spec1='SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '	SpecLatticeFunction('\
              '		LatticeFunctionExpressionConstantScalar('\
              '			"90",'\
              '			DataTypeInt'\
              '		),'\
              '		DataTypeUnknown,'\
              '		SpecFunctionTypeMetric,'\
              '		SpecFunctionSourceTypeCalculation,'\
              '		SpecDefaultValueNull,'\
              '		SpecDescription("")'\
              '	),'\
              '	ContainerElementName("Const_DaysOfDataForScoringBulk")'\
              '))'
        Const_DaysOfDataForScoringBulk=liaison.ExpressionVDBImplGeneric(spec1)
        conn_mgr.setSpec('Const_DaysOfDataForScoringBulk',Const_DaysOfDataForScoringBulk.definition())

      # Spec2: Modify HAVING_DownloadedLeadNotMatched_PD
        spec2= 'SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '	SpecLatticeAliasDeclaration('\
              '		LatticeAddressSetIdentifier('\
              '			ContainerElementName("HAVING_DownloadedLeadNotMatched_Timestamp")'\
              '		)'\
              '	),'\
              '	ContainerElementName("HAVING_DownloadedLeadNotMatched_PD")'\
              '))'

        HAVING_DownloadedLeadNotMatched_PD=liaison.ExpressionVDBImplGeneric(spec2)
        conn_mgr.setSpec('HAVING_DownloadedLeadNotMatched_PD',HAVING_DownloadedLeadNotMatched_PD.definition())

      # Spec3: Modify SelectedForScoringIncr
        spec3= 'SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '	SpecLatticeAliasDeclaration('\
              '		LatticeAddressSetIdentifier('\
              '			ContainerElementName("SelectedForScoringIncr_Timestamp")'\
              '		)'\
              '	),'\
              '	ContainerElementName("SelectedForScoringIncr")'\
              '))'

        SelectedForScoringIncr=liaison.ExpressionVDBImplGeneric(spec3)
        conn_mgr.setSpec('SelectedForScoringIncr',SelectedForScoringIncr.definition())

      # Spec4: Modify SelectedForScoringIncr
        spec4='SpecLatticeNamedElements('\
              'SpecLatticeNamedElement('\
              '	SpecLatticeAliasDeclaration('\
              '		LatticeAddressSetFcn('\
              '			LatticeFunctionExpression('\
              '				LatticeFunctionOperatorIdentifier("IF"),'\
              '				LatticeFunctionExpression('\
              '					LatticeFunctionOperatorIdentifier("Equal"),'\
              '					LatticeFunctionIdentifier('\
              '						ContainerElementName("Option_OlderLeads_NeedToBeScored")'\
              '					),'\
              '					LatticeFunctionExpressionConstant('\
              '						"1",'\
              '						DataTypeInt'\
              '					)'\
              '				),'\
              '				LatticeFunctionExpression('\
              '					LatticeFunctionOperatorIdentifier("OR"),'\
              '					LatticeFunctionIdentifier('\
              '						ContainerElementName("ELQ_Contact_ContactID_ToBeScored_Incr")'\
              '					),'\
              '					LatticeFunctionIdentifier('\
              '						ContainerElementName("ELQ_Contact_ContactID_ToBeScored_OlderLeads")'\
              '					)'\
              '				),'\
              '				LatticeFunctionIdentifier('\
              '					ContainerElementName("ELQ_Contact_ContactID_ToBeScored_Incr")'\
              '				)'\
              '			),'\
              '			LatticeAddressSetIdentifier('\
              '				ContainerElementName("Alias_AllLeadID")'\
              '			)'\
              '		)'\
              '	),'\
              '	ContainerElementName("SelectedForScoringIncr_Timestamp")'\
              '))'

        SelectedForScoringIncr_Timestamp=liaison.ExpressionVDBImplGeneric(spec4)
        conn_mgr.setSpec('SelectedForScoringIncr_Timestamp',SelectedForScoringIncr_Timestamp.definition())

        # Spec5: Modify SelectedForPushToDestination_ELQ
        spec5='SpecLatticeNamedElements('\
              'SpecLatticeNamedElement('\
              '	SpecLatticeAliasDeclaration('\
              '		LatticeAddressSetFcn('\
              '			LatticeFunctionExpression('\
              '				LatticeFunctionOperatorIdentifier("Greater"),'\
              '				LatticeFunctionIdentifier('\
              '					ContainerElementNameTableQualifiedName('\
              '						LatticeSourceTableIdentifier('\
              '							ContainerElementName("Timestamp_LoadScoredLeads")'\
              '						),'\
              '						ContainerElementName("Time_OfCompletion_LoadScoredLeads")'\
              '					)'\
              '				),'\
              '				LatticeFunctionIdentifier('\
              '					ContainerElementName("Time_OfMostRecentPushToDestination")'\
              '				)'\
              '			),'\
              '			LatticeAddressSetIdentifier('\
              '				ContainerElementName("Alias_AllLeadTable")'\
              '			)'\
              '		)'\
              '	),'\
              '	ContainerElementName("SelectedForPushToDestination_ELQ")'\
              '))'
        SelectedForPushToDestination_ELQ=liaison.ExpressionVDBImplGeneric(spec5)
        conn_mgr.setSpec('SelectedForPushToDestination_ELQ',SelectedForPushToDestination_ELQ.definition())

      else:
        spec1='SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '	SpecLatticeFunction('\
              '		LatticeFunctionExpressionConstantScalar('\
              '			"90",'\
              '			DataTypeInt'\
              '		),'\
              '		DataTypeUnknown,'\
              '		SpecFunctionTypeMetric,'\
              '		SpecFunctionSourceTypeCalculation,'\
              '		SpecDefaultValueNull,'\
              '		SpecDescription("")'\
              '	),'\
              '	ContainerElementName("Const_DaysOfDataForScoringBulk")'\
              '))'

        Const_DaysOfDataForScoringBulk=liaison.ExpressionVDBImplGeneric(spec1)
        conn_mgr.setSpec('Const_DaysOfDataForScoringBulk',Const_DaysOfDataForScoringBulk.definition())

       # Spec2: Modify HAVING_DownloadedLeadNotMatched_PD
        spec2= 'SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '	SpecLatticeAliasDeclaration('\
              '		LatticeAddressSetIdentifier('\
              '			ContainerElementName("HAVING_DownloadedLeadNotMatched_Timestamp")'\
              '		)'\
              '	),'\
              '	ContainerElementName("HAVING_DownloadedLeadNotMatched_PD")'\
              '))'

        HAVING_DownloadedLeadNotMatched_PD=liaison.ExpressionVDBImplGeneric(spec2)
        conn_mgr.setSpec('HAVING_DownloadedLeadNotMatched_PD',HAVING_DownloadedLeadNotMatched_PD.definition())

        # Spec3: Modify SelectedForScoringIncr
        spec3= 'SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '	SpecLatticeAliasDeclaration('\
              '		LatticeAddressSetIdentifier('\
              '			ContainerElementName("SelectedForScoringIncr_Timestamp")'\
              '		)'\
              '	),'\
              '	ContainerElementName("SelectedForScoringIncr")'\
              '))'

        SelectedForScoringIncr=liaison.ExpressionVDBImplGeneric(spec3)
        conn_mgr.setSpec('SelectedForScoringIncr',SelectedForScoringIncr.definition())

        # Spec4: Modify SelectedForScoringIncr_Timestamp
        spec4='SpecLatticeNamedElements('\
              'SpecLatticeNamedElement('\
              '	SpecLatticeAliasDeclaration('\
              '		LatticeAddressSetFcn('\
              '			LatticeFunctionExpression('\
              '				LatticeFunctionOperatorIdentifier("IF"),'\
              '				LatticeFunctionIdentifier('\
              '					ContainerElementName("Is_Contact")'\
              '				),'\
              '				LatticeFunctionExpression('\
              '					LatticeFunctionOperatorIdentifier("IF"),'\
              '					LatticeFunctionExpression('\
              '						LatticeFunctionOperatorIdentifier("Equal"),'\
              '						LatticeFunctionIdentifier('\
              '							ContainerElementName("Option_OlderContacts_NeedToBeScored")'\
              '						),'\
              '						LatticeFunctionExpressionConstant('\
              '							"1",'\
              '							DataTypeInt'\
              '						)'\
              '					),'\
              '					LatticeFunctionExpression('\
              '						LatticeFunctionOperatorIdentifier("OR"),'\
              '						LatticeFunctionExpression('\
              '							LatticeFunctionOperatorIdentifier("IsNull"),'\
              '							LatticeFunctionExpression('\
              '								LatticeFunctionOperatorIdentifier("Less"),'\
              '								LatticeFunctionIdentifier('\
              '									ContainerElementName("SFDC_Contact_ID_RankForScoring_Contact")'\
              '								),'\
              '								LatticeFunctionIdentifier('\
              '									ContainerElementName("Const_MaxNumberOfContactsToScore")'\
              '								)'\
              '							),'\
              '							LatticeFunctionExpressionConstant('\
              '								"FALSE",'\
              '								DataTypeBit'\
              '							)'\
              '						),'\
              '						LatticeFunctionExpression('\
              '							LatticeFunctionOperatorIdentifier("IsNull"),'\
              '							LatticeFunctionExpression('\
              '								LatticeFunctionOperatorIdentifier("Less"),'\
              '								LatticeFunctionIdentifier('\
              '									ContainerElementName("SFDC_Contact_ID_RankForScoring_OlderContact")'\
              '								),'\
              '								LatticeFunctionIdentifier('\
              '									ContainerElementName("Const_NumberOfOlderContactsForScoring")'\
              '								)'\
              '							),'\
              '							LatticeFunctionExpressionConstant('\
              '								"FALSE",'\
              '								DataTypeBit'\
              '							)'\
              '						)'\
              '					),'\
              '					LatticeFunctionExpression('\
              '						LatticeFunctionOperatorIdentifier("IsNull"),'\
              '						LatticeFunctionExpression('\
              '							LatticeFunctionOperatorIdentifier("Less"),'\
              '							LatticeFunctionIdentifier('\
              '								ContainerElementName("SFDC_Contact_ID_RankForScoring_OlderContact")'\
              '							),'\
              '							LatticeFunctionIdentifier('\
              '								ContainerElementName("Const_NumberOfOlderContactsForScoring")'\
              '							)'\
              '						),'\
              '						LatticeFunctionExpressionConstant('\
              '							"FALSE",'\
              '							DataTypeBit'\
              '						)'\
              '					)'\
              '				),'\
              '				LatticeFunctionExpression('\
              '					LatticeFunctionOperatorIdentifier("IF"),'\
              '					LatticeFunctionIdentifier('\
              '						ContainerElementName("Is_Lead")'\
              '					),'\
              '					LatticeFunctionExpression('\
              '						LatticeFunctionOperatorIdentifier("IF"),'\
              '						LatticeFunctionExpression('\
              '							LatticeFunctionOperatorIdentifier("Equal"),'\
              '							LatticeFunctionIdentifier('\
              '								ContainerElementName("Option_OlderLeads_NeedToBeScored")'\
              '							),'\
              '							LatticeFunctionExpressionConstant('\
              '								"1",'\
              '								DataTypeInt'\
              '							)'\
              '						),'\
              '						LatticeFunctionExpression('\
              '							LatticeFunctionOperatorIdentifier("OR"),'\
              '							LatticeFunctionExpression('\
              '								LatticeFunctionOperatorIdentifier("IsNull"),'\
              '								LatticeFunctionExpression('\
              '									LatticeFunctionOperatorIdentifier("Less"),'\
              '									LatticeFunctionIdentifier('\
              '										ContainerElementName("SFDC_Lead_ID_RankForScoring_OlderLead")'\
              '									),'\
              '									LatticeFunctionIdentifier('\
              '										ContainerElementName("Const_NumberOfOlderLeadsForScoring")'\
              '									)'\
              '								),'\
              '								LatticeFunctionExpressionConstant('\
              '									"FALSE",'\
              '									DataTypeBit'\
              '								)'\
              '							),'\
              '							LatticeFunctionExpression('\
              '								LatticeFunctionOperatorIdentifier("IsNull"),'\
              '								LatticeFunctionExpression('\
              '									LatticeFunctionOperatorIdentifier("Less"),'\
              '									LatticeFunctionIdentifier('\
              '										ContainerElementName("SFDC_Lead_ID_RankForScoring_Lead")'\
              '									),'\
              '									LatticeFunctionIdentifier('\
              '										ContainerElementName("Const_MaxNumberOfLeadsToScore")'\
              '									)'\
              '								),'\
              '								LatticeFunctionExpressionConstant('\
              '									"FALSE",'\
              '									DataTypeBit'\
              '								)'\
              '							)'\
              '						),'\
              '						LatticeFunctionExpression('\
              '							LatticeFunctionOperatorIdentifier("IsNull"),'\
              '							LatticeFunctionExpression('\
              '								LatticeFunctionOperatorIdentifier("Less"),'\
              '								LatticeFunctionIdentifier('\
              '									ContainerElementName("SFDC_Lead_ID_RankForScoring_Lead")'\
              '								),'\
              '								LatticeFunctionIdentifier('\
              '									ContainerElementName("Const_MaxNumberOfLeadsToScore")'\
              '								)'\
              '							),'\
              '							LatticeFunctionExpressionConstant('\
              '								"FALSE",'\
              '								DataTypeBit'\
              '							)'\
              '						)'\
              '					),'\
              '					LatticeFunctionExpression('\
              '						LatticeFunctionOperatorIdentifier("NOT"),'\
              '						LatticeFunctionExpression('\
              '							LatticeFunctionOperatorIdentifier("IsNullValue"),'\
              '							LatticeFunctionIdentifier('\
              '								ContainerElementNameTableQualifiedName('\
              '									LatticeSourceTableIdentifier('\
              '										ContainerElementName("Sys_LatticeSystemID")'\
              '									),'\
              '									ContainerElementName("Lattice_System_ID")'\
              '								)'\
              '							)'\
              '						)'\
              '					)'\
              '				)'\
              '			),'\
              '			LatticeAddressSetPi('\
              '				LatticeAddressExpressionAtomic('\
              '					LatticeAddressAtomicIdentifier('\
              '						ContainerElementName("SFDC_Lead_Contact_ID")'\
              '					)'\
              '				)'\
              '			)'\
              '		)'\
              '	),'\
              '	ContainerElementName("SelectedForScoringIncr_Timestamp")'\
              '))'

        SelectedForScoringIncr_Timestamp=liaison.ExpressionVDBImplGeneric(spec4)
        conn_mgr.setSpec('SelectedForScoringIncr_Timestamp',SelectedForScoringIncr_Timestamp.definition())

        # Spec5: Modify SelectedForPushingToDestination_Contact
        spec5='SpecLatticeNamedElements('\
              'SpecLatticeNamedElement('\
              '	SpecLatticeAliasDeclaration('\
              '		LatticeAddressSetFcn('\
              '			LatticeFunctionExpression('\
              '				LatticeFunctionOperatorIdentifier("AND"),'\
              '				LatticeFunctionIdentifier('\
              '					ContainerElementName("Is_Contact")'\
              '				),'\
              '				LatticeFunctionExpression('\
              '					LatticeFunctionOperatorIdentifier("Greater"),'\
              '					LatticeFunctionIdentifier('\
              '						ContainerElementNameTableQualifiedName('\
              '							LatticeSourceTableIdentifier('\
              '								ContainerElementName("Timestamp_LoadScoredLeads")'\
              '							),'\
              '							ContainerElementName("Time_OfCompletion_LoadScoredLeads")'\
              '						)'\
              '					),'\
              '					LatticeFunctionIdentifier('\
              '						ContainerElementName("Time_OfMostRecentPushToDestination")'\
              '					)'\
              '				)'\
              '			),'\
              '			LatticeAddressSetIdentifier('\
              '				ContainerElementName("Alias_AllLeadTable")'\
              '			)'\
              '		)'\
              '	),'\
              '	ContainerElementName("SelectedForPushingToDestination_Contact")'\
              '))'

        SelectedForPushingToDestination_Contact=liaison.ExpressionVDBImplGeneric(spec5)
        conn_mgr.setSpec('SelectedForPushingToDestination_Contact',SelectedForPushingToDestination_Contact.definition())

        # Spec6: Modify SelectedForPushingToDestination_Lead
        spec6='SpecLatticeNamedElements('\
              'SpecLatticeNamedElement('\
              '	SpecLatticeAliasDeclaration('\
              '		LatticeAddressSetFcn('\
              '			LatticeFunctionExpression('\
              '				LatticeFunctionOperatorIdentifier("AND"),'\
              '				LatticeFunctionIdentifier('\
              '					ContainerElementName("Is_Lead")'\
              '				),'\
              '				LatticeFunctionExpression('\
              '					LatticeFunctionOperatorIdentifier("Greater"),'\
              '					LatticeFunctionIdentifier('\
              '						ContainerElementNameTableQualifiedName('\
              '							LatticeSourceTableIdentifier('\
              '								ContainerElementName("Timestamp_LoadScoredLeads")'\
              '							),'\
              '							ContainerElementName("Time_OfCompletion_LoadScoredLeads")'\
              '						)'\
              '					),'\
              '					LatticeFunctionIdentifier('\
              '						ContainerElementName("Time_OfMostRecentPushToDestination")'\
              '					)'\
              '				)'\
              '			),'\
              '			LatticeAddressSetIdentifier('\
              '				ContainerElementName("Alias_AllLeadTable")'\
              '			)'\
              '		)'\
              '	),'\
              '	ContainerElementName("SelectedForPushingToDestination_Lead")'\
              '))'

        SelectedForPushingToDestination_Contact=liaison.ExpressionVDBImplGeneric(spec6)
        conn_mgr.setSpec('SelectedForPushingToDestination_Contact',SelectedForPushingToDestination_Contact.definition())

      success = True

      return success






