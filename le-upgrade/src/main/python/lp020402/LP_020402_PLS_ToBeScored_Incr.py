#
# $LastChangedBy: lyan $
# $LastChangedDate: 2015-11-20 01:50:33 +0800 (Fri, 20 Nov 2015) $
# $Rev: 71049 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import liaison


class LP_020402_PLS_ToBeScored_Incr(StepBase):
  name = 'LP_020200_VDB_ModifiedSpec'
  description = 'Upgrade Modified Specs from 2.1.2 to 2.2.0'
  version = '$Rev: 71049 $'

  def __init__(self, forceApply=False):
    super(LP_020402_PLS_ToBeScored_Incr, self).__init__(forceApply)


  def getApplicability(self, appseq):

    conn_mgr = appseq.getConnectionMgr()
    type = appseq.getText('template_type')

    if type == 'MKTO':
      conn_mgr.getSpec("MKTO_LeadRecord_ID_ToBeScored_Incr")
      if not conn_mgr.getSpec("MKTO_LeadRecord_ID_ToBeScored_Incr"):
        return Applicability.cannotApplyFail

    elif type == 'ELQ':
      conn_mgr.getSpec("ELQ_Contact_ContactID_ToBeScored_Incr")
      if not conn_mgr.getSpec("ELQ_Contact_ContactID_ToBeScored_Incr"):
        return Applicability.cannotApplyFail

    else:
      conn_mgr.getSpec("SFDC_Lead_ID_ToBeScored_Lead")
      conn_mgr.getSpec("SFDC_Contact_ID_ToBeScored_Contact")
      if not conn_mgr.getSpec("SFDC_Lead_ID_ToBeScored_Lead") \
        and not conn_mgr.getSpec("SFDC_Contact_ID_ToBeScored_Contact"):
        return Applicability.cannotApplyFail

    return Applicability.canApply

  def apply(self, appseq):

    success = False

    conn_mgr = appseq.getConnectionMgr()
    type = appseq.getText('template_type')

    if type == 'MKTO':
      # Spec1: Modify MKTO_LeadRecord_ID_ToBeScored_Incr
      spec1 =   'SpecLatticeNamedElements(SpecLatticeNamedElement('\
                '	SpecLatticeFunction('\
                '		LatticeFunctionExpressionTransform('\
                '			LatticeFunctionExpression('\
                '				LatticeFunctionOperatorIdentifier("AND"),'\
                '				LatticeFunctionExpression('\
                '					LatticeFunctionOperatorIdentifier("AND"),'\
                '					LatticeFunctionExpression('\
                '						LatticeFunctionOperatorIdentifier("Greater"),'\
                '						LatticeFunctionIdentifier('\
                '							ContainerElementNameTableQualifiedName('\
                '								LatticeSourceTableIdentifier('\
                '									ContainerElementName("Timestamp_DownloadedLeads")'\
                '								),'\
                '								ContainerElementName("Time_OfCompletion_DownloadedLeads")'\
                '							)'\
                '						),'\
                '						LatticeFunctionIdentifier('\
                '							ContainerElementName("Time_OfMostRecentScore")'\
                '						)'\
                '					),'\
                '					LatticeFunctionExpression('\
                '						LatticeFunctionOperatorIdentifier("NOT"),'\
                '						LatticeFunctionExpression('\
                '							LatticeFunctionOperatorIdentifier("IsNullValue"),'\
                '							LatticeFunctionIdentifier('\
                '								ContainerElementNameTableQualifiedName('\
                '									LatticeSourceTableIdentifier('\
                '										ContainerElementName("MKTO_LeadRecord")'\
                '									),'\
                '									ContainerElementName("Email")'\
                '								)'\
                '							)'\
                '						)'\
                '					)'\
                '				),'\
                '				LatticeFunctionExpression('\
                '					LatticeFunctionOperatorIdentifier("OR"),'\
                '					LatticeFunctionExpression('\
                '						LatticeFunctionOperatorIdentifier("OR"),'\
                '						LatticeFunctionExpression('\
                '							LatticeFunctionOperatorIdentifier("IsNullValue"),'\
                '							LatticeFunctionIdentifier('\
                '								ContainerElementNameTableQualifiedName('\
                '									LatticeSourceTableIdentifier('\
                '										ContainerElementName("Timestamp_PushToScoring")'\
                '									),'\
                '									ContainerElementName("MKTO_LeadRecord_ID")'\
                '								)'\
                '							)'\
                '						),'\
                '						LatticeFunctionExpression('\
                '							LatticeFunctionOperatorIdentifier("Greater"),'\
                '							LatticeFunctionExpression('\
                '								LatticeFunctionOperatorIdentifier("Minus"),'\
                '								LatticeFunctionExpression('\
                '									LatticeFunctionOperatorIdentifier("ConvertToInt"),'\
                '									LatticeFunctionExpressionConstant('\
                '										"Now",'\
                '										DataTypeDateTime'\
                '									)'\
                '								),'\
                '								LatticeFunctionExpression('\
                '									LatticeFunctionOperatorIdentifier("IsNull"),'\
                '									LatticeFunctionExpression('\
                '										LatticeFunctionOperatorIdentifier("ConvertToInt"),'\
                '										LatticeFunctionIdentifier('\
                '											ContainerElementNameTableQualifiedName('\
                '												LatticeSourceTableIdentifier('\
                '													ContainerElementName("Timestamp_PushToScoring")'\
                '												),'\
                '												ContainerElementName("Time_OfSubmission_PushToScoring")'\
                '											)'\
                '										)'\
                '									),'\
                '									LatticeFunctionExpressionConstantScalar('\
                '										"0",'\
                '										DataTypeInt'\
                '									)'\
                '								)'\
                '							),'\
                '							LatticeFunctionExpression('\
                '								LatticeFunctionOperatorIdentifier("Multiply"),'\
                '								LatticeFunctionExpressionConstant('\
                '									"86400",'\
                '									DataTypeInt'\
                '								),'\
                '								LatticeFunctionIdentifier('\
                '									ContainerElementName("Const_DaysOfValidityForScore")'\
                '								)'\
                '							)'\
                '						)'\
                '					),'\
                '					LatticeFunctionExpression('\
                '						LatticeFunctionOperatorIdentifier("AND"),'\
                '						LatticeFunctionExpression('\
                '							LatticeFunctionOperatorIdentifier("NotEqual"),'\
                '							LatticeFunctionIdentifier('\
                '								ContainerElementName("Alias_ID_Lead")'\
                '							),'\
                '							LatticeFunctionExpressionConstantScalar('\
                '								"-111111",'\
                '								DataTypeNVarChar(7)'\
                '							)'\
                '						),'\
                '						LatticeFunctionExpression('\
                '							LatticeFunctionOperatorIdentifier("AND"),'\
                '							LatticeFunctionExpression('\
                '								LatticeFunctionOperatorIdentifier("GreaterOrEqual"),'\
                '								LatticeFunctionExpressionConstant('\
                '									"Now",'\
                '									DataTypeDateTime'\
                '								),'\
                '								LatticeFunctionExpression('\
                '									LatticeFunctionOperatorIdentifier("AddHour"),'\
                '									LatticeFunctionExpression('\
                '										LatticeFunctionOperatorIdentifier("IsNull"),'\
                '										LatticeFunctionIdentifier('\
                '											ContainerElementNameTableQualifiedName('\
                '												LatticeSourceTableIdentifier('\
                '													ContainerElementName("Timestamp_PushToScoring")'\
                '												),'\
                '												ContainerElementName("Time_OfSubmission_PushToScoring")'\
                '											)'\
                '										),'\
                '										LatticeFunctionExpressionConstantScalar('\
                '											"1900/01/01",'\
                '											DataTypeDateTime'\
                '										)'\
                '									),'\
                '									LatticeFunctionExpressionConstantScalar('\
                '										"24",'\
                '										DataTypeInt'\
                '									)'\
                '								)'\
                '							),'\
                '							LatticeFunctionExpression('\
                '								LatticeFunctionOperatorIdentifier("Equal"),'\
                '								LatticeFunctionIdentifier('\
                '									ContainerElementName("Lead_Status")'\
                '								),'\
                '								LatticeFunctionExpressionConstantScalar('\
                '									"1",'\
                '									DataTypeInt'\
                '								)'\
                '							)'\
                '						)'\
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
                '	ContainerElementName("MKTO_LeadRecord_ID_ToBeScored_Incr")'\
                '))'

      MKTO_LeadRecord_ID_ToBeScored_Incr = liaison.ExpressionVDBImplGeneric(spec1)
      conn_mgr.setSpec('MKTO_LeadRecord_ID_ToBeScored_Incr', MKTO_LeadRecord_ID_ToBeScored_Incr.definition())

    elif type == 'ELQ':
      spec1 =  'SpecLatticeNamedElements(SpecLatticeNamedElement('\
                '	SpecLatticeFunction('\
                '		LatticeFunctionExpressionTransform('\
                '			LatticeFunctionExpression('\
                '				LatticeFunctionOperatorIdentifier("OR"),'\
                '				LatticeFunctionExpression('\
                '					LatticeFunctionOperatorIdentifier("OR"),'\
                '					LatticeFunctionExpression('\
                '						LatticeFunctionOperatorIdentifier("IsNull"),'\
                '						LatticeFunctionExpression('\
                '							LatticeFunctionOperatorIdentifier("Less"),'\
                '							LatticeFunctionIdentifier('\
                '								ContainerElementName("ELQ_Contact_ContactID_RankForScoring")'\
                '							),'\
                '							LatticeFunctionIdentifier('\
                '								ContainerElementName("Max_MaxNumberOfLeadsToScore")'\
                '							)'\
                '						),'\
                '						LatticeFunctionExpressionConstant('\
                '							"FALSE",'\
                '							DataTypeBit'\
                '						)'\
                '					),'\
                '					LatticeFunctionExpression('\
                '						LatticeFunctionOperatorIdentifier("AND"),'\
                '						LatticeFunctionExpression('\
                '							LatticeFunctionOperatorIdentifier("NotEqual"),'\
                '							LatticeFunctionIdentifier('\
                '								ContainerElementName("Alias_ID_Lead")'\
                '							),'\
                '							LatticeFunctionExpressionConstantScalar('\
                '								"-111111",'\
                '								DataTypeNVarChar(7)'\
                '							)'\
                '						),'\
                '						LatticeFunctionExpression('\
                '							LatticeFunctionOperatorIdentifier("AND"),'\
                '							LatticeFunctionExpression('\
                '								LatticeFunctionOperatorIdentifier("GreaterOrEqual"),'\
                '								LatticeFunctionExpressionConstant('\
                '									"Now",'\
                '									DataTypeDateTime'\
                '								),'\
                '								LatticeFunctionExpression('\
                '									LatticeFunctionOperatorIdentifier("AddHour"),'\
                '									LatticeFunctionExpression('\
                '										LatticeFunctionOperatorIdentifier("IsNull"),'\
                '										LatticeFunctionIdentifier('\
                '											ContainerElementNameTableQualifiedName('\
                '												LatticeSourceTableIdentifier('\
                '													ContainerElementName("Timestamp_PushToScoring")'\
                '												),'\
                '												ContainerElementName("Time_OfSubmission_PushToScoring")'\
                '											)'\
                '										),'\
                '										LatticeFunctionExpressionConstantScalar('\
                '											"1900/01/01",'\
                '											DataTypeDateTime'\
                '										)'\
                '									),'\
                '									LatticeFunctionExpressionConstantScalar('\
                '										"24",'\
                '										DataTypeInt'\
                '									)'\
                '								)'\
                '							),'\
                '							LatticeFunctionExpression('\
                '								LatticeFunctionOperatorIdentifier("Equal"),'\
                '								LatticeFunctionIdentifier('\
                '									ContainerElementName("Lead_Status")'\
                '								),'\
                '								LatticeFunctionExpressionConstantScalar('\
                '									"1",'\
                '									DataTypeInt'\
                '								)'\
                '							)'\
                '						)'\
                '					)'\
                '				),'\
                '				LatticeFunctionExpression('\
                '					LatticeFunctionOperatorIdentifier("NOT"),'\
                '					LatticeFunctionExpression('\
                '						LatticeFunctionOperatorIdentifier("IsNullValue"),'\
                '						LatticeFunctionIdentifier('\
                '							ContainerElementNameTableQualifiedName('\
                '								LatticeSourceTableIdentifier('\
                '									ContainerElementName("Sys_LatticeSystemID")'\
                '								),'\
                '								ContainerElementName("Lattice_System_ID")'\
                '							)'\
                '						)'\
                '					)'\
                '				)'\
                '			),'\
                '			LatticeAddressSetPi('\
                '				LatticeAddressExpressionAtomic('\
                '					LatticeAddressAtomicIdentifier('\
                '						ContainerElementName("ELQ_Contact_ContactID")'\
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
                '	ContainerElementName("ELQ_Contact_ContactID_ToBeScored_Incr")'\
                '))'
      ELQ_Contact_ContactID_ToBeScored_Incr = liaison.ExpressionVDBImplGeneric(spec1)
      conn_mgr.setSpec('ELQ_Contact_ContactID_ToBeScored_Incr', ELQ_Contact_ContactID_ToBeScored_Incr.definition())

    else:
      spec1 =   'SpecLatticeNamedElements(SpecLatticeNamedElement('\
                '	SpecLatticeFunction('\
                '		LatticeFunctionExpressionTransform('\
                '			LatticeFunctionExpression('\
                '				LatticeFunctionOperatorIdentifier("AND"),'\
                '				LatticeFunctionExpression('\
                '					LatticeFunctionOperatorIdentifier("Greater"),'\
                '					LatticeFunctionIdentifier('\
                '						ContainerElementNameTableQualifiedName('\
                '							LatticeSourceTableIdentifier('\
                '								ContainerElementName("Timestamp_Downloaded_Lead_Contact")'\
                '							),'\
                '							ContainerElementName("Time_OfCompletion_DownloadedLeads")'\
                '						)'\
                '					),'\
                '					LatticeFunctionIdentifier('\
                '						ContainerElementName("Time_OfMostRecentScore")'\
                '					)'\
                '				),'\
                '				LatticeFunctionExpression('\
                '					LatticeFunctionOperatorIdentifier("OR"),'\
                '					LatticeFunctionExpression('\
                '						LatticeFunctionOperatorIdentifier("Equal"),'\
                '						LatticeFunctionIdentifier('\
                '							ContainerElementNameTableQualifiedName('\
                '								LatticeSourceTableIdentifier('\
                '									ContainerElementName("Timestamp_Downloaded_Lead_Contact")'\
                '								),'\
                '								ContainerElementName("SFDC_Lead_Contact_ID")'\
                '							)'\
                '						),'\
                '						LatticeFunctionExpressionConstant('\
                '							"Lattice System",'\
                '							DataTypeVarChar(14)'\
                '						)'\
                '					),'\
                '					LatticeFunctionExpression('\
                '						LatticeFunctionOperatorIdentifier("AND"),'\
                '						LatticeFunctionExpression('\
                '							LatticeFunctionOperatorIdentifier("NOT"),'\
                '							LatticeFunctionIdentifier('\
                '								ContainerElementName("SFDC_Contact_Email_IsAnonymous_Contact")'\
                '							)'\
                '						),'\
                '						LatticeFunctionExpression('\
                '							LatticeFunctionOperatorIdentifier("OR"),'\
                '							LatticeFunctionExpression('\
                '								LatticeFunctionOperatorIdentifier("OR"),'\
                '								LatticeFunctionExpression('\
                '									LatticeFunctionOperatorIdentifier("IsNullValue"),'\
                '									LatticeFunctionIdentifier('\
                '										ContainerElementNameTableQualifiedName('\
                '											LatticeSourceTableIdentifier('\
                '												ContainerElementName("Timestamp_PushToScoring")'\
                '											),'\
                '											ContainerElementName("SFDC_Lead_Contact_ID")'\
                '										)'\
                '									)'\
                '								),'\
                '								LatticeFunctionExpression('\
                '									LatticeFunctionOperatorIdentifier("Greater"),'\
                '									LatticeFunctionExpression('\
                '										LatticeFunctionOperatorIdentifier("Minus"),'\
                '										LatticeFunctionExpression('\
                '											LatticeFunctionOperatorIdentifier("ConvertToInt"),'\
                '											LatticeFunctionExpressionConstant('\
                '												"Now",'\
                '												DataTypeDateTime'\
                '											)'\
                '										),'\
                '										LatticeFunctionExpression('\
                '											LatticeFunctionOperatorIdentifier("IsNull"),'\
                '											LatticeFunctionExpression('\
                '												LatticeFunctionOperatorIdentifier("ConvertToInt"),'\
                '												LatticeFunctionIdentifier('\
                '													ContainerElementNameTableQualifiedName('\
                '														LatticeSourceTableIdentifier('\
                '															ContainerElementName("Timestamp_PushToScoring")'\
                '														),'\
                '														ContainerElementName("Time_OfSubmission_PushToScoring")'\
                '													)'\
                '												)'\
                '											),'\
                '											LatticeFunctionExpressionConstantScalar('\
                '												"0",'\
                '												DataTypeInt'\
                '											)'\
                '										)'\
                '									),'\
                '									LatticeFunctionExpression('\
                '										LatticeFunctionOperatorIdentifier("Multiply"),'\
                '										LatticeFunctionExpressionConstant('\
                '											"86400",'\
                '											DataTypeInt'\
                '										),'\
                '										LatticeFunctionIdentifier('\
                '											ContainerElementName("Const_DaysOfValidityForScore")'\
                '										)'\
                '									)'\
                '								)'\
                '							),'\
                '							LatticeFunctionExpression('\
                '								LatticeFunctionOperatorIdentifier("AND"),'\
                '								LatticeFunctionExpression('\
                '									LatticeFunctionOperatorIdentifier("NotEqual"),'\
                '									LatticeFunctionIdentifier('\
                '										ContainerElementName("Alias_ID_Lead")'\
                '									),'\
                '									LatticeFunctionExpressionConstantScalar('\
                '										"-111111",'\
                '										DataTypeNVarChar(7)'\
                '									)'\
                '								),'\
                '								LatticeFunctionExpression('\
                '									LatticeFunctionOperatorIdentifier("AND"),'\
                '									LatticeFunctionExpression('\
                '										LatticeFunctionOperatorIdentifier("GreaterOrEqual"),'\
                '										LatticeFunctionExpressionConstant('\
                '											"Now",'\
                '											DataTypeDateTime'\
                '										),'\
                '										LatticeFunctionExpression('\
                '											LatticeFunctionOperatorIdentifier("AddHour"),'\
                '											LatticeFunctionExpression('\
                '												LatticeFunctionOperatorIdentifier("IsNull"),'\
                '												LatticeFunctionIdentifier('\
                '													ContainerElementNameTableQualifiedName('\
                '														LatticeSourceTableIdentifier('\
                '															ContainerElementName("Timestamp_PushToScoring")'\
                '														),'\
                '														ContainerElementName("Time_OfSubmission_PushToScoring")'\
                '													)'\
                '												),'\
                '												LatticeFunctionExpressionConstantScalar('\
                '													"1900/01/01",'\
                '													DataTypeDateTime'\
                '												)'\
                '											),'\
                '											LatticeFunctionExpressionConstantScalar('\
                '												"24",'\
                '												DataTypeInt'\
                '											)'\
                '										)'\
                '									),'\
                '									LatticeFunctionExpression('\
                '										LatticeFunctionOperatorIdentifier("Equal"),'\
                '										LatticeFunctionIdentifier('\
                '											ContainerElementName("Lead_Status")'\
                '										),'\
                '										LatticeFunctionExpressionConstantScalar('\
                '											"1",'\
                '											DataTypeInt'\
                '										)'\
                '									)'\
                '								)'\
                '							)'\
                '						)'\
                '					)'\
                '				)'\
                '			),'\
                '			LatticeAddressSetPi('\
                '				LatticeAddressExpressionAtomic('\
                '					LatticeAddressAtomicIdentifier('\
                '						ContainerElementName("SFDC_Contact")'\
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
                '	ContainerElementName("SFDC_Contact_ID_ToBeScored_Contact")'\
                '))'

      SFDC_Contact_ID_ToBeScored_Contact = liaison.ExpressionVDBImplGeneric(spec1)
      conn_mgr.setSpec('SFDC_Contact_ID_ToBeScored_Contact', SFDC_Contact_ID_ToBeScored_Contact.definition())

      # Spec2: Modify SFDC_Lead_ID_ToBeScored_Lead
      spec2 =  'SpecLatticeNamedElements(SpecLatticeNamedElement('\
                '	SpecLatticeFunction('\
                '		LatticeFunctionExpressionTransform('\
                '			LatticeFunctionExpression('\
                '				LatticeFunctionOperatorIdentifier("AND"),'\
                '				LatticeFunctionExpression('\
                '					LatticeFunctionOperatorIdentifier("Greater"),'\
                '					LatticeFunctionIdentifier('\
                '						ContainerElementNameTableQualifiedName('\
                '							LatticeSourceTableIdentifier('\
                '								ContainerElementName("Timestamp_Downloaded_Lead_Contact")'\
                '							),'\
                '							ContainerElementName("Time_OfCompletion_DownloadedLeads")'\
                '						)'\
                '					),'\
                '					LatticeFunctionIdentifier('\
                '						ContainerElementName("Time_OfMostRecentScore")'\
                '					)'\
                '				),'\
                '				LatticeFunctionExpression('\
                '					LatticeFunctionOperatorIdentifier("OR"),'\
                '					LatticeFunctionExpression('\
                '						LatticeFunctionOperatorIdentifier("Equal"),'\
                '						LatticeFunctionIdentifier('\
                '							ContainerElementNameTableQualifiedName('\
                '								LatticeSourceTableIdentifier('\
                '									ContainerElementName("Timestamp_Downloaded_Lead_Contact")'\
                '								),'\
                '								ContainerElementName("SFDC_Lead_Contact_ID")'\
                '							)'\
                '						),'\
                '						LatticeFunctionExpressionConstant('\
                '							"Lattice System",'\
                '							DataTypeVarChar(14)'\
                '						)'\
                '					),'\
                '					LatticeFunctionExpression('\
                '						LatticeFunctionOperatorIdentifier("AND"),'\
                '						LatticeFunctionExpression('\
                '							LatticeFunctionOperatorIdentifier("NOT"),'\
                '							LatticeFunctionIdentifier('\
                '								ContainerElementName("SFDC_Lead_Email_IsAnonymous_Lead")'\
                '							)'\
                '						),'\
                '						LatticeFunctionExpression('\
                '							LatticeFunctionOperatorIdentifier("OR"),'\
                '							LatticeFunctionExpression('\
                '								LatticeFunctionOperatorIdentifier("OR"),'\
                '								LatticeFunctionExpression('\
                '									LatticeFunctionOperatorIdentifier("IsNullValue"),'\
                '									LatticeFunctionIdentifier('\
                '										ContainerElementNameTableQualifiedName('\
                '											LatticeSourceTableIdentifier('\
                '												ContainerElementName("Timestamp_PushToScoring")'\
                '											),'\
                '											ContainerElementName("SFDC_Lead_Contact_ID")'\
                '										)'\
                '									)'\
                '								),'\
                '								LatticeFunctionExpression('\
                '									LatticeFunctionOperatorIdentifier("Greater"),'\
                '									LatticeFunctionExpression('\
                '										LatticeFunctionOperatorIdentifier("Minus"),'\
                '										LatticeFunctionExpression('\
                '											LatticeFunctionOperatorIdentifier("ConvertToInt"),'\
                '											LatticeFunctionExpressionConstant('\
                '												"Now",'\
                '												DataTypeDateTime'\
                '											)'\
                '										),'\
                '										LatticeFunctionExpression('\
                '											LatticeFunctionOperatorIdentifier("IsNull"),'\
                '											LatticeFunctionExpression('\
                '												LatticeFunctionOperatorIdentifier("ConvertToInt"),'\
                '												LatticeFunctionIdentifier('\
                '													ContainerElementNameTableQualifiedName('\
                '														LatticeSourceTableIdentifier('\
                '															ContainerElementName("Timestamp_PushToScoring")'\
                '														),'\
                '														ContainerElementName("Time_OfSubmission_PushToScoring")'\
                '													)'\
                '												)'\
                '											),'\
                '											LatticeFunctionExpressionConstantScalar('\
                '												"0",'\
                '												DataTypeInt'\
                '											)'\
                '										)'\
                '									),'\
                '									LatticeFunctionExpression('\
                '										LatticeFunctionOperatorIdentifier("Multiply"),'\
                '										LatticeFunctionExpressionConstant('\
                '											"86400",'\
                '											DataTypeInt'\
                '										),'\
                '										LatticeFunctionIdentifier('\
                '											ContainerElementName("Const_DaysOfValidityForScore")'\
                '										)'\
                '									)'\
                '								)'\
                '							),'\
                '							LatticeFunctionExpression('\
                '								LatticeFunctionOperatorIdentifier("AND"),'\
                '								LatticeFunctionExpression('\
                '									LatticeFunctionOperatorIdentifier("NotEqual"),'\
                '									LatticeFunctionIdentifier('\
                '										ContainerElementName("Alias_ID_Lead")'\
                '									),'\
                '									LatticeFunctionExpressionConstantScalar('\
                '										"-111111",'\
                '										DataTypeNVarChar(7)'\
                '									)'\
                '								),'\
                '								LatticeFunctionExpression('\
                '									LatticeFunctionOperatorIdentifier("AND"),'\
                '									LatticeFunctionExpression('\
                '										LatticeFunctionOperatorIdentifier("GreaterOrEqual"),'\
                '										LatticeFunctionExpressionConstant('\
                '											"Now",'\
                '											DataTypeDateTime'\
                '										),'\
                '										LatticeFunctionExpression('\
                '											LatticeFunctionOperatorIdentifier("AddHour"),'\
                '											LatticeFunctionExpression('\
                '												LatticeFunctionOperatorIdentifier("IsNull"),'\
                '												LatticeFunctionIdentifier('\
                '													ContainerElementNameTableQualifiedName('\
                '														LatticeSourceTableIdentifier('\
                '															ContainerElementName("Timestamp_PushToScoring")'\
                '														),'\
                '														ContainerElementName("Time_OfSubmission_PushToScoring")'\
                '													)'\
                '												),'\
                '												LatticeFunctionExpressionConstantScalar('\
                '													"1900/01/01",'\
                '													DataTypeDateTime'\
                '												)'\
                '											),'\
                '											LatticeFunctionExpressionConstantScalar('\
                '												"24",'\
                '												DataTypeInt'\
                '											)'\
                '										)'\
                '									),'\
                '									LatticeFunctionExpression('\
                '										LatticeFunctionOperatorIdentifier("Equal"),'\
                '										LatticeFunctionIdentifier('\
                '											ContainerElementName("Lead_Status")'\
                '										),'\
                '										LatticeFunctionExpressionConstantScalar('\
                '											"1",'\
                '											DataTypeInt'\
                '										)'\
                '									)'\
                '								)'\
                '							)'\
                '						)'\
                '					)'\
                '				)'\
                '			),'\
                '			LatticeAddressSetPi('\
                '				LatticeAddressExpressionAtomic('\
                '					LatticeAddressAtomicIdentifier('\
                '						ContainerElementName("SFDC_Lead")'\
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
                '	ContainerElementName("SFDC_Lead_ID_ToBeScored_Lead")'\
                '))'

      SFDC_Lead_ID_ToBeScored_Lead = liaison.ExpressionVDBImplGeneric(spec2)
      conn_mgr.setSpec('SFDC_Lead_ID_ToBeScored_Lead', SFDC_Lead_ID_ToBeScored_Lead.definition())

    success = True

    return success
