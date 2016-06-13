
#
# $LastChangedBy: lyan $
# $LastChangedDate: 2015-11-20 01:50:33 +0800 (Fri, 20 Nov 2015) $
# $Rev: 71049 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import liaison

class LP_020600_VDB_ModifiedSpec_SelectedForDantetoHandleNULLValues( StepBase ):
  
  name        = 'LP_020100_VDB_ModifiedSpec'
  description = 'Upgrade Modified Specs from 2.5.1 to 2.6.0'
  version     = '$Rev: 71049 $'
  def __init__( self, forceApply = False ):
    super( LP_020600_VDB_ModifiedSpec_SelectedForDantetoHandleNULLValues, self ).__init__( forceApply )

  def getApplicability( self, appseq ):

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )
      Spec_SelectedForPD =''
      if type in ('MKTO','ELQ'):
            Spec_DanteContact = conn_mgr.getSpec("Dante_Stage_IsSelectedForDanteContact")
            Spec_DanteLead = conn_mgr.getSpec("Dante_Stage_IsSelectedForDanteLead")
            if not Spec_DanteContact or not Spec_DanteLead:
              return Applicability.cannotApplyPass
      else:
            pass
      return  Applicability.canApply

  def apply( self, appseq ):

      success = False

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )

      #Modify Dante_Stage_IsSelectedForDanteContact
      if type == 'MKTO':
          spec1=  'SpecLatticeNamedElements('\
                  'SpecLatticeNamedElement('\
                  '	SpecLatticeFunction('\
                  '		LatticeFunctionExpressionTransform('\
                  '			LatticeFunctionExpression('\
                  '				LatticeFunctionOperatorIdentifier("IsNull"),'\
                  '				LatticeFunctionExpression('\
                  '					LatticeFunctionOperatorIdentifier("Equal"),'\
                  '					LatticeFunctionExpressionTransform('\
                  '						LatticeFunctionIdentifier('\
                  '							ContainerElementNameTableQualifiedName('\
                  '								LatticeSourceTableIdentifier('\
                  '									ContainerElementName("MKTO_LeadRecord")'\
                  '								),'\
                  '								ContainerElementName("Id")'\
                  '							)'\
                  '						),'\
                  '						LatticeAddressSetPi('\
                  '							LatticeAddressExpressionAtomic('\
                  '								LatticeAddressAtomicIdentifier('\
                  '									ContainerElementName("MKTO_LeadRecord")'\
                  '								)'\
                  '							)'\
                  '						),'\
                  '						FunctionAggregationOperator("Max")'\
                  '					),'\
                  '					LatticeFunctionExpressionTransform('\
                  '						LatticeFunctionExpressionTransform('\
                  '							LatticeFunctionExpressionTransform('\
                  '								LatticeFunctionIdentifier('\
                  '									ContainerElementNameTableQualifiedName('\
                  '										LatticeSourceTableIdentifier('\
                  '											ContainerElementName("MKTO_LeadRecord")'\
                  '										),'\
                  '										ContainerElementName("Id")'\
                  '									)'\
                  '								),'\
                  '								LatticeAddressSetPi('\
                  '									LatticeAddressExpressionAtomic('\
                  '										LatticeAddressAtomicIdentifier('\
                  '											ContainerElementName("MKTO_LeadRecord")'\
                  '										)'\
                  '									)'\
                  '								),'\
                  '								FunctionAggregationOperator("Max")'\
                  '							),'\
                  '							LatticeAddressSetPi('\
                  '								LatticeAddressExpressionAtomic('\
                  '									LatticeAddressAtomicIdentifier('\
                  '										ContainerElementName("MKTO_LeadRecord_ID")'\
                  '									)'\
                  '								)'\
                  '							),'\
                  '							FunctionAggregationSelectWhere('\
                  '								FunctionAggregationOperator("Max"),'\
                  '								LatticeFunctionIdentifier('\
                  '									ContainerElementName("Dante_RankForLead")'\
                  '								),'\
                  '								FunctionAggregationOperator("Max")'\
                  '							)'\
                  '						),'\
                  '						LatticeAddressSetPi('\
                  '							LatticeAddressExpressionAtomic('\
                  '								LatticeAddressAtomicIdentifier('\
                  '									ContainerElementName("SFDC_Contact_ID")'\
                  '								)'\
                  '							)'\
                  '						),'\
                  '						FunctionAggregationOperator("Max")'\
                  '					)'\
                  '				),'\
                  '				LatticeFunctionExpressionConstant('\
                  '					"FALSE",'\
                  '					DataTypeBit'\
                  '				)'\
                  '			),'\
                  '			LatticeAddressSetPi('\
                  '				LatticeAddressExpressionAtomic('\
                  '					LatticeAddressAtomicIdentifier('\
                  '						ContainerElementName("MKTO_LeadRecord")'\
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
                  '	ContainerElementName("Dante_Stage_IsSelectedForDanteContact")'\
                  '))'
          ID_IsSelectedForPD=liaison.ExpressionVDBImplGeneric(spec1)
          conn_mgr.setSpec('Dante_Stage_IsSelectedForDanteContact',ID_IsSelectedForPD.definition())

          spec2=  'SpecLatticeNamedElements('\
                  'SpecLatticeNamedElement('\
                  '	SpecLatticeFunction('\
                  '		LatticeFunctionExpressionTransform('\
                  '			LatticeFunctionExpression('\
                  '				LatticeFunctionOperatorIdentifier("IsNull"),'\
                  '				LatticeFunctionExpression('\
                  '					LatticeFunctionOperatorIdentifier("Equal"),'\
                  '					LatticeFunctionExpressionTransform('\
                  '						LatticeFunctionIdentifier('\
                  '							ContainerElementNameTableQualifiedName('\
                  '								LatticeSourceTableIdentifier('\
                  '									ContainerElementName("MKTO_LeadRecord")'\
                  '								),'\
                  '								ContainerElementName("Id")'\
                  '							)'\
                  '						),'\
                  '						LatticeAddressSetSourceTable('\
                  '							LatticeSourceTableIdentifier('\
                  '								ContainerElementName("MKTO_LeadRecord")'\
                  '							),'\
                  '							LatticeAddressExpressionAtomic('\
                  '								LatticeAddressAtomicIdentifier('\
                  '									ContainerElementName("MKTO_LeadRecord")'\
                  '								)'\
                  '							)'\
                  '						),'\
                  '						FunctionAggregationOperator("Max")'\
                  '					),'\
                  '					LatticeFunctionExpressionTransform('\
                  '						LatticeFunctionExpressionTransform('\
                  '							LatticeFunctionExpressionTransform('\
                  '								LatticeFunctionIdentifier('\
                  '									ContainerElementNameTableQualifiedName('\
                  '										LatticeSourceTableIdentifier('\
                  '											ContainerElementName("MKTO_LeadRecord")'\
                  '										),'\
                  '										ContainerElementName("Id")'\
                  '									)'\
                  '								),'\
                  '								LatticeAddressSetSourceTable('\
                  '									LatticeSourceTableIdentifier('\
                  '										ContainerElementName("MKTO_LeadRecord")'\
                  '									),'\
                  '									LatticeAddressExpressionAtomic('\
                  '										LatticeAddressAtomicIdentifier('\
                  '											ContainerElementName("MKTO_LeadRecord")'\
                  '										)'\
                  '									)'\
                  '								),'\
                  '								FunctionAggregationOperator("Max")'\
                  '							),'\
                  '							LatticeAddressSetPi('\
                  '								LatticeAddressExpressionAtomic('\
                  '									LatticeAddressAtomicIdentifier('\
                  '										ContainerElementName("MKTO_LeadRecord_ID")'\
                  '									)'\
                  '								)'\
                  '							),'\
                  '							FunctionAggregationSelectWhere('\
                  '								FunctionAggregationOperator("Max"),'\
                  '								LatticeFunctionIdentifier('\
                  '									ContainerElementName("Dante_RankForLead")'\
                  '								),'\
                  '								FunctionAggregationOperator("Max")'\
                  '							)'\
                  '						),'\
                  '						LatticeAddressSetPi('\
                  '							LatticeAddressExpressionAtomic('\
                  '								LatticeAddressAtomicIdentifier('\
                  '									ContainerElementName("SFDC_Lead_ID")'\
                  '								)'\
                  '							)'\
                  '						),'\
                  '						FunctionAggregationOperator("Max")'\
                  '					)'\
                  '				),'\
                  '				LatticeFunctionExpressionConstant('\
                  '					"FALSE",'\
                  '					DataTypeBit'\
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
                  '	ContainerElementName("Dante_Stage_IsSelectedForDanteLead")'\
                  '))'
          ID_IsSelectedForPD=liaison.ExpressionVDBImplGeneric(spec2)
          conn_mgr.setSpec('Dante_Stage_IsSelectedForDanteLead',ID_IsSelectedForPD.definition())
      elif type == 'ELQ':
         spec1=   'SpecLatticeNamedElements('\
                  'SpecLatticeNamedElement('\
                  '	SpecLatticeFunction('\
                  '		LatticeFunctionExpressionTransform('\
                  '			LatticeFunctionExpression('\
                  '				LatticeFunctionOperatorIdentifier("IsNull"),'\
                  '				LatticeFunctionExpression('\
                  '					LatticeFunctionOperatorIdentifier("Equal"),'\
                  '					LatticeFunctionExpressionTransform('\
                  '						LatticeFunctionIdentifier('\
                  '							ContainerElementNameTableQualifiedName('\
                  '								LatticeSourceTableIdentifier('\
                  '									ContainerElementName("ELQ_Contact")'\
                  '								),'\
                  '								ContainerElementName("ContactID")'\
                  '							)'\
                  '						),'\
                  '						LatticeAddressSetPi('\
                  '							LatticeAddressExpressionAtomic('\
                  '								LatticeAddressAtomicIdentifier('\
                  '									ContainerElementName("ELQ_Contact")'\
                  '								)'\
                  '							)'\
                  '						),'\
                  '						FunctionAggregationOperator("Max")'\
                  '					),'\
                  '					LatticeFunctionExpressionTransform('\
                  '						LatticeFunctionExpressionTransform('\
                  '							LatticeFunctionExpressionTransform('\
                  '								LatticeFunctionIdentifier('\
                  '									ContainerElementNameTableQualifiedName('\
                  '										LatticeSourceTableIdentifier('\
                  '											ContainerElementName("ELQ_Contact")'\
                  '										),'\
                  '										ContainerElementName("ContactID")'\
                  '									)'\
                  '								),'\
                  '								LatticeAddressSetPi('\
                  '									LatticeAddressExpressionAtomic('\
                  '										LatticeAddressAtomicIdentifier('\
                  '											ContainerElementName("ELQ_Contact")'\
                  '										)'\
                  '									)'\
                  '								),'\
                  '								FunctionAggregationOperator("Max")'\
                  '							),'\
                  '							LatticeAddressSetPi('\
                  '								LatticeAddressExpressionAtomic('\
                  '									LatticeAddressAtomicIdentifier('\
                  '										ContainerElementName("ELQ_Contact_ContactID")'\
                  '									)'\
                  '								)'\
                  '							),'\
                  '							FunctionAggregationSelectWhere('\
                  '								FunctionAggregationOperator("Max"),'\
                  '								LatticeFunctionIdentifier('\
                  '									ContainerElementName("Dante_RankForLead")'\
                  '								),'\
                  '								FunctionAggregationOperator("Max")'\
                  '							)'\
                  '						),'\
                  '						LatticeAddressSetPi('\
                  '							LatticeAddressExpressionAtomic('\
                  '								LatticeAddressAtomicIdentifier('\
                  '									ContainerElementName("SFDC_Contact_ID")'\
                  '								)'\
                  '							)'\
                  '						),'\
                  '						FunctionAggregationOperator("Max")'\
                  '					)'\
                  '				),'\
                  '				LatticeFunctionExpressionConstant('\
                  '					"FALSE",'\
                  '					DataTypeBit'\
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
                  '	ContainerElementName("Dante_Stage_IsSelectedForDanteContact")'\
                  '))'
         ID_IsSelectedForPD=liaison.ExpressionVDBImplGeneric(spec1)
         conn_mgr.setSpec('Dante_Stage_IsSelectedForDanteContact',ID_IsSelectedForPD.definition())
         spec2=  'SpecLatticeNamedElements('\
                'SpecLatticeNamedElement('\
                '	SpecLatticeFunction('\
                '		LatticeFunctionExpressionTransform('\
                '			LatticeFunctionExpression('\
                '				LatticeFunctionOperatorIdentifier("IsNull"),'\
                '				LatticeFunctionExpression('\
                '					LatticeFunctionOperatorIdentifier("Equal"),'\
                '					LatticeFunctionExpressionTransform('\
                '						LatticeFunctionIdentifier('\
                '							ContainerElementNameTableQualifiedName('\
                '								LatticeSourceTableIdentifier('\
                '									ContainerElementName("ELQ_Contact")'\
                '								),'\
                '								ContainerElementName("ContactID")'\
                '							)'\
                '						),'\
                '						LatticeAddressSetPi('\
                '							LatticeAddressExpressionAtomic('\
                '								LatticeAddressAtomicIdentifier('\
                '									ContainerElementName("ELQ_Contact")'\
                '								)'\
                '							)'\
                '						),'\
                '						FunctionAggregationOperator("Max")'\
                '					),'\
                '					LatticeFunctionExpressionTransform('\
                '						LatticeFunctionExpressionTransform('\
                '							LatticeFunctionExpressionTransform('\
                '								LatticeFunctionIdentifier('\
                '									ContainerElementNameTableQualifiedName('\
                '										LatticeSourceTableIdentifier('\
                '											ContainerElementName("ELQ_Contact")'\
                '										),'\
                '										ContainerElementName("ContactID")'\
                '									)'\
                '								),'\
                '								LatticeAddressSetPi('\
                '									LatticeAddressExpressionAtomic('\
                '										LatticeAddressAtomicIdentifier('\
                '											ContainerElementName("ELQ_Contact")'\
                '										)'\
                '									)'\
                '								),'\
                '								FunctionAggregationOperator("Max")'\
                '							),'\
                '							LatticeAddressSetPi('\
                '								LatticeAddressExpressionAtomic('\
                '									LatticeAddressAtomicIdentifier('\
                '										ContainerElementName("ELQ_Contact_ContactID")'\
                '									)'\
                '								)'\
                '							),'\
                '							FunctionAggregationSelectWhere('\
                '								FunctionAggregationOperator("Max"),'\
                '								LatticeFunctionIdentifier('\
                '									ContainerElementName("Dante_RankForLead")'\
                '								),'\
                '								FunctionAggregationOperator("Max")'\
                '							)'\
                '						),'\
                '						LatticeAddressSetPi('\
                '							LatticeAddressExpressionAtomic('\
                '								LatticeAddressAtomicIdentifier('\
                '									ContainerElementName("SFDC_Lead_ID")'\
                '								)'\
                '							)'\
                '						),'\
                '						FunctionAggregationOperator("Max")'\
                '					)'\
                '				),'\
                '				LatticeFunctionExpressionConstant('\
                '					"FALSE",'\
                '					DataTypeBit'\
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
                '	ContainerElementName("Dante_Stage_IsSelectedForDanteLead")'\
                '))'
         ID_IsSelectedForPD=liaison.ExpressionVDBImplGeneric(spec2)
         conn_mgr.setSpec('Dante_Stage_IsSelectedForDanteLead',ID_IsSelectedForPD.definition())
      else:
        pass

      success = True
      return success






