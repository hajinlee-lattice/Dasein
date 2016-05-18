
#
# $LastChangedBy: lyan $
# $LastChangedDate: 2015-11-20 01:50:33 +0800 (Fri, 20 Nov 2015) $
# $Rev: 71049 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import liaison

class LP_020501_VDB_ModifiedSpec_ID_IsSelectedForPD( StepBase ):
  
  name        = 'LP_020100_VDB_ModifiedSpec'
  description = 'Upgrade Modified Specs from 2.3.0 to 2.4.0'
  version     = '$Rev: 71049 $'
  def __init__( self, forceApply = False ):
    super( LP_020501_VDB_ModifiedSpec_ID_IsSelectedForPD, self ).__init__( forceApply )

  def getApplicability( self, appseq ):

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )
      Spec_SelectedForPD =''
      if type == 'MKTO':
            Spec_SelectedForPD = conn_mgr.getSpec("MKTO_LeadRecord_ID_IsSelectedForPD")
      elif type == 'ELQ':
            Spec_SelectedForPD = conn_mgr.getSpec("ELQ_Contact_ContactID_IsSelectedForPD")
      else:
            Spec_SelectedForPD = conn_mgr.getSpec("SFDC_Lead_Contact_ID_IsSelectedForPD")

      if not Spec_SelectedForPD:
        return Applicability.cannotApplyPass

      return  Applicability.canApply

  def apply( self, appseq ):

      success = False

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )

      #Modify DS_PD_Alexa_RelatedLinks_Count
      if type == 'MKTO':
          spec1=  'SpecLatticeNamedElements('\
                  'SpecLatticeNamedElement('\
                  '	SpecLatticeFunction('\
                  '		LatticeFunctionExpressionTransform('\
                  '			LatticeFunctionExpression('\
                  '				LatticeFunctionOperatorIdentifier("Equal"),'\
                  '				LatticeFunctionIdentifier('\
                  '					ContainerElementNameTableQualifiedName('\
                  '						LatticeSourceTableIdentifier('\
                  '							ContainerElementName("MKTO_LeadRecord")'\
                  '						),'\
                  '						ContainerElementName("Id")'\
                  '					)'\
                  '				),'\
                  '				LatticeFunctionExpressionTransform('\
                  '					LatticeFunctionExpressionTransform('\
                  '						LatticeFunctionIdentifier('\
                  '							ContainerElementNameTableQualifiedName('\
                  '								LatticeSourceTableIdentifier('\
                  '									ContainerElementName("MKTO_LeadRecord")'\
                  '								),'\
                  '								ContainerElementName("Id")'\
                  '							)'\
                  '						),'\
                  '						LatticeAddressSetIdentifier('\
                  '							ContainerElementName("Alias_AllLeadTable")'\
                  '						),'\
                  '						FunctionAggregationSelectWhere('\
                  '							FunctionAggregationOperator("Max"),'\
                  '							LatticeFunctionIdentifier('\
                  '								ContainerElementNameTableQualifiedName('\
                  '									LatticeSourceTableIdentifier('\
                  '										ContainerElementName("Timestamp_DownloadedLeads")'\
                  '									),'\
                  '									ContainerElementName("Time_OfCompletion_DownloadedLeads")'\
                  '								)'\
                  '							),'\
                  '							FunctionAggregationOperator("Max")'\
                  '						)'\
                  '					),'\
                  '					LatticeAddressSetPi('\
                  '						LatticeAddressExpressionAtomic('\
                  '							LatticeAddressAtomicIdentifier('\
                  '								ContainerElementName("PropDataID")'\
                  '							)'\
                  '						)'\
                  '					),'\
                  '					FunctionAggregationOperator("None")'\
                  '				)'\
                  '			),'\
                  '			LatticeAddressSetIdentifier('\
                  '				ContainerElementName("Alias_AllLeadTable")'\
                  '			),'\
                  '			FunctionAggregationOperator("Max")'\
                  '		),'\
                  '		DataTypeUnknown,'\
                  '		SpecFunctionTypeMetric,'\
                  '		SpecFunctionSourceTypeCalculation,'\
                  '		SpecDefaultValueNull,'\
                  '		SpecDescription("")'\
                  '	),'\
                  '	ContainerElementName("MKTO_LeadRecord_ID_IsSelectedForPD")'\
                  '))'
          ID_IsSelectedForPD=liaison.ExpressionVDBImplGeneric(spec1)
          conn_mgr.setSpec('MKTO_LeadRecord_ID_IsSelectedForPD',ID_IsSelectedForPD.definition())
      elif type == 'ELQ':
        spec1=    'SpecLatticeNamedElements('\
                  'SpecLatticeNamedElement('\
                  '	SpecLatticeFunction('\
                  '		LatticeFunctionExpressionTransform('\
                  '			LatticeFunctionExpression('\
                  '				LatticeFunctionOperatorIdentifier("Equal"),'\
                  '				LatticeFunctionIdentifier('\
                  '					ContainerElementNameTableQualifiedName('\
                  '						LatticeSourceTableIdentifier('\
                  '							ContainerElementName("ELQ_Contact")'\
                  '						),'\
                  '						ContainerElementName("ContactID")'\
                  '					)'\
                  '				),'\
                  '				LatticeFunctionExpressionTransform('\
                  '					LatticeFunctionExpressionTransform('\
                  '						LatticeFunctionIdentifier('\
                  '							ContainerElementNameTableQualifiedName('\
                  '								LatticeSourceTableIdentifier('\
                  '									ContainerElementName("ELQ_Contact")'\
                  '								),'\
                  '								ContainerElementName("ContactID")'\
                  '							)'\
                  '						),'\
                  '						LatticeAddressSetIdentifier('\
                  '							ContainerElementName("Alias_AllLeadTable")'\
                  '						),'\
                  '						FunctionAggregationSelectWhere('\
                  '							FunctionAggregationOperator("Max"),'\
                  '							LatticeFunctionIdentifier('\
                  '								ContainerElementNameTableQualifiedName('\
                  '									LatticeSourceTableIdentifier('\
                  '										ContainerElementName("Timestamp_DownloadedLeads")'\
                  '									),'\
                  '									ContainerElementName("Time_OfCompletion_DownloadedLeads")'\
                  '								)'\
                  '							),'\
                  '							FunctionAggregationOperator("Max")'\
                  '						)'\
                  '					),'\
                  '					LatticeAddressSetPi('\
                  '						LatticeAddressExpressionAtomic('\
                  '							LatticeAddressAtomicIdentifier('\
                  '								ContainerElementName("PropDataID")'\
                  '							)'\
                  '						)'\
                  '					),'\
                  '					FunctionAggregationOperator("None")'\
                  '				)'\
                  '			),'\
                  '			LatticeAddressSetIdentifier('\
                  '				ContainerElementName("Alias_AllLeadTable")'\
                  '			),'\
                  '			FunctionAggregationOperator("Max")'\
                  '		),'\
                  '		DataTypeUnknown,'\
                  '		SpecFunctionTypeMetric,'\
                  '		SpecFunctionSourceTypeCalculation,'\
                  '		SpecDefaultValueNull,'\
                  '		SpecDescription("")'\
                  '	),'\
                  '	ContainerElementName("ELQ_Contact_ContactID_IsSelectedForPD")'\
                  '))'
        ID_IsSelectedForPD=liaison.ExpressionVDBImplGeneric(spec1)
        conn_mgr.setSpec('ELQ_Contact_ContactID_IsSelectedForPD',ID_IsSelectedForPD.definition())
      else:
        spec1=    'SpecLatticeNamedElements('\
                  'SpecLatticeNamedElement('\
                  '	SpecLatticeFunction('\
                  '		LatticeFunctionExpressionTransform('\
                  '			LatticeFunctionExpression('\
                  '				LatticeFunctionOperatorIdentifier("Equal"),'\
                  '				LatticeFunctionIdentifier('\
                  '					ContainerElementName("Alias_ID_Lead")'\
                  '				),'\
                  '				LatticeFunctionExpressionTransform('\
                  '					LatticeFunctionExpressionTransform('\
                  '						LatticeFunctionIdentifier('\
                  '							ContainerElementName("Alias_ID_Lead")'\
                  '						),'\
                  '						LatticeAddressSetIdentifier('\
                  '							ContainerElementName("Alias_AllLeadTable")'\
                  '						),'\
                  '						FunctionAggregationSelectWhere('\
                  '							FunctionAggregationOperator("Max"),'\
                  '							LatticeFunctionIdentifier('\
                  '								ContainerElementNameTableQualifiedName('\
                  '									LatticeSourceTableIdentifier('\
                  '										ContainerElementName("Timestamp_Downloaded_Lead_Contact")'\
                  '									),'\
                  '									ContainerElementName("Time_OfCompletion_DownloadedLeads")'\
                  '								)'\
                  '							),'\
                  '							FunctionAggregationOperator("Max")'\
                  '						)'\
                  '					),'\
                  '					LatticeAddressSetPi('\
                  '						LatticeAddressExpressionAtomic('\
                  '							LatticeAddressAtomicIdentifier('\
                  '								ContainerElementName("PropDataID")'\
                  '							)'\
                  '						)'\
                  '					),'\
                  '					FunctionAggregationOperator("None")'\
                  '				)'\
                  '			),'\
                  '			LatticeAddressSetIdentifier('\
                  '				ContainerElementName("Alias_AllLeadTable")'\
                  '			),'\
                  '			FunctionAggregationOperator("Max")'\
                  '		),'\
                  '		DataTypeUnknown,'\
                  '		SpecFunctionTypeMetric,'\
                  '		SpecFunctionSourceTypeCalculation,'\
                  '		SpecDefaultValueNull,'\
                  '		SpecDescription("")'\
                  '	),'\
                  '	ContainerElementName("SFDC_Lead_Contact_ID_IsSelectedForPD")'\
                  '))'
        ID_IsSelectedForPD=liaison.ExpressionVDBImplGeneric(spec1)
        conn_mgr.setSpec('SFDC_Lead_Contact_ID_IsSelectedForPD',ID_IsSelectedForPD.definition())

      success = True
      return success






