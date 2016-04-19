#
# $LastChangedBy: lyan $
# $LastChangedDate: 2015-11-19 19:51:40 +0800 (Thu, 19 Nov 2015) $
# $Rev: 71042 $
#

from liaison import *
from appsequence import Applicability, StepBase
from lxml import etree
import os, liaison

class LP_020404_SFDC_Lead_Contact_ID_ToBeMatched(StepBase):
  name = 'LP_020401_ChangeTimezoneOffset'
  description = 'Fix the bug in SFDC funnction:SFDC_Lead_Contact_ID_ToBeMatched'
  version = '$Rev: 71042 $'

  def __init__(self, forceApply=False):
    super(LP_020404_SFDC_Lead_Contact_ID_ToBeMatched, self).__init__(forceApply)

  def getApplicability(self, appseq):
    conn_mgr = appseq.getConnectionMgr()
    type = appseq.getText('template_type')
    if type=='SFDC' and conn_mgr.getSpec('SFDC_Lead_Contact_ID_ToBeMatched'):
      return Applicability.canApply
    else:
      return Applicability.alreadyAppliedPass

  def apply(self, appseq):

    # Modified SFDC_Lead_Contact_ID_ToBeMatched
    success = False
    conn_mgr = appseq.getConnectionMgr()
    type = appseq.getText( 'template_type' )
    if type == 'SFDC':
      spec='SpecLatticeNamedElements(SpecLatticeNamedElement('\
          '	SpecLatticeFunction('\
          '		LatticeFunctionExpressionTransform('\
          '			LatticeFunctionExpression('\
          '				LatticeFunctionOperatorIdentifier("IF"),'\
          '				LatticeFunctionIdentifier('\
          '					ContainerElementName("Is_Contact")'\
          '				),'\
          '				LatticeFunctionExpression('\
          '					LatticeFunctionOperatorIdentifier("AND"),'\
          '					LatticeFunctionExpression('\
          '						LatticeFunctionOperatorIdentifier("NOT"),'\
          '						LatticeFunctionIdentifier('\
          '							ContainerElementName("SFDC_Contact_Email_IsAnonymous_Contact")'\
          '						)'\
          '					),'\
          '					LatticeFunctionExpression('\
          '						LatticeFunctionOperatorIdentifier("OR"),'\
          '						LatticeFunctionExpression('\
          '							LatticeFunctionOperatorIdentifier("IsNullValue"),'\
          '							LatticeFunctionIdentifier('\
          '								ContainerElementNameTableQualifiedName('\
          '									LatticeSourceTableIdentifier('\
          '										ContainerElementName("Timestamp_MatchToPD")'\
          '									),'\
          '									ContainerElementName("SFDC_Lead_Contact_ID")'\
          '								)'\
          '							)'\
          '						),'\
          '						LatticeFunctionExpression('\
          '							LatticeFunctionOperatorIdentifier("IsNull"),'\
          '							LatticeFunctionExpression('\
          '								LatticeFunctionOperatorIdentifier("AND"),'\
          '								LatticeFunctionExpression('\
          '									LatticeFunctionOperatorIdentifier("Greater"),'\
          '									LatticeFunctionIdentifier('\
          '										ContainerElementNameTableQualifiedName('\
          '											LatticeSourceTableIdentifier('\
          '												ContainerElementName("Timestamp_Downloaded_Lead_Contact")'\
          '											),'\
          '											ContainerElementName("Time_OfCompletion_DownloadedLeads")'\
          '										)'\
          '									),'\
          '									LatticeFunctionIdentifier('\
          '										ContainerElementName("Time_OfMostRecentMatchToPD")'\
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
          '															ContainerElementName("Timestamp_MatchToPD")'\
          '														),'\
          '														ContainerElementName("Time_OfSubmission_MatchToPD")'\
          '													)'\
          '												)'\
          '											),'\
          '											LatticeFunctionExpressionConstant('\
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
          '											ContainerElementName("Const_DaysOfValidityForPD")'\
          '										)'\
          '									)'\
          '								)'\
          '							),'\
          '							LatticeFunctionExpressionConstant('\
          '								"FALSE",'\
          '								DataTypeBit'\
          '							)'\
          '						)'\
          '					)'\
          '				),'\
          '				LatticeFunctionExpression('\
          '					LatticeFunctionOperatorIdentifier("IF"),'\
          '					LatticeFunctionIdentifier('\
          '						ContainerElementName("Is_Lead")'\
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
          '								LatticeFunctionOperatorIdentifier("IsNullValue"),'\
          '								LatticeFunctionIdentifier('\
          '									ContainerElementNameTableQualifiedName('\
          '										LatticeSourceTableIdentifier('\
          '											ContainerElementName("Timestamp_MatchToPD")'\
          '										),'\
          '										ContainerElementName("SFDC_Lead_Contact_ID")'\
          '									)'\
          '								)'\
          '							),'\
          '							LatticeFunctionExpression('\
          '								LatticeFunctionOperatorIdentifier("IsNull"),'\
          '								LatticeFunctionExpression('\
          '									LatticeFunctionOperatorIdentifier("AND"),'\
          '									LatticeFunctionExpression('\
          '										LatticeFunctionOperatorIdentifier("Greater"),'\
          '										LatticeFunctionIdentifier('\
          '											ContainerElementNameTableQualifiedName('\
          '												LatticeSourceTableIdentifier('\
          '													ContainerElementName("Timestamp_Downloaded_Lead_Contact")'\
          '												),'\
          '												ContainerElementName("Time_OfCompletion_DownloadedLeads")'\
          '											)'\
          '										),'\
          '										LatticeFunctionIdentifier('\
          '											ContainerElementName("Time_OfMostRecentMatchToPD")'\
          '										)'\
          '									),'\
          '									LatticeFunctionExpression('\
          '										LatticeFunctionOperatorIdentifier("Greater"),'\
          '										LatticeFunctionExpression('\
          '											LatticeFunctionOperatorIdentifier("Minus"),'\
          '											LatticeFunctionExpression('\
          '												LatticeFunctionOperatorIdentifier("ConvertToInt"),'\
          '												LatticeFunctionExpressionConstant('\
          '													"Now",'\
          '													DataTypeDateTime'\
          '												)'\
          '											),'\
          '											LatticeFunctionExpression('\
          '												LatticeFunctionOperatorIdentifier("IsNull"),'\
          '												LatticeFunctionExpression('\
          '													LatticeFunctionOperatorIdentifier("ConvertToInt"),'\
          '													LatticeFunctionIdentifier('\
          '														ContainerElementNameTableQualifiedName('\
          '															LatticeSourceTableIdentifier('\
          '																ContainerElementName("Timestamp_MatchToPD")'\
          '															),'\
          '															ContainerElementName("Time_OfSubmission_MatchToPD")'\
          '														)'\
          '													)'\
          '												),'\
          '												LatticeFunctionExpressionConstant('\
          '													"0",'\
          '													DataTypeInt'\
          '												)'\
          '											)'\
          '										),'\
          '										LatticeFunctionExpression('\
          '											LatticeFunctionOperatorIdentifier("Multiply"),'\
          '											LatticeFunctionExpressionConstant('\
          '												"86400",'\
          '												DataTypeInt'\
          '											),'\
          '											LatticeFunctionIdentifier('\
          '												ContainerElementName("Const_DaysOfValidityForPD")'\
          '											)'\
          '										)'\
          '									)'\
          '								),'\
          '								LatticeFunctionExpressionConstant('\
          '									"FALSE",'\
          '									DataTypeBit'\
          '								)'\
          '							)'\
          '						)'\
          '					),'\
          '					LatticeFunctionExpressionConstant('\
          '						"FALSE",'\
          '						DataTypeBit'\
          '					)'\
          '				)'\
          '			),'\
          '			LatticeAddressSetPi('\
          '				LatticeAddressExpressionAtomic('\
          '					LatticeAddressAtomicIdentifier('\
          '						ContainerElementName("SFDC_Lead_Contact_ID")'\
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
          '	ContainerElementName("SFDC_Lead_Contact_ID_ToBeMatched")'\
          '))'
      SFDC_Lead_Contact_ID_ToBeMatched=liaison.ExpressionVDBImplGeneric(spec)
      conn_mgr.setSpec('SFDC_Lead_Contact_ID_ToBeMatched',SFDC_Lead_Contact_ID_ToBeMatched.definition())
    else:
      pass
    return True
