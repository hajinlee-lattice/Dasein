
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from lxml import etree
from appsequence import Applicability, StepBase
import exceptions
import liaison
import re

class LP_020601_VDB_ResetLE_DomainInMKTO_LeadRecord( StepBase ):
  
  name        = 'LP_020601_VDB_ResetLE_DomainInMKTO_LeadRecord'
  description = 'Upgrade Modified Specs from 2.6.0 to 2.6.1:LP_020601_VDB_ResetLE_DomainInMKTO_LeadRecord'
  version     = '$Rev$'
  def __init__( self, forceApply = False ):
    super( LP_020601_VDB_ResetLE_DomainInMKTO_LeadRecord, self ).__init__( forceApply )

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
    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText('template_type')

    if type == 'MKTO':

      # Modify the Column in MKTO_LeadRecord
      MKTO_LeadRecord = conn_mgr.getTable('MKTO_LeadRecord')
      strTable = MKTO_LeadRecord.SpecLatticeNamedElements()
      strTable = strTable.replace(" ", "")
      strTable = strTable.replace('ContainerElementName("LE_Domain"),DataTypeVarChar(150)', 'ContainerElementName("LE_Domain"),DataTypeVarChar(150),TableFunctionExpression(TableFunctionOperator("SubString"),TableFunctionList(TableFunctionColumn(ContainerElementName("Email")) TableFunctionExpression(TableFunctionOperator("Plus"),TableFunctionList(TableFunctionExpression(TableFunctionOperator("LeftCharIndex"),TableFunctionList(TableFunctionColumn(ContainerElementName("Email")) TableFunctionConstant(ScalarValueTypedString("@",VectorDataTypeString(4))))) TableFunctionConstant(ScalarValueTypedString("1",VectorDataTypeWord)))) TableFunctionExpression(TableFunctionOperator("Length"),TableFunctionList(TableFunctionColumn(ContainerElementName("Email"))))))' )
      conn_mgr.setSpec("MKTO_LeadRecord", strTable)

      # Create the LG to run  MKTO_LeadRecord
      lgm.createLoadGroup('Adhoc_MKTO_LeadRecord_Reimport', 'Adhoc',
                          'Adhoc_MKTO_LeadRecord_Reimport', True, False)
      mlr_xml = etree.fromstring(lgm.getLoadGroup('Adhoc_MKTO_LeadRecord_Reimport').encode('ascii', 'xmlcharrefreplace'))
      mlr_xml = '''<extractQueries>
        <extractQuery qw="Workspace" queryName="Q_MKTO_LeadRecord" queryAlias="Q_MKTO_LeadRecord" sw="Workspace" schemaName="MKTO_LeadRecord" at="False" ucm="False">
          <schemas />
          <specs />
          <cms />
        </extractQuery>
      </extractQueries>'''

      lgm.setLoadGroupFunctionality('Adhoc_MKTO_LeadRecord_Reimport', mlr_xml)

      # Create the Validation Query
      spec_Validation = 'SpecLatticeNamedElements(SpecLatticeNamedElement('\
                        '	SpecLatticeQuery('\
                        '		LatticeAddressSetPushforward('\
                        '			LatticeAddressExpressionFromLAS('\
                        '				LatticeAddressSetMeet('\
                        '					('\
                        '						LatticeAddressSetFcn('\
                        '							LatticeFunctionExpression('\
                        '								LatticeFunctionOperatorIdentifier("NOT"),'\
                        '								LatticeFunctionExpression('\
                        '									LatticeFunctionOperatorIdentifier("IsNullValue"),'\
                        '									LatticeFunctionExpressionTransform('\
                        '										LatticeFunctionIdentifier('\
                        '											ContainerElementNameTableQualifiedName('\
                        '												LatticeSourceTableIdentifier('\
                        '													ContainerElementName("MKTO_LeadRecord")'\
                        '												),'\
                        '												ContainerElementName("LE_Domain")'\
                        '											)'\
                        '										),'\
                        '										LatticeAddressSetFromFcnSupport('\
                        '											LatticeFunctionIdentifier('\
                        '												ContainerElementNameTableQualifiedName('\
                        '													LatticeSourceTableIdentifier('\
                        '														ContainerElementName("MKTO_LeadRecord")'\
                        '													),'\
                        '													ContainerElementName("LE_Domain")'\
                        '												)'\
                        '											)'\
                        '										),'\
                        '										FunctionAggregationOperator("Max")'\
                        '									)'\
                        '								)'\
                        '							),'\
                        '							LatticeAddressExpressionMeet('\
                        '								('\
                        '									LatticeAddressExpressionAtomic('\
                        '										LatticeAddressAtomicIdentifier('\
                        '											ContainerElementName("MKTO_LeadRecord")'\
                        '										)'\
                        '									)'\
                        '								)'\
                        '							)'\
                        '						)'\
                        '					)'\
                        '				)'\
                        '			),'\
                        '			LatticeAddressSetMeet('\
                        '				('\
                        '					LatticeAddressSetFcn('\
                        '						LatticeFunctionExpression('\
                        '							LatticeFunctionOperatorIdentifier("NOT"),'\
                        '							LatticeFunctionExpression('\
                        '								LatticeFunctionOperatorIdentifier("IsNullValue"),'\
                        '								LatticeFunctionExpressionTransform('\
                        '									LatticeFunctionIdentifier('\
                        '										ContainerElementNameTableQualifiedName('\
                        '											LatticeSourceTableIdentifier('\
                        '												ContainerElementName("MKTO_LeadRecord")'\
                        '											),'\
                        '											ContainerElementName("LE_Domain")'\
                        '										)'\
                        '									),'\
                        '									LatticeAddressSetFromFcnSupport('\
                        '										LatticeFunctionIdentifier('\
                        '											ContainerElementNameTableQualifiedName('\
                        '												LatticeSourceTableIdentifier('\
                        '													ContainerElementName("MKTO_LeadRecord")'\
                        '												),'\
                        '												ContainerElementName("LE_Domain")'\
                        '											)'\
                        '										)'\
                        '									),'\
                        '									FunctionAggregationOperator("Max")'\
                        '								)'\
                        '							)'\
                        '						),'\
                        '						LatticeAddressExpressionMeet('\
                        '							('\
                        '								LatticeAddressExpressionAtomic('\
                        '									LatticeAddressAtomicIdentifier('\
                        '										ContainerElementName("MKTO_LeadRecord")'\
                        '									)'\
                        '								)'\
                        '							)'\
                        '						)'\
                        '					)'\
                        '				)'\
                        '			),'\
                        '			LatticeAddressExpressionMeet('\
                        '				('\
                        '					LatticeAddressExpressionAtomic('\
                        '						LatticeAddressAtomicIdentifier('\
                        '							ContainerElementName("MKTO_LeadRecord")'\
                        '						)'\
                        '					)'\
                        '				)'\
                        '			)'\
                        '		),'\
                        '		SpecQueryNamedFunctions('\
                        '			SpecQueryNamedFunctionExpression('\
                        '				ContainerElementName("MKTO_LeadRecord"),'\
                        '				LatticeFunctionIdentifierAddressAtomic('\
                        '					LatticeAddressAtomicIdentifier('\
                        '						ContainerElementName("MKTO_LeadRecord")'\
                        '					)'\
                        '				)'\
                        '			) '\
                        '			SpecQueryNamedFunctionExpression('\
                        '				ContainerElementName("Email"),'\
                        '				LatticeFunctionExpressionTransform('\
                        '					LatticeFunctionIdentifier('\
                        '						ContainerElementNameTableQualifiedName('\
                        '							LatticeSourceTableIdentifier('\
                        '								ContainerElementName("MKTO_LeadRecord")'\
                        '							),'\
                        '							ContainerElementName("Email")'\
                        '						)'\
                        '					),'\
                        '					LatticeAddressSetFromFcnSupport('\
                        '						LatticeFunctionIdentifier('\
                        '							ContainerElementNameTableQualifiedName('\
                        '								LatticeSourceTableIdentifier('\
                        '									ContainerElementName("MKTO_LeadRecord")'\
                        '								),'\
                        '								ContainerElementName("Email")'\
                        '							)'\
                        '						)'\
                        '					),'\
                        '					FunctionAggregationOperator("Max")'\
                        '				)'\
                        '			) '\
                        '			SpecQueryNamedFunctionExpression('\
                        '				ContainerElementName("LE_Domain"),'\
                        '				LatticeFunctionExpressionTransform('\
                        '					LatticeFunctionIdentifier('\
                        '						ContainerElementNameTableQualifiedName('\
                        '							LatticeSourceTableIdentifier('\
                        '								ContainerElementName("MKTO_LeadRecord")'\
                        '							),'\
                        '							ContainerElementName("LE_Domain")'\
                        '						)'\
                        '					),'\
                        '					LatticeAddressSetFromFcnSupport('\
                        '						LatticeFunctionIdentifier('\
                        '							ContainerElementNameTableQualifiedName('\
                        '								LatticeSourceTableIdentifier('\
                        '									ContainerElementName("MKTO_LeadRecord")'\
                        '								),'\
                        '								ContainerElementName("LE_Domain")'\
                        '							)'\
                        '						)'\
                        '					),'\
                        '					FunctionAggregationOperator("Max")'\
                        '				)'\
                        '			)'\
                        '		),'\
                        '		SpecQueryResultSetAll'\
                        '	),'\
                        '	ContainerElementName("Q_MKTO_LeadRecord_ValidateLEDomain")'\
                        '))'
      conn_mgr.setSpec('Q_MKTO_LeadRecord_ValidateLEDomain', spec_Validation)

    success = True

    return success
