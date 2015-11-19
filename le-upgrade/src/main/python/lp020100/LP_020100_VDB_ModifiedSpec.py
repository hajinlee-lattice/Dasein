
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from lxml import etree
from appsequence import Applicability, StepBase
import liaison

class LP_020100_VDB_ModifiedSpec( StepBase ):
  
  name        = 'LP_020100_VDB_ModifiedSpec'
  description = 'Upgrade Modified Specs from 2.0.1 to 2.1.0'
  version     = '$Rev$'
  def __init__( self, forceApply = False ):
    super( LP_020100_VDB_ModifiedSpec, self ).__init__( forceApply )

 ##Check SelectedForDante existig in Spec
  def getApplicability( self, appseq ):

      conn_mgr = appseq.getConnectionMgr()
      conn_mgr.getSpec("SelectedForDante")
      conn_mgr.getSpec("Time_OfMostRecentPushToDante")

      if not conn_mgr.getSpec("SelectedForDante") and conn_mgr.getSpec("Time_OfMostRecentPushToDante"):
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

      #Modify SelectedForDante
      if type == 'MKTO':
          spec1='SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '	SpecLatticeAliasDeclaration('\
              '		LatticeAddressSetFcn('\
              '			LatticeFunctionExpression('\
              '				LatticeFunctionOperatorIdentifier("OR"),'\
              '				LatticeFunctionIdentifier('\
              '					ContainerElementName("Dante_Stage_IsSelectedForDanteContact")'\
              '				),'\
              '				LatticeFunctionIdentifier('\
              '					ContainerElementName("Dante_Stage_IsSelectedForDanteLead")'\
              '				)'\
              '			),'\
              '			LatticeAddressSetPi('\
              '      LatticeAddressExpressionAtomic('\
              '        LatticeAddressAtomicIdentifier('\
              '         ContainerElementName('\
              '           "Timestamp_PushToDante_Stage"'\
              '         ) '\
              '       ) '\
              '     ) '\
              '    ) '\
              '		)'\
              '	),'\
              '	ContainerElementName("SelectedForDante")'\
              '))'
          SelectedForDante=liaison.ExpressionVDBImplGeneric(spec1)
          conn_mgr.setSpec('SelectedForDante',SelectedForDante.definition())

      elif type =='ELQ':
        spec1='SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '	SpecLatticeAliasDeclaration('\
              '		LatticeAddressSetFcn('\
              '			LatticeFunctionExpression('\
              '				LatticeFunctionOperatorIdentifier("OR"),'\
              '				LatticeFunctionIdentifier('\
              '					ContainerElementName("Dante_Stage_IsSelectedForDanteContact")'\
              '				),'\
              '				LatticeFunctionIdentifier('\
              '					ContainerElementName("Dante_Stage_IsSelectedForDanteLead")'\
              '				)'\
              '			),'\
              '			LatticeAddressSetPi('\
              '      LatticeAddressExpressionAtomic('\
              '        LatticeAddressAtomicIdentifier('\
              '         ContainerElementName('\
              '           "Timestamp_PushToDante_Stage"'\
              '         ) '\
              '       ) '\
              '     ) '\
              '    ) '\
              '	),'\
              '	ContainerElementName("SelectedForDante")'\
              '))'
        SelectedForDante=liaison.ExpressionVDBImplGeneric(spec1)
        conn_mgr.setSpec('SelectedForDante',SelectedForDante.definition())

      else:
        spec1='SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '	SpecLatticeAliasDeclaration('\
              '		LatticeAddressSetPi('\
              '			LatticeAddressExpressionAtomic('\
              '				LatticeAddressAtomicIdentifier('\
              '					ContainerElementName("Timestamp_PushToDante_Stage")'\
              '				)'\
              '			)'\
              '		)'\
              '	),'\
              '	ContainerElementName("SelectedForDante")'\
              '))'
        SelectedForDante=liaison.ExpressionVDBImplGeneric(spec1)
        conn_mgr.setSpec('SelectedForDante',SelectedForDante.definition())

      #Modify Time_OfMostRecentPushToDante
      if type == 'MKTO':
          Time_OfMostRecentPushToDante=liaison.ExpressionVDBImplGeneric('SpecLatticeNamedElements(SpecLatticeNamedElement(SpecLatticeFunction(LatticeFunctionExpressionTransform(LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("IsNull"),LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Timestamp_PushToDante")),ContainerElementName("Time_OfCompletion_PushToDante"))),LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante")))),FunctionAggregationOperator("Max")),LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("AddDay"),LatticeFunctionExpressionConstant("Now",DataTypeDateTime),LatticeFunctionExpressionConstant("-7305",DataTypeInt))),LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicAll)),FunctionAggregationOperator("None")),DataTypeUnknown,SpecFunctionTypeMetric,SpecFunctionSourceTypeCalculation,SpecDefaultValueNull,SpecDescription("")),ContainerElementName("Time_OfMostRecentPushToDante")))')
          conn_mgr.setSpec('Time_OfMostRecentPushToDante',Time_OfMostRecentPushToDante.definition())
      elif type =='ELQ':
          Time_OfMostRecentPushToDante=liaison.ExpressionVDBImplGeneric('SpecLatticeNamedElements(SpecLatticeNamedElement(SpecLatticeFunction(LatticeFunctionExpressionTransform(LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("IsNull"),LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Timestamp_PushToDante")),ContainerElementName("Time_OfCompletion_PushToDante"))),LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante")))),FunctionAggregationOperator("Max")),LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("AddDay"),LatticeFunctionExpressionConstant("Now",DataTypeDateTime),LatticeFunctionExpressionConstant("-7305",DataTypeInt))),LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicAll)),FunctionAggregationOperator("None")),DataTypeUnknown,SpecFunctionTypeMetric,SpecFunctionSourceTypeCalculation,SpecDefaultValueNull,SpecDescription("")),ContainerElementName("Time_OfMostRecentPushToDante")))')
          conn_mgr.setSpec('Time_OfMostRecentPushToDante',Time_OfMostRecentPushToDante.definition())
      else:
          Time_OfMostRecentPushToDante=liaison.ExpressionVDBImplGeneric('SpecLatticeNamedElements(SpecLatticeNamedElement(SpecLatticeFunction(LatticeFunctionExpressionTransform(LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("IsNull"),LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Timestamp_PushToDante")),ContainerElementName("Time_OfCompletion_PushToDante"))),LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante")))),FunctionAggregationOperator("Max")),LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("AddDay"),LatticeFunctionExpressionConstant("Now",DataTypeDateTime),LatticeFunctionExpressionConstant("-7305",DataTypeInt))),LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicAll)),FunctionAggregationOperator("None")),DataTypeUnknown,SpecFunctionTypeMetric,SpecFunctionSourceTypeCalculation,SpecDefaultValueNull,SpecDescription("")),ContainerElementName("Time_OfMostRecentPushToDante")))')
          conn_mgr.setSpec('Time_OfMostRecentPushToDante',Time_OfMostRecentPushToDante.definition())

      success = True

      return success






