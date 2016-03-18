
#
# $LastChangedBy: lyan $
# $LastChangedDate: 2015-11-20 01:50:33 +0800 (Fri, 20 Nov 2015) $
# $Rev: 71049 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import liaison

class LP_020400_VDB_ModifiedSpec( StepBase ):
  
  name        = 'LP_020100_VDB_ModifiedSpec'
  description = 'Upgrade Modified Specs from 2.3.0 to 2.4.0'
  version     = '$Rev: 71049 $'
  def __init__( self, forceApply = False ):
    super( LP_020400_VDB_ModifiedSpec, self ).__init__( forceApply )

  def getApplicability( self, appseq ):

      conn_mgr = appseq.getConnectionMgr()
      conn_mgr.getSpec("DS_PD_Alexa_RelatedLinks_Count")
      conn_mgr.getSpec("DS_PD_JobsTrendString_Ordered")
      conn_mgr.getSpec("DS_PD_FundingStage_Ordered")
      conn_mgr.getSpec("DS_PD_ModelAction_Ordered")
      conn_mgr.getSpec("Lead_RankForModeling")

      if not conn_mgr.getSpec("DS_PD_Alexa_RelatedLinks_Count") and conn_mgr.getSpec("DS_PD_Alexa_RelatedLinks_Count")\
        and conn_mgr.getSpec("DS_PD_FundingStage_Ordered") and conn_mgr.getSpec("DS_PD_ModelAction_Ordered")\
        and conn_mgr.getSpec("Lead_RankForModeling"):
        return Applicability.cannotApplyPass

      return  Applicability.canApply

  def apply( self, appseq ):

      success = False

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )

      #Modify DS_PD_Alexa_RelatedLinks_Count
      if type == 'MKTO':
          spec1='SpecLatticeNamedElements(SpecLatticeNamedElement('\
                '      SpecLatticeFunction('\
                '        LatticeFunctionExpressionTransform('\
                '          LatticeFunctionExpression('\
                '            LatticeFunctionOperatorIdentifier('\
                '              "Plus"'\
                '            )'\
                '          , LatticeFunctionExpression('\
                '              LatticeFunctionOperatorIdentifier('\
                '                "Minus"'\
                '              )'\
                '            , LatticeFunctionExpression('\
                '                LatticeFunctionOperatorIdentifier('\
                '                  "Length"'\
                '                )'\
                '              , LatticeFunctionIdentifier('\
                '                  ContainerElementNameTableQualifiedName('\
                '                    LatticeSourceTableIdentifier('\
                '                      ContainerElementName('\
                '                        "PD_DerivedColumns"'\
                '                      )'\
                '                    )'\
                '                  , ContainerElementName('\
                '                      "AlexaRelatedLinks"'\
                '                    )'\
                '                  )'\
                '                )'\
                '              )'\
                '            , LatticeFunctionExpression('\
                '                LatticeFunctionOperatorIdentifier('\
                '                  "Length"'\
                '                )'\
                '              , LatticeFunctionExpression('\
                '                  LatticeFunctionOperatorIdentifier('\
                '                    "Replace"'\
                '                  )'\
                '                , LatticeFunctionIdentifier('\
                '                    ContainerElementNameTableQualifiedName('\
                '                      LatticeSourceTableIdentifier('\
                '                        ContainerElementName('\
                '                          "PD_DerivedColumns"'\
                '                        )'\
                '                      )'\
                '                    , ContainerElementName('\
                '                        "AlexaRelatedLinks"'\
                '                      )'\
                '                    )'\
                '                  )'\
                '                , LatticeFunctionExpressionConstantScalar('\
                '                    ",", DataTypeNVarChar('\
                '                      1'\
                '                    )'\
                '                  )'\
                '                , LatticeFunctionExpressionConstantScalar('\
                '                    "", DataTypeNVarChar('\
                '                      0'\
                '                    )'\
                '                  )'\
                '                )'\
                '              )'\
                '            )'\
                '          , LatticeFunctionExpressionConstantScalar('\
                '              "1", DataTypeInt'\
                '            )'\
                '          )'\
                '        , LatticeAddressSetPi('\
                '            LatticeAddressExpressionAtomic('\
                '              LatticeAddressAtomicIdentifier('\
                '                ContainerElementName('\
                '                  "PropDataID"'\
                '                )'\
                '              )'\
                '            )'\
                '          )'\
                '        , FunctionAggregationOperator('\
                '            "None"'\
                '          )'\
                '        )'\
                '      , DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription('\
                '          ""'\
                '        )'\
                '      )'\
                '        , ContainerElementName("DS_PD_Alexa_RelatedLinks_Count")'\
                '    ))'
      elif type == 'ELQ':
        spec1='SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '      SpecLatticeFunction('\
              '        LatticeFunctionExpressionTransform('\
              '          LatticeFunctionExpression('\
              '            LatticeFunctionOperatorIdentifier('\
              '              "Plus"'\
              '            )'\
              '          , LatticeFunctionExpression('\
              '              LatticeFunctionOperatorIdentifier('\
              '                "Minus"'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "Length"'\
              '                )'\
              '              , LatticeFunctionIdentifier('\
              '                  ContainerElementNameTableQualifiedName('\
              '                    LatticeSourceTableIdentifier('\
              '                      ContainerElementName('\
              '                        "PD_DerivedColumns"'\
              '                      )'\
              '                    )'\
              '                  , ContainerElementName('\
              '                      "AlexaRelatedLinks"'\
              '                    )'\
              '                  )'\
              '                )'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "Length"'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Replace"'\
              '                  )'\
              '                , LatticeFunctionIdentifier('\
              '                    ContainerElementNameTableQualifiedName('\
              '                      LatticeSourceTableIdentifier('\
              '                        ContainerElementName('\
              '                          "PD_DerivedColumns"'\
              '                        )'\
              '                      )'\
              '                    , ContainerElementName('\
              '                        "AlexaRelatedLinks"'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpressionConstantScalar('\
              '                    ",", DataTypeNVarChar('\
              '                      1'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpressionConstantScalar('\
              '                    "", DataTypeNVarChar('\
              '                      0'\
              '                    )'\
              '                  )'\
              '                )'\
              '              )'\
              '            )'\
              '          , LatticeFunctionExpressionConstantScalar('\
              '              "1", DataTypeInt'\
              '            )'\
              '          )'\
              '        , LatticeAddressSetPi('\
              '            LatticeAddressExpressionAtomic('\
              '              LatticeAddressAtomicIdentifier('\
              '                ContainerElementName('\
              '                  "PropDataID"'\
              '                )'\
              '              )'\
              '            )'\
              '          )'\
              '        , FunctionAggregationOperator('\
              '            "None"'\
              '          )'\
              '        )'\
              '      , DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription('\
              '          ""'\
              '        )'\
              '      )'\
              '        , ContainerElementName("DS_PD_Alexa_RelatedLinks_Count")'\
              '    ))'
      else:
        spec1='SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '      SpecLatticeFunction('\
              '        LatticeFunctionExpressionTransform('\
              '          LatticeFunctionExpression('\
              '            LatticeFunctionOperatorIdentifier('\
              '              "Plus"'\
              '            )'\
              '          , LatticeFunctionExpression('\
              '              LatticeFunctionOperatorIdentifier('\
              '                "Minus"'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "Length"'\
              '                )'\
              '              , LatticeFunctionIdentifier('\
              '                  ContainerElementNameTableQualifiedName('\
              '                    LatticeSourceTableIdentifier('\
              '                      ContainerElementName('\
              '                        "PD_DerivedColumns"'\
              '                      )'\
              '                    )'\
              '                  , ContainerElementName('\
              '                      "AlexaRelatedLinks"'\
              '                    )'\
              '                  )'\
              '                )'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "Length"'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Replace"'\
              '                  )'\
              '                , LatticeFunctionIdentifier('\
              '                    ContainerElementNameTableQualifiedName('\
              '                      LatticeSourceTableIdentifier('\
              '                        ContainerElementName('\
              '                          "PD_DerivedColumns"'\
              '                        )'\
              '                      )'\
              '                    , ContainerElementName('\
              '                        "AlexaRelatedLinks"'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpressionConstantScalar('\
              '                    ",", DataTypeNVarChar('\
              '                      1'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpressionConstantScalar('\
              '                    "", DataTypeNVarChar('\
              '                      0'\
              '                    )'\
              '                  )'\
              '                )'\
              '              )'\
              '            )'\
              '          , LatticeFunctionExpressionConstantScalar('\
              '              "1", DataTypeInt'\
              '            )'\
              '          )'\
              '        , LatticeAddressSetPi('\
              '            LatticeAddressExpressionAtomic('\
              '              LatticeAddressAtomicIdentifier('\
              '                ContainerElementName('\
              '                  "PropDataID"'\
              '                )'\
              '              )'\
              '            )'\
              '          )'\
              '        , FunctionAggregationOperator('\
              '            "None"'\
              '          )'\
              '        )'\
              '      , DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription('\
              '          ""'\
              '        )'\
              '      )'\
              '        , ContainerElementName("DS_PD_Alexa_RelatedLinks_Count")'\
              '    ))'

      DS_PD_Alexa_RelatedLinks_Count=liaison.ExpressionVDBImplGeneric(spec1)
      conn_mgr.setSpec('DS_PD_Alexa_RelatedLinks_Count',DS_PD_Alexa_RelatedLinks_Count.definition())

      #Modify DS_PD_JobsTrendString_Ordered
      if type == 'MKTO':
          spec2='SpecLatticeNamedElements(SpecLatticeNamedElement('\
                '      SpecLatticeFunction('\
                '        LatticeFunctionExpressionTransform('\
                '          LatticeFunctionExpression('\
                '            LatticeFunctionOperatorIdentifier('\
                '              "IF"'\
                '            )'\
                '          , LatticeFunctionExpression('\
                '              LatticeFunctionOperatorIdentifier('\
                '                "IsNullValue"'\
                '              )'\
                '            , LatticeFunctionIdentifier('\
                '                ContainerElementNameTableQualifiedName('\
                '                  LatticeSourceTableIdentifier('\
                '                    ContainerElementName('\
                '                      "PD_DerivedColumns"'\
                '                    )'\
                '                  )'\
                '                , ContainerElementName('\
                '                    "JobsTrendString"'\
                '                  )'\
                '                )'\
                '              )'\
                '            )'\
                '          , LatticeFunctionExpressionConstantNull('\
                '              DataTypeInt'\
                '            )'\
                '          , LatticeFunctionExpression('\
                '              LatticeFunctionOperatorIdentifier('\
                '                "IF"'\
                '              )'\
                '            , LatticeFunctionExpression('\
                '                LatticeFunctionOperatorIdentifier('\
                '                  "Equal"'\
                '                )'\
                '              , LatticeFunctionExpression('\
                '                  LatticeFunctionOperatorIdentifier('\
                '                    "Lower"'\
                '                  )'\
                '                , LatticeFunctionIdentifier('\
                '                    ContainerElementNameTableQualifiedName('\
                '                      LatticeSourceTableIdentifier('\
                '                        ContainerElementName('\
                '                          "PD_DerivedColumns"'\
                '                        )'\
                '                      )'\
                '                    , ContainerElementName('\
                '                        "JobsTrendString"'\
                '                      )'\
                '                    )'\
                '                  )'\
                '                )'\
                '              , LatticeFunctionExpression('\
                '                  LatticeFunctionOperatorIdentifier('\
                '                    "Lower"'\
                '                  )'\
                '                , LatticeFunctionExpressionConstant('\
                '                    "Moderately Hiring", DataTypeNVarChar('\
                '                      17'\
                '                    )'\
                '                  )'\
                '                )'\
                '              )'\
                '            , LatticeFunctionExpressionConstantScalar('\
                '                "1", DataTypeInt'\
                '              )'\
                '            , LatticeFunctionExpression('\
                '                LatticeFunctionOperatorIdentifier('\
                '                  "IF"'\
                '                )'\
                '              , LatticeFunctionExpression('\
                '                  LatticeFunctionOperatorIdentifier('\
                '                    "Equal"'\
                '                  )'\
                '                , LatticeFunctionExpression('\
                '                    LatticeFunctionOperatorIdentifier('\
                '                      "Lower"'\
                '                    )'\
                '                  , LatticeFunctionIdentifier('\
                '                      ContainerElementNameTableQualifiedName('\
                '                        LatticeSourceTableIdentifier('\
                '                          ContainerElementName('\
                '                            "PD_DerivedColumns"'\
                '                          )'\
                '                        )'\
                '                      , ContainerElementName('\
                '                          "JobsTrendString"'\
                '                        )'\
                '                      )'\
                '                    )'\
                '                  )'\
                '                , LatticeFunctionExpression('\
                '                    LatticeFunctionOperatorIdentifier('\
                '                      "Lower"'\
                '                    )'\
                '                  , LatticeFunctionExpressionConstant('\
                '                      "Significantly Hiring", DataTypeNVarChar('\
                '                        20'\
                '                      )'\
                '                    )'\
                '                  )'\
                '                )'\
                '              , LatticeFunctionExpressionConstantScalar('\
                '                  "2", DataTypeInt'\
                '                )'\
                '              , LatticeFunctionExpression('\
                '                  LatticeFunctionOperatorIdentifier('\
                '                    "IF"'\
                '                  )'\
                '                , LatticeFunctionExpression('\
                '                    LatticeFunctionOperatorIdentifier('\
                '                      "Equal"'\
                '                    )'\
                '                  , LatticeFunctionExpression('\
                '                      LatticeFunctionOperatorIdentifier('\
                '                        "Lower"'\
                '                      )'\
                '                    , LatticeFunctionIdentifier('\
                '                        ContainerElementNameTableQualifiedName('\
                '                          LatticeSourceTableIdentifier('\
                '                            ContainerElementName('\
                '                              "PD_DerivedColumns"'\
                '                            )'\
                '                          )'\
                '                        , ContainerElementName('\
                '                            "JobsTrendString"'\
                '                          )'\
                '                        )'\
                '                      )'\
                '                    )'\
                '                  , LatticeFunctionExpression('\
                '                      LatticeFunctionOperatorIdentifier('\
                '                        "Lower"'\
                '                      )'\
                '                    , LatticeFunctionExpressionConstant('\
                '                        "Aggressively Hiring", DataTypeNVarChar('\
                '                          19'\
                '                        )'\
                '                      )'\
                '                    )'\
                '                  )'\
                '                , LatticeFunctionExpressionConstantScalar('\
                '                    "3", DataTypeInt'\
                '                  )'\
                '                , LatticeFunctionExpressionConstantScalar('\
                '                    "0", DataTypeInt'\
                '                  )'\
                '                )'\
                '              )'\
                '            )'\
                '          )'\
                '        , LatticeAddressSetPi('\
                '            LatticeAddressExpressionAtomic('\
                '              LatticeAddressAtomicIdentifier('\
                '                ContainerElementName('\
                '                  "PropDataID"'\
                '                )'\
                '              )'\
                '            )'\
                '          )'\
                '        , FunctionAggregationOperator('\
                '            "None"'\
                '          )'\
                '        )'\
                '      , DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription('\
                '          ""'\
                '        )'\
                '      )'\
                '        , ContainerElementName("DS_PD_JobsTrendString_Ordered")'\
                '	))'
      elif type == 'ELQ':
        spec2='SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '      SpecLatticeFunction('\
              '        LatticeFunctionExpressionTransform('\
              '          LatticeFunctionExpression('\
              '            LatticeFunctionOperatorIdentifier('\
              '              "IF"'\
              '            )'\
              '          , LatticeFunctionExpression('\
              '              LatticeFunctionOperatorIdentifier('\
              '                "IsNullValue"'\
              '              )'\
              '            , LatticeFunctionIdentifier('\
              '                ContainerElementNameTableQualifiedName('\
              '                  LatticeSourceTableIdentifier('\
              '                    ContainerElementName('\
              '                      "PD_DerivedColumns"'\
              '                    )'\
              '                  )'\
              '                , ContainerElementName('\
              '                    "JobsTrendString"'\
              '                  )'\
              '                )'\
              '              )'\
              '            )'\
              '          , LatticeFunctionExpressionConstantNull('\
              '              DataTypeInt'\
              '            )'\
              '          , LatticeFunctionExpression('\
              '              LatticeFunctionOperatorIdentifier('\
              '                "IF"'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "Equal"'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Lower"'\
              '                  )'\
              '                , LatticeFunctionIdentifier('\
              '                    ContainerElementNameTableQualifiedName('\
              '                      LatticeSourceTableIdentifier('\
              '                        ContainerElementName('\
              '                          "PD_DerivedColumns"'\
              '                        )'\
              '                      )'\
              '                    , ContainerElementName('\
              '                        "JobsTrendString"'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Lower"'\
              '                  )'\
              '                , LatticeFunctionExpressionConstant('\
              '                    "Moderately Hiring", DataTypeNVarChar('\
              '                      17'\
              '                    )'\
              '                  )'\
              '                )'\
              '              )'\
              '            , LatticeFunctionExpressionConstantScalar('\
              '                "1", DataTypeInt'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "IF"'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Equal"'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Lower"'\
              '                    )'\
              '                  , LatticeFunctionIdentifier('\
              '                      ContainerElementNameTableQualifiedName('\
              '                        LatticeSourceTableIdentifier('\
              '                          ContainerElementName('\
              '                            "PD_DerivedColumns"'\
              '                          )'\
              '                        )'\
              '                      , ContainerElementName('\
              '                          "JobsTrendString"'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Lower"'\
              '                    )'\
              '                  , LatticeFunctionExpressionConstant('\
              '                      "Significantly Hiring", DataTypeNVarChar('\
              '                        20'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                )'\
              '              , LatticeFunctionExpressionConstantScalar('\
              '                  "2", DataTypeInt'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "IF"'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Equal"'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Lower"'\
              '                      )'\
              '                    , LatticeFunctionIdentifier('\
              '                        ContainerElementNameTableQualifiedName('\
              '                          LatticeSourceTableIdentifier('\
              '                            ContainerElementName('\
              '                              "PD_DerivedColumns"'\
              '                            )'\
              '                          )'\
              '                        , ContainerElementName('\
              '                            "JobsTrendString"'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Lower"'\
              '                      )'\
              '                    , LatticeFunctionExpressionConstant('\
              '                        "Aggressively Hiring", DataTypeNVarChar('\
              '                          19'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpressionConstantScalar('\
              '                    "3", DataTypeInt'\
              '                  )'\
              '                , LatticeFunctionExpressionConstantScalar('\
              '                    "0", DataTypeInt'\
              '                  )'\
              '                )'\
              '              )'\
              '            )'\
              '          )'\
              '        , LatticeAddressSetPi('\
              '            LatticeAddressExpressionAtomic('\
              '              LatticeAddressAtomicIdentifier('\
              '                ContainerElementName('\
              '                  "PropDataID"'\
              '                )'\
              '              )'\
              '            )'\
              '          )'\
              '        , FunctionAggregationOperator('\
              '            "None"'\
              '          )'\
              '        )'\
              '      , DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription('\
              '          ""'\
              '        )'\
              '      )'\
              '        , ContainerElementName("DS_PD_JobsTrendString_Ordered")'\
              '    ))'
      else:
        spec2='SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '      SpecLatticeFunction('\
              '        LatticeFunctionExpressionTransform('\
              '          LatticeFunctionExpression('\
              '            LatticeFunctionOperatorIdentifier('\
              '              "IF"'\
              '            )'\
              '          , LatticeFunctionExpression('\
              '              LatticeFunctionOperatorIdentifier('\
              '                "IsNullValue"'\
              '              )'\
              '            , LatticeFunctionIdentifier('\
              '                ContainerElementNameTableQualifiedName('\
              '                  LatticeSourceTableIdentifier('\
              '                    ContainerElementName('\
              '                      "PD_DerivedColumns"'\
              '                    )'\
              '                  )'\
              '                , ContainerElementName('\
              '                    "JobsTrendString"'\
              '                  )'\
              '                )'\
              '              )'\
              '            )'\
              '          , LatticeFunctionExpressionConstantNull('\
              '              DataTypeInt'\
              '            )'\
              '          , LatticeFunctionExpression('\
              '              LatticeFunctionOperatorIdentifier('\
              '                "IF"'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "Equal"'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Lower"'\
              '                  )'\
              '                , LatticeFunctionIdentifier('\
              '                    ContainerElementNameTableQualifiedName('\
              '                      LatticeSourceTableIdentifier('\
              '                        ContainerElementName('\
              '                          "PD_DerivedColumns"'\
              '                        )'\
              '                      )'\
              '                    , ContainerElementName('\
              '                        "JobsTrendString"'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Lower"'\
              '                  )'\
              '                , LatticeFunctionExpressionConstant('\
              '                    "Moderately Hiring", DataTypeNVarChar('\
              '                      17'\
              '                    )'\
              '                  )'\
              '                )'\
              '              )'\
              '            , LatticeFunctionExpressionConstantScalar('\
              '                "1", DataTypeInt'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "IF"'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Equal"'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Lower"'\
              '                    )'\
              '                  , LatticeFunctionIdentifier('\
              '                      ContainerElementNameTableQualifiedName('\
              '                        LatticeSourceTableIdentifier('\
              '                          ContainerElementName('\
              '                            "PD_DerivedColumns"'\
              '                          )'\
              '                        )'\
              '                      , ContainerElementName('\
              '                          "JobsTrendString"'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Lower"'\
              '                    )'\
              '                  , LatticeFunctionExpressionConstant('\
              '                      "Significantly Hiring", DataTypeNVarChar('\
              '                        20'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                )'\
              '              , LatticeFunctionExpressionConstantScalar('\
              '                  "2", DataTypeInt'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "IF"'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Equal"'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Lower"'\
              '                      )'\
              '                    , LatticeFunctionIdentifier('\
              '                        ContainerElementNameTableQualifiedName('\
              '                          LatticeSourceTableIdentifier('\
              '                            ContainerElementName('\
              '                              "PD_DerivedColumns"'\
              '                            )'\
              '                          )'\
              '                        , ContainerElementName('\
              '                            "JobsTrendString"'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Lower"'\
              '                      )'\
              '                    , LatticeFunctionExpressionConstant('\
              '                        "Aggressively Hiring", DataTypeNVarChar('\
              '                          19'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpressionConstantScalar('\
              '                    "3", DataTypeInt'\
              '                  )'\
              '                , LatticeFunctionExpressionConstantScalar('\
              '                    "0", DataTypeInt'\
              '                  )'\
              '                )'\
              '              )'\
              '            )'\
              '          )'\
              '        , LatticeAddressSetPi('\
              '            LatticeAddressExpressionAtomic('\
              '              LatticeAddressAtomicIdentifier('\
              '                ContainerElementName('\
              '                  "PropDataID"'\
              '                )'\
              '              )'\
              '            )'\
              '          )'\
              '        , FunctionAggregationOperator('\
              '            "None"'\
              '          )'\
              '        )'\
              '      , DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription('\
              '          ""'\
              '        )'\
              '      )'\
              '        , ContainerElementName("DS_PD_JobsTrendString_Ordered")'\
              '    ))'

      DS_PD_JobsTrendString_Ordered=liaison.ExpressionVDBImplGeneric(spec2)
      conn_mgr.setSpec('DS_PD_JobsTrendString_Ordered',DS_PD_JobsTrendString_Ordered.definition())

      #Modify DS_PD_FundingStage_Ordered
      if type == 'MKTO':
          spec3='SpecLatticeNamedElements(SpecLatticeNamedElement('\
                '      SpecLatticeFunction('\
                '        LatticeFunctionExpressionTransform('\
                '          LatticeFunctionExpression('\
                '            LatticeFunctionOperatorIdentifier('\
                '              "IF"'\
                '            )'\
                '          , LatticeFunctionExpression('\
                '              LatticeFunctionOperatorIdentifier('\
                '                "IsNullValue"'\
                '              )'\
                '            , LatticeFunctionIdentifier('\
                '                ContainerElementNameTableQualifiedName('\
                '                  LatticeSourceTableIdentifier('\
                '                    ContainerElementName('\
                '                      "PD_DerivedColumns"'\
                '                    )'\
                '                  )'\
                '                , ContainerElementName('\
                '                    "FundingStage"'\
                '                  )'\
                '                )'\
                '              )'\
                '            )'\
                '          , LatticeFunctionExpressionConstantNull('\
                '              DataTypeInt'\
                '            )'\
                '          , LatticeFunctionExpression('\
                '              LatticeFunctionOperatorIdentifier('\
                '                "IF"'\
                '              )'\
                '            , LatticeFunctionExpression('\
                '                LatticeFunctionOperatorIdentifier('\
                '                  "Equal"'\
                '                )'\
                '              , LatticeFunctionExpression('\
                '                  LatticeFunctionOperatorIdentifier('\
                '                    "Lower"'\
                '                  )'\
                '                , LatticeFunctionIdentifier('\
                '                    ContainerElementNameTableQualifiedName('\
                '                      LatticeSourceTableIdentifier('\
                '                        ContainerElementName('\
                '                          "PD_DerivedColumns"'\
                '                        )'\
                '                      )'\
                '                    , ContainerElementName('\
                '                        "FundingStage"'\
                '                      )'\
                '                    )'\
                '                  )'\
                '                )'\
                '              , LatticeFunctionExpression('\
                '                  LatticeFunctionOperatorIdentifier('\
                '                    "Lower"'\
                '                  )'\
                '                , LatticeFunctionExpressionConstant('\
                '                    "Startup/Seed", DataTypeNVarChar('\
                '                      12'\
                '                    )'\
                '                  )'\
                '                )'\
                '              )'\
                '            , LatticeFunctionExpressionConstantScalar('\
                '                "1", DataTypeInt'\
                '              )'\
                '            , LatticeFunctionExpression('\
                '                LatticeFunctionOperatorIdentifier('\
                '                  "IF"'\
                '                )'\
                '              , LatticeFunctionExpression('\
                '                  LatticeFunctionOperatorIdentifier('\
                '                    "Equal"'\
                '                  )'\
                '                , LatticeFunctionExpression('\
                '                    LatticeFunctionOperatorIdentifier('\
                '                      "Lower"'\
                '                    )'\
                '                  , LatticeFunctionIdentifier('\
                '                      ContainerElementNameTableQualifiedName('\
                '                        LatticeSourceTableIdentifier('\
                '                          ContainerElementName('\
                '                            "PD_DerivedColumns"'\
                '                          )'\
                '                        )'\
                '                      , ContainerElementName('\
                '                          "FundingStage"'\
                '                        )'\
                '                      )'\
                '                    )'\
                '                  )'\
                '                , LatticeFunctionExpression('\
                '                    LatticeFunctionOperatorIdentifier('\
                '                      "Lower"'\
                '                    )'\
                '                  , LatticeFunctionExpressionConstant('\
                '                      "Early Stage", DataTypeNVarChar('\
                '                        11'\
                '                      )'\
                '                    )'\
                '                  )'\
                '                )'\
                '              , LatticeFunctionExpressionConstantScalar('\
                '                  "2", DataTypeInt'\
                '                )'\
                '              , LatticeFunctionExpression('\
                '                  LatticeFunctionOperatorIdentifier('\
                '                    "IF"'\
                '                  )'\
                '                , LatticeFunctionExpression('\
                '                    LatticeFunctionOperatorIdentifier('\
                '                      "Equal"'\
                '                    )'\
                '                  , LatticeFunctionExpression('\
                '                      LatticeFunctionOperatorIdentifier('\
                '                        "Lower"'\
                '                      )'\
                '                    , LatticeFunctionIdentifier('\
                '                        ContainerElementNameTableQualifiedName('\
                '                          LatticeSourceTableIdentifier('\
                '                            ContainerElementName('\
                '                              "PD_DerivedColumns"'\
                '                            )'\
                '                          )'\
                '                        , ContainerElementName('\
                '                            "FundingStage"'\
                '                          )'\
                '                        )'\
                '                      )'\
                '                    )'\
                '                  , LatticeFunctionExpression('\
                '                      LatticeFunctionOperatorIdentifier('\
                '                        "Lower"'\
                '                      )'\
                '                    , LatticeFunctionExpressionConstant('\
                '                        "Expansion", DataTypeNVarChar('\
                '                          9'\
                '                        )'\
                '                      )'\
                '                    )'\
                '                  )'\
                '                , LatticeFunctionExpressionConstantScalar('\
                '                    "3", DataTypeInt'\
                '                  )'\
                '                , LatticeFunctionExpression('\
                '                    LatticeFunctionOperatorIdentifier('\
                '                      "IF"'\
                '                    )'\
                '                  , LatticeFunctionExpression('\
                '                      LatticeFunctionOperatorIdentifier('\
                '                        "Equal"'\
                '                      )'\
                '                    , LatticeFunctionExpression('\
                '                        LatticeFunctionOperatorIdentifier('\
                '                          "Lower"'\
                '                        )'\
                '                      , LatticeFunctionIdentifier('\
                '                          ContainerElementNameTableQualifiedName('\
                '                            LatticeSourceTableIdentifier('\
                '                              ContainerElementName('\
                '                                "PD_DerivedColumns"'\
                '                              )'\
                '                            )'\
                '                          , ContainerElementName('\
                '                              "FundingStage"'\
                '                            )'\
                '                          )'\
                '                        )'\
                '                      )'\
                '                    , LatticeFunctionExpression('\
                '                        LatticeFunctionOperatorIdentifier('\
                '                          "Lower"'\
                '                        )'\
                '                      , LatticeFunctionExpressionConstant('\
                '                          "Later Stage", DataTypeNVarChar('\
                '                            11'\
                '                          )'\
                '                        )'\
                '                      )'\
                '                    )'\
                '                  , LatticeFunctionExpressionConstantScalar('\
                '                      "4", DataTypeInt'\
                '                    )'\
                '                  , LatticeFunctionExpressionConstantScalar('\
                '                      "0", DataTypeInt'\
                '                    )'\
                '                  )'\
                '                )'\
                '              )'\
                '            )'\
                '          )'\
                '        , LatticeAddressSetPi('\
                '            LatticeAddressExpressionAtomic('\
                '              LatticeAddressAtomicIdentifier('\
                '                ContainerElementName('\
                '                  "PropDataID"'\
                '                )'\
                '              )'\
                '            )'\
                '          )'\
                '        , FunctionAggregationOperator('\
                '            "None"'\
                '          )'\
                '        )'\
                '      , DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription('\
                '          ""'\
                '        )'\
                '      )'\
                '        , ContainerElementName("DS_PD_FundingStage_Ordered")'\
                '    ))'
      elif type == 'ELQ':
        spec3='SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '      SpecLatticeFunction('\
              '        LatticeFunctionExpressionTransform('\
              '          LatticeFunctionExpression('\
              '            LatticeFunctionOperatorIdentifier('\
              '              "IF"'\
              '            )'\
              '          , LatticeFunctionExpression('\
              '              LatticeFunctionOperatorIdentifier('\
              '                "IsNullValue"'\
              '              )'\
              '            , LatticeFunctionIdentifier('\
              '                ContainerElementNameTableQualifiedName('\
              '                  LatticeSourceTableIdentifier('\
              '                    ContainerElementName('\
              '                      "PD_DerivedColumns"'\
              '                    )'\
              '                  )'\
              '                , ContainerElementName('\
              '                    "FundingStage"'\
              '                  )'\
              '                )'\
              '              )'\
              '            )'\
              '          , LatticeFunctionExpressionConstantNull('\
              '              DataTypeInt'\
              '            )'\
              '          , LatticeFunctionExpression('\
              '              LatticeFunctionOperatorIdentifier('\
              '                "IF"'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "Equal"'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Lower"'\
              '                  )'\
              '                , LatticeFunctionIdentifier('\
              '                    ContainerElementNameTableQualifiedName('\
              '                      LatticeSourceTableIdentifier('\
              '                        ContainerElementName('\
              '                          "PD_DerivedColumns"'\
              '                        )'\
              '                      )'\
              '                    , ContainerElementName('\
              '                        "FundingStage"'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Lower"'\
              '                  )'\
              '                , LatticeFunctionExpressionConstant('\
              '                    "Startup/Seed", DataTypeNVarChar('\
              '                      12'\
              '                    )'\
              '                  )'\
              '                )'\
              '              )'\
              '            , LatticeFunctionExpressionConstantScalar('\
              '                "1", DataTypeInt'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "IF"'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Equal"'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Lower"'\
              '                    )'\
              '                  , LatticeFunctionIdentifier('\
              '                      ContainerElementNameTableQualifiedName('\
              '                        LatticeSourceTableIdentifier('\
              '                          ContainerElementName('\
              '                            "PD_DerivedColumns"'\
              '                          )'\
              '                        )'\
              '                      , ContainerElementName('\
              '                          "FundingStage"'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Lower"'\
              '                    )'\
              '                  , LatticeFunctionExpressionConstant('\
              '                      "Early Stage", DataTypeNVarChar('\
              '                        11'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                )'\
              '              , LatticeFunctionExpressionConstantScalar('\
              '                  "2", DataTypeInt'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "IF"'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Equal"'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Lower"'\
              '                      )'\
              '                    , LatticeFunctionIdentifier('\
              '                        ContainerElementNameTableQualifiedName('\
              '                          LatticeSourceTableIdentifier('\
              '                            ContainerElementName('\
              '                              "PD_DerivedColumns"'\
              '                            )'\
              '                          )'\
              '                        , ContainerElementName('\
              '                            "FundingStage"'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Lower"'\
              '                      )'\
              '                    , LatticeFunctionExpressionConstant('\
              '                        "Expansion", DataTypeNVarChar('\
              '                          9'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpressionConstantScalar('\
              '                    "3", DataTypeInt'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "IF"'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Equal"'\
              '                      )'\
              '                    , LatticeFunctionExpression('\
              '                        LatticeFunctionOperatorIdentifier('\
              '                          "Lower"'\
              '                        )'\
              '                      , LatticeFunctionIdentifier('\
              '                          ContainerElementNameTableQualifiedName('\
              '                            LatticeSourceTableIdentifier('\
              '                              ContainerElementName('\
              '                                "PD_DerivedColumns"'\
              '                              )'\
              '                            )'\
              '                          , ContainerElementName('\
              '                              "FundingStage"'\
              '                            )'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    , LatticeFunctionExpression('\
              '                        LatticeFunctionOperatorIdentifier('\
              '                          "Lower"'\
              '                        )'\
              '                      , LatticeFunctionExpressionConstant('\
              '                          "Later Stage", DataTypeNVarChar('\
              '                            11'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  , LatticeFunctionExpressionConstantScalar('\
              '                      "4", DataTypeInt'\
              '                    )'\
              '                  , LatticeFunctionExpressionConstantScalar('\
              '                      "0", DataTypeInt'\
              '                    )'\
              '                  )'\
              '                )'\
              '              )'\
              '            )'\
              '          )'\
              '        , LatticeAddressSetPi('\
              '            LatticeAddressExpressionAtomic('\
              '              LatticeAddressAtomicIdentifier('\
              '                ContainerElementName('\
              '                  "PropDataID"'\
              '                )'\
              '              )'\
              '            )'\
              '          )'\
              '        , FunctionAggregationOperator('\
              '            "None"'\
              '          )'\
              '        )'\
              '      , DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription('\
              '          ""'\
              '        )'\
              '      )'\
              '        , ContainerElementName("DS_PD_FundingStage_Ordered")'\
              '    ))'
      else:
        spec3='SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '      SpecLatticeFunction('\
              '        LatticeFunctionExpressionTransform('\
              '          LatticeFunctionExpression('\
              '            LatticeFunctionOperatorIdentifier('\
              '              "IF"'\
              '            )'\
              '          , LatticeFunctionExpression('\
              '              LatticeFunctionOperatorIdentifier('\
              '                "IsNullValue"'\
              '              )'\
              '            , LatticeFunctionIdentifier('\
              '                ContainerElementNameTableQualifiedName('\
              '                  LatticeSourceTableIdentifier('\
              '                    ContainerElementName('\
              '                      "PD_DerivedColumns"'\
              '                    )'\
              '                  )'\
              '                , ContainerElementName('\
              '                    "FundingStage"'\
              '                  )'\
              '                )'\
              '              )'\
              '            )'\
              '          , LatticeFunctionExpressionConstantNull('\
              '              DataTypeInt'\
              '            )'\
              '          , LatticeFunctionExpression('\
              '              LatticeFunctionOperatorIdentifier('\
              '                "IF"'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "Equal"'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Lower"'\
              '                  )'\
              '                , LatticeFunctionIdentifier('\
              '                    ContainerElementNameTableQualifiedName('\
              '                      LatticeSourceTableIdentifier('\
              '                        ContainerElementName('\
              '                          "PD_DerivedColumns"'\
              '                        )'\
              '                      )'\
              '                    , ContainerElementName('\
              '                        "FundingStage"'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Lower"'\
              '                  )'\
              '                , LatticeFunctionExpressionConstant('\
              '                    "Startup/Seed", DataTypeNVarChar('\
              '                      12'\
              '                    )'\
              '                  )'\
              '                )'\
              '              )'\
              '            , LatticeFunctionExpressionConstantScalar('\
              '                "1", DataTypeInt'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "IF"'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Equal"'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Lower"'\
              '                    )'\
              '                  , LatticeFunctionIdentifier('\
              '                      ContainerElementNameTableQualifiedName('\
              '                        LatticeSourceTableIdentifier('\
              '                          ContainerElementName('\
              '                            "PD_DerivedColumns"'\
              '                          )'\
              '                        )'\
              '                      , ContainerElementName('\
              '                          "FundingStage"'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Lower"'\
              '                    )'\
              '                  , LatticeFunctionExpressionConstant('\
              '                      "Early Stage", DataTypeNVarChar('\
              '                        11'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                )'\
              '              , LatticeFunctionExpressionConstantScalar('\
              '                  "2", DataTypeInt'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "IF"'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Equal"'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Lower"'\
              '                      )'\
              '                    , LatticeFunctionIdentifier('\
              '                        ContainerElementNameTableQualifiedName('\
              '                          LatticeSourceTableIdentifier('\
              '                            ContainerElementName('\
              '                              "PD_DerivedColumns"'\
              '                            )'\
              '                          )'\
              '                        , ContainerElementName('\
              '                            "FundingStage"'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Lower"'\
              '                      )'\
              '                    , LatticeFunctionExpressionConstant('\
              '                        "Expansion", DataTypeNVarChar('\
              '                          9'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpressionConstantScalar('\
              '                    "3", DataTypeInt'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "IF"'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Equal"'\
              '                      )'\
              '                    , LatticeFunctionExpression('\
              '                        LatticeFunctionOperatorIdentifier('\
              '                          "Lower"'\
              '                        )'\
              '                      , LatticeFunctionIdentifier('\
              '                          ContainerElementNameTableQualifiedName('\
              '                            LatticeSourceTableIdentifier('\
              '                              ContainerElementName('\
              '                                "PD_DerivedColumns"'\
              '                              )'\
              '                            )'\
              '                          , ContainerElementName('\
              '                              "FundingStage"'\
              '                            )'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    , LatticeFunctionExpression('\
              '                        LatticeFunctionOperatorIdentifier('\
              '                          "Lower"'\
              '                        )'\
              '                      , LatticeFunctionExpressionConstant('\
              '                          "Later Stage", DataTypeNVarChar('\
              '                            11'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  , LatticeFunctionExpressionConstantScalar('\
              '                      "4", DataTypeInt'\
              '                    )'\
              '                  , LatticeFunctionExpressionConstantScalar('\
              '                      "0", DataTypeInt'\
              '                    )'\
              '                  )'\
              '                )'\
              '              )'\
              '            )'\
              '          )'\
              '        , LatticeAddressSetPi('\
              '            LatticeAddressExpressionAtomic('\
              '              LatticeAddressAtomicIdentifier('\
              '                ContainerElementName('\
              '                  "PropDataID"'\
              '                )'\
              '              )'\
              '            )'\
              '          )'\
              '        , FunctionAggregationOperator('\
              '            "None"'\
              '          )'\
              '        )'\
              '      , DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription('\
              '          ""'\
              '        )'\
              '      )'\
              '        , ContainerElementName("DS_PD_FundingStage_Ordered")'\
              '    ))'

      DS_PD_Alexa_RelatedLinks_Count=liaison.ExpressionVDBImplGeneric(spec3)
      conn_mgr.setSpec('DS_PD_FundingStage_Ordered',DS_PD_Alexa_RelatedLinks_Count.definition())

    #Modify DS_PD_ModelAction_Ordered
      if type == 'MKTO':
          spec4='SpecLatticeNamedElements(SpecLatticeNamedElement('\
                '      SpecLatticeFunction('\
                '        LatticeFunctionExpressionTransform('\
                '          LatticeFunctionExpression('\
                '            LatticeFunctionOperatorIdentifier('\
                '              "IF"'\
                '            )'\
                '          , LatticeFunctionExpression('\
                '              LatticeFunctionOperatorIdentifier('\
                '                "IsNullValue"'\
                '              )'\
                '            , LatticeFunctionIdentifier('\
                '                ContainerElementNameTableQualifiedName('\
                '                  LatticeSourceTableIdentifier('\
                '                    ContainerElementName('\
                '                      "PD_DerivedColumns"'\
                '                    )'\
                '                  )'\
                '                , ContainerElementName('\
                '                    "ModelAction"'\
                '                  )'\
                '                )'\
                '              )'\
                '            )'\
                '          , LatticeFunctionExpressionConstantNull('\
                '              DataTypeInt'\
                '            )'\
                '          , LatticeFunctionExpression('\
                '              LatticeFunctionOperatorIdentifier('\
                '                "IF"'\
                '              )'\
                '            , LatticeFunctionExpression('\
                '                LatticeFunctionOperatorIdentifier('\
                '                  "Equal"'\
                '                )'\
                '              , LatticeFunctionExpression('\
                '                  LatticeFunctionOperatorIdentifier('\
                '                    "Lower"'\
                '                  )'\
                '                , LatticeFunctionIdentifier('\
                '                    ContainerElementNameTableQualifiedName('\
                '                      LatticeSourceTableIdentifier('\
                '                        ContainerElementName('\
                '                          "PD_DerivedColumns"'\
                '                        )'\
                '                      )'\
                '                    , ContainerElementName('\
                '                        "ModelAction"'\
                '                      )'\
                '                    )'\
                '                  )'\
                '                )'\
                '              , LatticeFunctionExpression('\
                '                  LatticeFunctionOperatorIdentifier('\
                '                    "Lower"'\
                '                  )'\
                '                , LatticeFunctionExpressionConstant('\
                '                    "LOW RISK", DataTypeNVarChar('\
                '                      8'\
                '                    )'\
                '                  )'\
                '                )'\
                '              )'\
                '            , LatticeFunctionExpressionConstantScalar('\
                '                "1", DataTypeInt'\
                '              )'\
                '            , LatticeFunctionExpression('\
                '                LatticeFunctionOperatorIdentifier('\
                '                  "IF"'\
                '                )'\
                '              , LatticeFunctionExpression('\
                '                  LatticeFunctionOperatorIdentifier('\
                '                    "Equal"'\
                '                  )'\
                '                , LatticeFunctionExpression('\
                '                    LatticeFunctionOperatorIdentifier('\
                '                      "Lower"'\
                '                    )'\
                '                  , LatticeFunctionIdentifier('\
                '                      ContainerElementNameTableQualifiedName('\
                '                        LatticeSourceTableIdentifier('\
                '                          ContainerElementName('\
                '                            "PD_DerivedColumns"'\
                '                          )'\
                '                        )'\
                '                      , ContainerElementName('\
                '                          "ModelAction"'\
                '                        )'\
                '                      )'\
                '                    )'\
                '                  )'\
                '                , LatticeFunctionExpression('\
                '                    LatticeFunctionOperatorIdentifier('\
                '                      "Lower"'\
                '                    )'\
                '                  , LatticeFunctionExpressionConstant('\
                '                      "LOW-MEDIUM RISK", DataTypeNVarChar('\
                '                        15'\
                '                      )'\
                '                    )'\
                '                  )'\
                '                )'\
                '              , LatticeFunctionExpressionConstantScalar('\
                '                  "2", DataTypeInt'\
                '                )'\
                '              , LatticeFunctionExpression('\
                '                  LatticeFunctionOperatorIdentifier('\
                '                    "IF"'\
                '                  )'\
                '                , LatticeFunctionExpression('\
                '                    LatticeFunctionOperatorIdentifier('\
                '                      "Equal"'\
                '                    )'\
                '                  , LatticeFunctionExpression('\
                '                      LatticeFunctionOperatorIdentifier('\
                '                        "Lower"'\
                '                      )'\
                '                    , LatticeFunctionIdentifier('\
                '                        ContainerElementNameTableQualifiedName('\
                '                          LatticeSourceTableIdentifier('\
                '                            ContainerElementName('\
                '                              "PD_DerivedColumns"'\
                '                            )'\
                '                          )'\
                '                        , ContainerElementName('\
                '                            "ModelAction"'\
                '                          )'\
                '                        )'\
                '                      )'\
                '                    )'\
                '                  , LatticeFunctionExpression('\
                '                      LatticeFunctionOperatorIdentifier('\
                '                        "Lower"'\
                '                      )'\
                '                    , LatticeFunctionExpressionConstant('\
                '                        "MEDIUM RISK", DataTypeNVarChar('\
                '                          11'\
                '                        )'\
                '                      )'\
                '                    )'\
                '                  )'\
                '                , LatticeFunctionExpressionConstantScalar('\
                '                    "3", DataTypeInt'\
                '                  )'\
                '                , LatticeFunctionExpression('\
                '                    LatticeFunctionOperatorIdentifier('\
                '                      "IF"'\
                '                    )'\
                '                  , LatticeFunctionExpression('\
                '                      LatticeFunctionOperatorIdentifier('\
                '                        "Equal"'\
                '                      )'\
                '                    , LatticeFunctionExpression('\
                '                        LatticeFunctionOperatorIdentifier('\
                '                          "Lower"'\
                '                        )'\
                '                      , LatticeFunctionIdentifier('\
                '                          ContainerElementNameTableQualifiedName('\
                '                            LatticeSourceTableIdentifier('\
                '                              ContainerElementName('\
                '                                "PD_DerivedColumns"'\
                '                              )'\
                '                            )'\
                '                          , ContainerElementName('\
                '                              "ModelAction"'\
                '                            )'\
                '                          )'\
                '                        )'\
                '                      )'\
                '                    , LatticeFunctionExpression('\
                '                        LatticeFunctionOperatorIdentifier('\
                '                          "Lower"'\
                '                        )'\
                '                      , LatticeFunctionExpressionConstant('\
                '                          "MEDIUM-HIGH RISK", DataTypeNVarChar('\
                '                            16'\
                '                          )'\
                '                        )'\
                '                      )'\
                '                    )'\
                '                  , LatticeFunctionExpressionConstantScalar('\
                '                      "4", DataTypeInt'\
                '                    )'\
                '                  , LatticeFunctionExpression('\
                '                      LatticeFunctionOperatorIdentifier('\
                '                        "IF"'\
                '                      )'\
                '                    , LatticeFunctionExpression('\
                '                        LatticeFunctionOperatorIdentifier('\
                '                          "Equal"'\
                '                        )'\
                '                      , LatticeFunctionExpression('\
                '                          LatticeFunctionOperatorIdentifier('\
                '                            "Lower"'\
                '                          )'\
                '                        , LatticeFunctionIdentifier('\
                '                            ContainerElementNameTableQualifiedName('\
                '                              LatticeSourceTableIdentifier('\
                '                                ContainerElementName('\
                '                                  "PD_DerivedColumns"'\
                '                                )'\
                '                              )'\
                '                            , ContainerElementName('\
                '                                "ModelAction"'\
                '                              )'\
                '                            )'\
                '                          )'\
                '                        )'\
                '                      , LatticeFunctionExpression('\
                '                          LatticeFunctionOperatorIdentifier('\
                '                            "Lower"'\
                '                          )'\
                '                        , LatticeFunctionExpressionConstant('\
                '                            "HIGH RISK", DataTypeNVarChar('\
                '                              9'\
                '                            )'\
                '                          )'\
                '                        )'\
                '                      )'\
                '                    , LatticeFunctionExpressionConstantScalar('\
                '                        "5", DataTypeInt'\
                '                      )'\
                '                    , LatticeFunctionExpression('\
                '                        LatticeFunctionOperatorIdentifier('\
                '                          "IF"'\
                '                        )'\
                '                      , LatticeFunctionExpression('\
                '                          LatticeFunctionOperatorIdentifier('\
                '                            "Equal"'\
                '                          )'\
                '                        , LatticeFunctionExpression('\
                '                            LatticeFunctionOperatorIdentifier('\
                '                              "Lower"'\
                '                            )'\
                '                          , LatticeFunctionIdentifier('\
                '                              ContainerElementNameTableQualifiedName('\
                '                                LatticeSourceTableIdentifier('\
                '                                  ContainerElementName('\
                '                                    "PD_DerivedColumns"'\
                '                                  )'\
                '                                )'\
                '                              , ContainerElementName('\
                '                                  "ModelAction"'\
                '                                )'\
                '                              )'\
                '                            )'\
                '                          )'\
                '                        , LatticeFunctionExpression('\
                '                            LatticeFunctionOperatorIdentifier('\
                '                              "Lower"'\
                '                            )'\
                '                          , LatticeFunctionExpressionConstant('\
                '                              "RECENT BANKRUPTCY ON FILE", DataTypeNVarChar('\
                '                                25'\
                '                              )'\
                '                            )'\
                '                          )'\
                '                        )'\
                '                      , LatticeFunctionExpressionConstantScalar('\
                '                          "6", DataTypeInt'\
                '                        )'\
                '                      , LatticeFunctionExpressionConstantScalar('\
                '                          "0", DataTypeInt'\
                '                        )'\
                '                      )'\
                '                    )'\
                '                  )'\
                '                )'\
                '              )'\
                '            )'\
                '          )'\
                '        , LatticeAddressSetPi('\
                '            LatticeAddressExpressionAtomic('\
                '              LatticeAddressAtomicIdentifier('\
                '                ContainerElementName('\
                '                  "PropDataID"'\
                '                )'\
                '              )'\
                '            )'\
                '          )'\
                '        , FunctionAggregationOperator('\
                '            "None"'\
                '          )'\
                '        )'\
                '      , DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription('\
                '          ""'\
                '        )'\
                '      )'\
                '        , ContainerElementName("DS_PD_ModelAction_Ordered")'\
                '    ))'
      elif type == 'ELQ':
        spec4='SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '      SpecLatticeFunction('\
              '        LatticeFunctionExpressionTransform('\
              '          LatticeFunctionExpression('\
              '            LatticeFunctionOperatorIdentifier('\
              '              "IF"'\
              '            )'\
              '          , LatticeFunctionExpression('\
              '              LatticeFunctionOperatorIdentifier('\
              '                "IsNullValue"'\
              '              )'\
              '            , LatticeFunctionIdentifier('\
              '                ContainerElementNameTableQualifiedName('\
              '                  LatticeSourceTableIdentifier('\
              '                    ContainerElementName('\
              '                      "PD_DerivedColumns"'\
              '                    )'\
              '                  )'\
              '                , ContainerElementName('\
              '                    "ModelAction"'\
              '                  )'\
              '                )'\
              '              )'\
              '            )'\
              '          , LatticeFunctionExpressionConstantNull('\
              '              DataTypeInt'\
              '            )'\
              '          , LatticeFunctionExpression('\
              '              LatticeFunctionOperatorIdentifier('\
              '                "IF"'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "Equal"'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Lower"'\
              '                  )'\
              '                , LatticeFunctionIdentifier('\
              '                    ContainerElementNameTableQualifiedName('\
              '                      LatticeSourceTableIdentifier('\
              '                        ContainerElementName('\
              '                          "PD_DerivedColumns"'\
              '                        )'\
              '                      )'\
              '                    , ContainerElementName('\
              '                        "ModelAction"'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Lower"'\
              '                  )'\
              '                , LatticeFunctionExpressionConstant('\
              '                    "LOW RISK", DataTypeNVarChar('\
              '                      8'\
              '                    )'\
              '                  )'\
              '                )'\
              '              )'\
              '            , LatticeFunctionExpressionConstantScalar('\
              '                "1", DataTypeInt'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "IF"'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Equal"'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Lower"'\
              '                    )'\
              '                  , LatticeFunctionIdentifier('\
              '                      ContainerElementNameTableQualifiedName('\
              '                        LatticeSourceTableIdentifier('\
              '                          ContainerElementName('\
              '                            "PD_DerivedColumns"'\
              '                          )'\
              '                        )'\
              '                      , ContainerElementName('\
              '                          "ModelAction"'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Lower"'\
              '                    )'\
              '                  , LatticeFunctionExpressionConstant('\
              '                      "LOW-MEDIUM RISK", DataTypeNVarChar('\
              '                        15'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                )'\
              '              , LatticeFunctionExpressionConstantScalar('\
              '                  "2", DataTypeInt'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "IF"'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Equal"'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Lower"'\
              '                      )'\
              '                    , LatticeFunctionIdentifier('\
              '                        ContainerElementNameTableQualifiedName('\
              '                          LatticeSourceTableIdentifier('\
              '                            ContainerElementName('\
              '                              "PD_DerivedColumns"'\
              '                            )'\
              '                          )'\
              '                        , ContainerElementName('\
              '                            "ModelAction"'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Lower"'\
              '                      )'\
              '                    , LatticeFunctionExpressionConstant('\
              '                        "MEDIUM RISK", DataTypeNVarChar('\
              '                          11'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpressionConstantScalar('\
              '                    "3", DataTypeInt'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "IF"'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Equal"'\
              '                      )'\
              '                    , LatticeFunctionExpression('\
              '                        LatticeFunctionOperatorIdentifier('\
              '                          "Lower"'\
              '                        )'\
              '                      , LatticeFunctionIdentifier('\
              '                          ContainerElementNameTableQualifiedName('\
              '                            LatticeSourceTableIdentifier('\
              '                              ContainerElementName('\
              '                                "PD_DerivedColumns"'\
              '                              )'\
              '                            )'\
              '                          , ContainerElementName('\
              '                              "ModelAction"'\
              '                            )'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    , LatticeFunctionExpression('\
              '                        LatticeFunctionOperatorIdentifier('\
              '                          "Lower"'\
              '                        )'\
              '                      , LatticeFunctionExpressionConstant('\
              '                          "MEDIUM-HIGH RISK", DataTypeNVarChar('\
              '                            16'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  , LatticeFunctionExpressionConstantScalar('\
              '                      "4", DataTypeInt'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "IF"'\
              '                      )'\
              '                    , LatticeFunctionExpression('\
              '                        LatticeFunctionOperatorIdentifier('\
              '                          "Equal"'\
              '                        )'\
              '                      , LatticeFunctionExpression('\
              '                          LatticeFunctionOperatorIdentifier('\
              '                            "Lower"'\
              '                          )'\
              '                        , LatticeFunctionIdentifier('\
              '                            ContainerElementNameTableQualifiedName('\
              '                              LatticeSourceTableIdentifier('\
              '                                ContainerElementName('\
              '                                  "PD_DerivedColumns"'\
              '                                )'\
              '                              )'\
              '                            , ContainerElementName('\
              '                                "ModelAction"'\
              '                              )'\
              '                            )'\
              '                          )'\
              '                        )'\
              '                      , LatticeFunctionExpression('\
              '                          LatticeFunctionOperatorIdentifier('\
              '                            "Lower"'\
              '                          )'\
              '                        , LatticeFunctionExpressionConstant('\
              '                            "HIGH RISK", DataTypeNVarChar('\
              '                              9'\
              '                            )'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    , LatticeFunctionExpressionConstantScalar('\
              '                        "5", DataTypeInt'\
              '                      )'\
              '                    , LatticeFunctionExpression('\
              '                        LatticeFunctionOperatorIdentifier('\
              '                          "IF"'\
              '                        )'\
              '                      , LatticeFunctionExpression('\
              '                          LatticeFunctionOperatorIdentifier('\
              '                            "Equal"'\
              '                          )'\
              '                        , LatticeFunctionExpression('\
              '                            LatticeFunctionOperatorIdentifier('\
              '                              "Lower"'\
              '                            )'\
              '                          , LatticeFunctionIdentifier('\
              '                              ContainerElementNameTableQualifiedName('\
              '                                LatticeSourceTableIdentifier('\
              '                                  ContainerElementName('\
              '                                    "PD_DerivedColumns"'\
              '                                  )'\
              '                                )'\
              '                              , ContainerElementName('\
              '                                  "ModelAction"'\
              '                                )'\
              '                              )'\
              '                            )'\
              '                          )'\
              '                        , LatticeFunctionExpression('\
              '                            LatticeFunctionOperatorIdentifier('\
              '                              "Lower"'\
              '                            )'\
              '                          , LatticeFunctionExpressionConstant('\
              '                              "RECENT BANKRUPTCY ON FILE", DataTypeNVarChar('\
              '                                25'\
              '                              )'\
              '                            )'\
              '                          )'\
              '                        )'\
              '                      , LatticeFunctionExpressionConstantScalar('\
              '                          "6", DataTypeInt'\
              '                        )'\
              '                      , LatticeFunctionExpressionConstantScalar('\
              '                          "0", DataTypeInt'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                )'\
              '              )'\
              '            )'\
              '          )'\
              '        , LatticeAddressSetPi('\
              '            LatticeAddressExpressionAtomic('\
              '              LatticeAddressAtomicIdentifier('\
              '                ContainerElementName('\
              '                  "PropDataID"'\
              '                )'\
              '              )'\
              '            )'\
              '          )'\
              '        , FunctionAggregationOperator('\
              '            "None"'\
              '          )'\
              '        )'\
              '      , DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription('\
              '          ""'\
              '        )'\
              '      )'\
              '        , ContainerElementName("DS_PD_ModelAction_Ordered")'\
              '    ))'
      else:
        spec4='SpecLatticeNamedElements(SpecLatticeNamedElement('\
              '      SpecLatticeFunction('\
              '        LatticeFunctionExpressionTransform('\
              '          LatticeFunctionExpression('\
              '            LatticeFunctionOperatorIdentifier('\
              '              "IF"'\
              '            )'\
              '          , LatticeFunctionExpression('\
              '              LatticeFunctionOperatorIdentifier('\
              '                "IsNullValue"'\
              '              )'\
              '            , LatticeFunctionIdentifier('\
              '                ContainerElementNameTableQualifiedName('\
              '                  LatticeSourceTableIdentifier('\
              '                    ContainerElementName('\
              '                      "PD_DerivedColumns"'\
              '                    )'\
              '                  )'\
              '                , ContainerElementName('\
              '                    "ModelAction"'\
              '                  )'\
              '                )'\
              '              )'\
              '            )'\
              '          , LatticeFunctionExpressionConstantNull('\
              '              DataTypeInt'\
              '            )'\
              '          , LatticeFunctionExpression('\
              '              LatticeFunctionOperatorIdentifier('\
              '                "IF"'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "Equal"'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Lower"'\
              '                  )'\
              '                , LatticeFunctionIdentifier('\
              '                    ContainerElementNameTableQualifiedName('\
              '                      LatticeSourceTableIdentifier('\
              '                        ContainerElementName('\
              '                          "PD_DerivedColumns"'\
              '                        )'\
              '                      )'\
              '                    , ContainerElementName('\
              '                        "ModelAction"'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Lower"'\
              '                  )'\
              '                , LatticeFunctionExpressionConstant('\
              '                    "LOW RISK", DataTypeNVarChar('\
              '                      8'\
              '                    )'\
              '                  )'\
              '                )'\
              '              )'\
              '            , LatticeFunctionExpressionConstantScalar('\
              '                "1", DataTypeInt'\
              '              )'\
              '            , LatticeFunctionExpression('\
              '                LatticeFunctionOperatorIdentifier('\
              '                  "IF"'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "Equal"'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Lower"'\
              '                    )'\
              '                  , LatticeFunctionIdentifier('\
              '                      ContainerElementNameTableQualifiedName('\
              '                        LatticeSourceTableIdentifier('\
              '                          ContainerElementName('\
              '                            "PD_DerivedColumns"'\
              '                          )'\
              '                        )'\
              '                      , ContainerElementName('\
              '                          "ModelAction"'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Lower"'\
              '                    )'\
              '                  , LatticeFunctionExpressionConstant('\
              '                      "LOW-MEDIUM RISK", DataTypeNVarChar('\
              '                        15'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                )'\
              '              , LatticeFunctionExpressionConstantScalar('\
              '                  "2", DataTypeInt'\
              '                )'\
              '              , LatticeFunctionExpression('\
              '                  LatticeFunctionOperatorIdentifier('\
              '                    "IF"'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "Equal"'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Lower"'\
              '                      )'\
              '                    , LatticeFunctionIdentifier('\
              '                        ContainerElementNameTableQualifiedName('\
              '                          LatticeSourceTableIdentifier('\
              '                            ContainerElementName('\
              '                              "PD_DerivedColumns"'\
              '                            )'\
              '                          )'\
              '                        , ContainerElementName('\
              '                            "ModelAction"'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Lower"'\
              '                      )'\
              '                    , LatticeFunctionExpressionConstant('\
              '                        "MEDIUM RISK", DataTypeNVarChar('\
              '                          11'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                , LatticeFunctionExpressionConstantScalar('\
              '                    "3", DataTypeInt'\
              '                  )'\
              '                , LatticeFunctionExpression('\
              '                    LatticeFunctionOperatorIdentifier('\
              '                      "IF"'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "Equal"'\
              '                      )'\
              '                    , LatticeFunctionExpression('\
              '                        LatticeFunctionOperatorIdentifier('\
              '                          "Lower"'\
              '                        )'\
              '                      , LatticeFunctionIdentifier('\
              '                          ContainerElementNameTableQualifiedName('\
              '                            LatticeSourceTableIdentifier('\
              '                              ContainerElementName('\
              '                                "PD_DerivedColumns"'\
              '                              )'\
              '                            )'\
              '                          , ContainerElementName('\
              '                              "ModelAction"'\
              '                            )'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    , LatticeFunctionExpression('\
              '                        LatticeFunctionOperatorIdentifier('\
              '                          "Lower"'\
              '                        )'\
              '                      , LatticeFunctionExpressionConstant('\
              '                          "MEDIUM-HIGH RISK", DataTypeNVarChar('\
              '                            16'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  , LatticeFunctionExpressionConstantScalar('\
              '                      "4", DataTypeInt'\
              '                    )'\
              '                  , LatticeFunctionExpression('\
              '                      LatticeFunctionOperatorIdentifier('\
              '                        "IF"'\
              '                      )'\
              '                    , LatticeFunctionExpression('\
              '                        LatticeFunctionOperatorIdentifier('\
              '                          "Equal"'\
              '                        )'\
              '                      , LatticeFunctionExpression('\
              '                          LatticeFunctionOperatorIdentifier('\
              '                            "Lower"'\
              '                          )'\
              '                        , LatticeFunctionIdentifier('\
              '                            ContainerElementNameTableQualifiedName('\
              '                              LatticeSourceTableIdentifier('\
              '                                ContainerElementName('\
              '                                  "PD_DerivedColumns"'\
              '                                )'\
              '                              )'\
              '                            , ContainerElementName('\
              '                                "ModelAction"'\
              '                              )'\
              '                            )'\
              '                          )'\
              '                        )'\
              '                      , LatticeFunctionExpression('\
              '                          LatticeFunctionOperatorIdentifier('\
              '                            "Lower"'\
              '                          )'\
              '                        , LatticeFunctionExpressionConstant('\
              '                            "HIGH RISK", DataTypeNVarChar('\
              '                              9'\
              '                            )'\
              '                          )'\
              '                        )'\
              '                      )'\
              '                    , LatticeFunctionExpressionConstantScalar('\
              '                        "5", DataTypeInt'\
              '                      )'\
              '                    , LatticeFunctionExpression('\
              '                        LatticeFunctionOperatorIdentifier('\
              '                          "IF"'\
              '                        )'\
              '                      , LatticeFunctionExpression('\
              '                          LatticeFunctionOperatorIdentifier('\
              '                            "Equal"'\
              '                          )'\
              '                        , LatticeFunctionExpression('\
              '                            LatticeFunctionOperatorIdentifier('\
              '                              "Lower"'\
              '                            )'\
              '                          , LatticeFunctionIdentifier('\
              '                              ContainerElementNameTableQualifiedName('\
              '                                LatticeSourceTableIdentifier('\
              '                                  ContainerElementName('\
              '                                    "PD_DerivedColumns"'\
              '                                  )'\
              '                                )'\
              '                              , ContainerElementName('\
              '                                  "ModelAction"'\
              '                                )'\
              '                              )'\
              '                            )'\
              '                          )'\
              '                        , LatticeFunctionExpression('\
              '                            LatticeFunctionOperatorIdentifier('\
              '                              "Lower"'\
              '                            )'\
              '                          , LatticeFunctionExpressionConstant('\
              '                              "RECENT BANKRUPTCY ON FILE", DataTypeNVarChar('\
              '                                25'\
              '                              )'\
              '                            )'\
              '                          )'\
              '                        )'\
              '                      , LatticeFunctionExpressionConstantScalar('\
              '                          "6", DataTypeInt'\
              '                        )'\
              '                      , LatticeFunctionExpressionConstantScalar('\
              '                          "0", DataTypeInt'\
              '                        )'\
              '                      )'\
              '                    )'\
              '                  )'\
              '                )'\
              '              )'\
              '            )'\
              '          )'\
              '        , LatticeAddressSetPi('\
              '            LatticeAddressExpressionAtomic('\
              '              LatticeAddressAtomicIdentifier('\
              '                ContainerElementName('\
              '                  "PropDataID"'\
              '                )'\
              '              )'\
              '            )'\
              '          )'\
              '        , FunctionAggregationOperator('\
              '            "None"'\
              '          )'\
              '        )'\
              '      , DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription('\
              '          ""'\
              '        )'\
              '      )'\
              '        , ContainerElementName("DS_PD_ModelAction_Ordered")'\
              '    ))'

      DS_PD_Alexa_RelatedLinks_Count=liaison.ExpressionVDBImplGeneric(spec4)
      conn_mgr.setSpec('DS_PD_ModelAction_Ordered',DS_PD_Alexa_RelatedLinks_Count.definition())

      #Modifying Lead_RankForModeling
      spec5='SpecLatticeNamedElements(SpecLatticeNamedElement('\
            '      SpecLatticeFunction('\
            '        LatticeFunctionExpressionTransform('\
            '          LatticeFunctionExpression('\
            '            LatticeFunctionOperatorIdentifier('\
            '              "IF"'\
            '            )'\
            '          , LatticeFunctionIdentifier('\
            '              ContainerElementName('\
            '                "AppData_Lead_IsConsideredForModeling"'\
            '              )'\
            '            )'\
            '          , LatticeFunctionExpression('\
            '              LatticeFunctionOperatorIdentifier('\
            '                "Plus"'\
            '              )'\
            '            , LatticeFunctionExpression('\
            '                LatticeFunctionOperatorIdentifier('\
            '                  "Plus"'\
            '                )'\
            '              , LatticeFunctionIdentifier('\
            '                  ContainerElementName('\
            '                    "Lead_OpportunityEventScore"'\
            '                  )'\
            '                )'\
            '              , LatticeFunctionIdentifier('\
            '                  ContainerElementName('\
            '                    "Lead_OpportunityAssociationScore"'\
            '                  )'\
            '                )'\
            '              )'\
            '            , LatticeFunctionExpression('\
            '                LatticeFunctionOperatorIdentifier('\
            '                  "Plus"'\
            '                )'\
            '              , LatticeFunctionIdentifier('\
            '                  ContainerElementName('\
            '                    "Lead_ActivityCountThreeMonthsAgo"'\
            '                  )'\
            '                )'\
            '              , LatticeFunctionIdentifier('\
            '                  ContainerElementName('\
            '                    "Lead_CreationDateRecencyForModeling"'\
            '                  )'\
            '                )'\
            '              )'\
            '            )'\
            '          , LatticeFunctionExpressionConstant('\
            '              "0.0", DataTypeDouble'\
            '            )'\
            '          )'\
            '        , LatticeAddressSetIdentifier('\
            '            ContainerElementName('\
            '              "Alias_AllLeadID"'\
            '            )'\
            '          )'\
            '        , FunctionAggregationOperator('\
            '            "Max"'\
            '          )'\
            '        )'\
            '      , DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription('\
            '          ""'\
            '        )'\
            '      )'\
            '        , ContainerElementName("Lead_RankForModeling")'\
            '    ))'

      DS_PD_Alexa_RelatedLinks_Count=liaison.ExpressionVDBImplGeneric(spec5)
      conn_mgr.setSpec('Lead_RankForModeling',DS_PD_Alexa_RelatedLinks_Count.definition())

      success = True

      return success






