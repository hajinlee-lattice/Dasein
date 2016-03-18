
#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import appsequence
import liaison

class LP_020400_VDB_ModifiedColumns( StepBase ):
  
  name        = 'LP_020100_VDB_ModifiedColumns'
  description = 'Upgrade Modified Specs from 2.0.1 to 2.1.0'
  version     = '$Rev: 70934 $'
  def __init__( self, forceApply = False ):
    super( LP_020400_VDB_ModifiedColumns, self ).__init__( forceApply )

  def getApplicability( self, appseq ):

      conn_mgr = appseq.getConnectionMgr()
      conn_mgr.getTable("Bard_LeadScoreStage")
      conn_mgr.getTable("Bard_LeadScoreHistory")
      if not conn_mgr.getTable("Bard_LeadScoreStage") and not conn_mgr.getTable("Bard_LeadScoreHistory"):
          return Applicability.cannotApplyPass

      return  Applicability.canApply

  def apply( self, appseq ):

      success = False

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )


      if type == 'MKTO':

          #Modify the Column in Bard_LeadScoreStage and Bard_LeadScoreHistory
          table1 = conn_mgr.getTable( 'Bard_LeadScoreStage')
          col1_1 = liaison.TableColumnVDBImpl( 'Percentile', 'Bard_LeadScoreStage', 'Int','SpecFieldTypeAttribute(LatticeAddressAtomicIdentifier(ContainerElementName("Percentile")),SpecConstructiveIsConstructive,FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Percentile")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col1_2 = liaison.TableColumnVDBImpl( 'Probability', 'Bard_LeadScoreStage', 'Double','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Probability")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col1_3 = liaison.TableColumnVDBImpl( 'RawScore', 'Bard_LeadScoreStage', 'Double','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("RawScore")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col1_4 = liaison.TableColumnVDBImpl( 'Score', 'Bard_LeadScoreStage', 'Int','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Score")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')

          table1.removeColumn( 'Percentile' )
          table1.removeColumn( 'Probability' )
          table1.removeColumn( 'RawScore' )
          table1.removeColumn( 'Score' )
          table1.appendColumn( col1_1 )
          table1.appendColumn( col1_2 )
          table1.appendColumn( col1_3 )
          table1.appendColumn( col1_4 )
          conn_mgr.setTable( table1 )

          #Modify the Column in Bard_LeadScoreHistory
          table2 = conn_mgr.getTable( 'Bard_LeadScoreHistory')
          col2_1 = liaison.TableColumnVDBImpl( 'Percentile', 'Bard_LeadScoreHistory', 'Int','SpecFieldTypeAttribute(LatticeAddressAtomicIdentifier(ContainerElementName("Percentile")),SpecConstructiveIsConstructive,FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Percentile")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col2_2 = liaison.TableColumnVDBImpl( 'Probability', 'Bard_LeadScoreHistory', 'Double','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Probability")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col2_3 = liaison.TableColumnVDBImpl( 'RawScore', 'Bard_LeadScoreHistory', 'Double','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("RawScore")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col2_4 = liaison.TableColumnVDBImpl( 'Score', 'Bard_LeadScoreHistory', 'Int','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Score")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')

          table2.removeColumn( 'Percentile' )
          table2.removeColumn( 'Probability' )
          table2.removeColumn( 'RawScore' )
          table2.removeColumn( 'Score' )
          table2.appendColumn( col2_1 )
          table2.appendColumn( col2_2 )
          table2.appendColumn( col2_3 )
          table2.appendColumn( col2_4 )
          conn_mgr.setTable( table2 )

      elif type == 'ELQ':
          #Modify the Column in Bard_LeadScoreStage
          table1 = conn_mgr.getTable( 'Bard_LeadScoreStage')
          col1_1 = liaison.TableColumnVDBImpl( 'Percentile', 'Bard_LeadScoreStage', 'Int','SpecFieldTypeAttribute(LatticeAddressAtomicIdentifier(ContainerElementName("Percentile")),SpecConstructiveIsConstructive,FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Percentile")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col1_2 = liaison.TableColumnVDBImpl( 'Probability', 'Bard_LeadScoreStage', 'Double','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Probability")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col1_3 = liaison.TableColumnVDBImpl( 'RawScore', 'Bard_LeadScoreStage', 'Double','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("RawScore")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col1_4 = liaison.TableColumnVDBImpl( 'Score', 'Bard_LeadScoreStage', 'Int','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Score")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')

          table1.removeColumn( 'Percentile' )
          table1.removeColumn( 'Probability' )
          table1.removeColumn( 'RawScore' )
          table1.removeColumn( 'Score' )
          table1.appendColumn( col1_1 )
          table1.appendColumn( col1_2 )
          table1.appendColumn( col1_3 )
          table1.appendColumn( col1_4 )
          conn_mgr.setTable( table1 )

          #Modify the Column in Bard_LeadScoreHistory
          table2 = conn_mgr.getTable( 'Bard_LeadScoreHistory')
          col2_1 = liaison.TableColumnVDBImpl( 'Percentile', 'Bard_LeadScoreHistory', 'Int','SpecFieldTypeAttribute(LatticeAddressAtomicIdentifier(ContainerElementName("Percentile")),SpecConstructiveIsConstructive,FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Percentile")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col2_2 = liaison.TableColumnVDBImpl( 'Probability', 'Bard_LeadScoreHistory', 'Double','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Probability")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col2_3 = liaison.TableColumnVDBImpl( 'RawScore', 'Bard_LeadScoreHistory', 'Double','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("RawScore")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col2_4 = liaison.TableColumnVDBImpl( 'Score', 'Bard_LeadScoreHistory', 'Int','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Score")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')

          table2.removeColumn( 'Percentile' )
          table2.removeColumn( 'Probability' )
          table2.removeColumn( 'RawScore' )
          table2.removeColumn( 'Score' )
          table2.appendColumn( col2_1 )
          table2.appendColumn( col2_2 )
          table2.appendColumn( col2_3 )
          table2.appendColumn( col2_4 )
          conn_mgr.setTable( table2 )

      else:
           #Modify the Column in Bard_LeadScoreStage
          table1 = conn_mgr.getTable( 'Bard_LeadScoreStage')
          col1_1 = liaison.TableColumnVDBImpl( 'Percentile', 'Bard_LeadScoreStage', 'Int','SpecFieldTypeAttribute(LatticeAddressAtomicIdentifier(ContainerElementName("Percentile")),SpecConstructiveIsConstructive,FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Percentile")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col1_2 = liaison.TableColumnVDBImpl( 'Probability', 'Bard_LeadScoreStage', 'Double','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Probability")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col1_3 = liaison.TableColumnVDBImpl( 'RawScore', 'Bard_LeadScoreStage', 'Double','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("RawScore")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col1_4 = liaison.TableColumnVDBImpl( 'Score', 'Bard_LeadScoreStage', 'Int','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Score")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')

          table1.removeColumn( 'Percentile' )
          table1.removeColumn( 'Probability' )
          table1.removeColumn( 'RawScore' )
          table1.removeColumn( 'Score' )
          table1.appendColumn( col1_1 )
          table1.appendColumn( col1_2 )
          table1.appendColumn( col1_3 )
          table1.appendColumn( col1_4 )
          conn_mgr.setTable( table1 )

           #Modify the Column in Bard_LeadScoreHistory
          table2 = conn_mgr.getTable( 'Bard_LeadScoreHistory')
          col2_1 = liaison.TableColumnVDBImpl( 'Percentile', 'Bard_LeadScoreHistory', 'Int','SpecFieldTypeAttribute(LatticeAddressAtomicIdentifier(ContainerElementName("Percentile")),SpecConstructiveIsConstructive,FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Percentile")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col2_2 = liaison.TableColumnVDBImpl( 'Probability', 'Bard_LeadScoreHistory', 'Double','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Probability")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col2_3 = liaison.TableColumnVDBImpl( 'RawScore', 'Bard_LeadScoreHistory', 'Double','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("RawScore")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')
          col2_4 = liaison.TableColumnVDBImpl( 'Score', 'Bard_LeadScoreHistory', 'Int','SpecFieldTypeMetric(FunctionAggregationOperator("Max")),SpecColumnContentContainerElementName(ContainerElementName("Score")),SpecEndpointTypeNone,SpecDefaultValueNull,SpecKeyAggregation(SpecColumnAggregationRuleFunction("Max")),SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Max"))')

          table2.removeColumn( 'Percentile' )
          table2.removeColumn( 'Probability' )
          table2.removeColumn( 'RawScore' )
          table2.removeColumn( 'Score' )
          table2.appendColumn( col2_1 )
          table2.appendColumn( col2_2 )
          table2.appendColumn( col2_3 )
          table2.appendColumn( col2_4 )
          conn_mgr.setTable( table2 )

      success = True

      return success






