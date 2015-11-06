
#
# $LastChangedBy: YeTian $
# $LastChangedDate: 2015-10-21 14:33:11 +0800 (Wed, 21 Oct 2015) $
# $Rev: 70508 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import appsequence
import liaison

class LP_020100_VDB_ModifiedColumns( StepBase ):
  
  name        = 'LP_020100_VDB_ModifiedColumns'
  description = 'Upgrade Modified Specs from 2.0.1 to 2.1.0'
  version     = '$Rev: 70508 $'
  def __init__( self, forceApply = False ):
    super( LP_020100_VDB_ModifiedColumns, self ).__init__( forceApply )

  def getApplicability( self, appseq ):

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )
      if type == 'MKTO':
          conn_mgr.getQuery("Q_Timestamp_PushToDante")
          conn_mgr.getTable("Timestamp_PushToDante")
          conn_mgr.getQuery("Q_Dante_ContactSourceTable")
          conn_mgr.getQuery("Q_Dante_LeadSourceTable")

          if not conn_mgr.getQuery("Q_Timestamp_PushToDante") and not conn_mgr.getTable("Timestamp_PushToDante") and not conn_mgr.getQuery("Q_Dante_ContactSourceTable") and not conn_mgr.getQuery("Q_Dante_LeadSourceTable"):
              return Applicability.cannotApplyFail

      elif type == 'ELQ':
          conn_mgr.getQuery("Q_Timestamp_PushToDante")
          conn_mgr.getTable("Timestamp_PushToDante")
          conn_mgr.getQuery("Q_Dante_ContactSourceTable")
          conn_mgr.getQuery("Q_Dante_LeadSourceTable")

          if not conn_mgr.getQuery("Q_Timestamp_PushToDante") and not conn_mgr.getTable("Timestamp_PushToDante") and not conn_mgr.getQuery("Q_Dante_ContactSourceTable") and not conn_mgr.getQuery("Q_Dante_LeadSourceTable"):
              return Applicability.cannotApplyFail

      else:
          conn_mgr.getSpec("Q_Timestamp_PushToDante")
          conn_mgr.getTable("Timestamp_PushToDante")
          conn_mgr.getTable("SFDC_Opportunity")
          conn_mgr.getQuery("Q_Dante_LeadSourceTable")
          conn_mgr.getQuery("Q_SFDC_Lead_Score")
          conn_mgr.getQuery("Q_SFDC_Contact_Score")
          if not conn_mgr.getSpec("Q_Dante_LeadSourceTable") and not conn_mgr.getTable("Timestamp_PushToDante") and not conn_mgr.getTable( 'SFDC_Opportunity') and not conn_mgr.getQuery("Q_SFDC_Lead_Score") and not conn_mgr.getQuery("Q_SFDC_Contact_Score"):
              return Applicability.cannotApplyFail

      return  Applicability.canApply

  def apply( self, appseq ):

      success = False

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )


      if type == 'MKTO':

          #Modify the Column in Q_Timestamp_PushToDante
          query4 = conn_mgr.getQuery("Q_Timestamp_PushToDante")
          exp4_1 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionConstant("Now", DataTypeDateTime)')
          col4_1 = liaison.QueryColumnVDBImpl('Time_OfCompletion_PushToDante',exp4_1)

          exp4_2 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("SFDC_Contact")), ContainerElementName("Id"))), LatticeAddressSetFromFcnSupport(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("SFDC_Contact")), ContainerElementName("Id")))), FunctionAggregationOperator("Max"))')
          col4_2 = liaison.QueryColumnVDBImpl('SFDC_Contact_ID',exp4_2)

          exp4_3 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("SFDC_Lead")), ContainerElementName("Id"))), LatticeAddressSetFromFcnSupport(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("SFDC_Lead")), ContainerElementName("Id")))), FunctionAggregationOperator("Max"))')
          col4_3 = liaison.QueryColumnVDBImpl('SFDC_Lead_ID',exp4_3)

          exp4_4 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), LatticeAddressSetFromFcnSupport(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate")))), FunctionAggregationOperator("Max"))')
          col4_4 = liaison.QueryColumnVDBImpl('ScoreDate',exp4_4)

          query4.appendColumn(col4_1)
          query4.appendColumn(col4_2)
          query4.appendColumn(col4_3)
          query4.appendColumn(col4_4)
          query4.removeColumn('Time_OfSubmission_PushToDante')

          conn_mgr.setQuery(query4)

          #Modify the Column in Timestamp_PushToDante
          table1 = conn_mgr.getTable( 'Timestamp_PushToDante')
          col1_1 = liaison.TableColumnVDBImpl( 'Time_OfCompletion_PushToDante', 'Timestamp_PushToDante', 'DateTime' )
          col1_2 = liaison.TableColumnVDBImpl( 'SFDC_Contact_ID', 'Timestamp_PushToDante', 'VarChar(18)' )
          col1_3 = liaison.TableColumnVDBImpl( 'SFDC_Lead_ID', 'Timestamp_PushToDante', 'VarChar(18)' )
          col1_4 = liaison.TableColumnVDBImpl( 'ScoreDate', 'Timestamp_PushToDante', 'DateTime' )

          table1.appendColumn( col1_1 )
          table1.appendColumn( col1_2 )
          table1.appendColumn( col1_3 )
          table1.appendColumn( col1_4 )
          table1.removeColumn( 'Time_OfSubmission_PushToDante' )

          conn_mgr.setTable( table1 )

          #Modify the Column in Q_LeadScoreForLeadDestination_Query
          query5 = conn_mgr.getQuery("Q_LeadScoreForLeadDestination_Query")
          exp5_1 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Percentile"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col5_1 = liaison.QueryColumnVDBImpl('Score',exp5_1)

          query5.removeColumn('Score')
          query5.removeColumn('SFDCLeadID')
          query5.appendColumn(col5_1)

          conn_mgr.setQuery(query5)

          #Modify the Column in Q_Dante_ContactSourceTable
          query6 = conn_mgr.getQuery("Q_Dante_ContactSourceTable")
          exp6_1 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Score"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col6_1 = liaison.QueryColumnVDBImpl('Score',exp6_1)

          exp6_2 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Bucket_Display_Name"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col6_2 = liaison.QueryColumnVDBImpl('Bucket_Display_Name',exp6_2)

          exp6_3 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Probability"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col6_3 = liaison.QueryColumnVDBImpl('Probability',exp6_3)

          exp6_4 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Lift"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col6_4 = liaison.QueryColumnVDBImpl('Lift',exp6_4)

          exp6_5 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Percentile"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col6_5 = liaison.QueryColumnVDBImpl('Percentile',exp6_5)

          exp6_6 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Play_Display_Name"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col6_6 = liaison.QueryColumnVDBImpl('ModelID',exp6_6)

          exp6_7 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Play_Display_Name"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col6_7 = liaison.QueryColumnVDBImpl('PlayDisplayName',exp6_7)

          query6.updateColumn(col6_1)
          query6.updateColumn(col6_2)
          query6.updateColumn(col6_3)
          query6.updateColumn(col6_4)
          query6.updateColumn(col6_5)
          query6.updateColumn(col6_6)
          query6.updateColumn(col6_7)

          conn_mgr.setQuery(query6)

          #Modify the Column in Q_Dante_LeadSourceTable
          query7 = conn_mgr.getQuery("Q_Dante_LeadSourceTable")
          exp7_1 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Score"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col7_1 = liaison.QueryColumnVDBImpl('Score',exp7_1)

          exp7_2 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Bucket_Display_Name"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col7_2 = liaison.QueryColumnVDBImpl('Bucket_Display_Name',exp7_2)

          exp7_3 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Probability"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col7_3 = liaison.QueryColumnVDBImpl('Probability',exp7_3)

          exp7_4 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Lift"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col7_4 = liaison.QueryColumnVDBImpl('Lift',exp7_4)

          exp7_5 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Percentile"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col7_5 = liaison.QueryColumnVDBImpl('Percentile',exp7_5)

          exp7_6 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Play_Display_Name"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col7_6 = liaison.QueryColumnVDBImpl('ModelID',exp7_6)

          exp7_7 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Play_Display_Name"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col7_7 = liaison.QueryColumnVDBImpl('PlayDisplayName',exp7_7)

          query7.updateColumn(col7_1)
          query7.updateColumn(col7_2)
          query7.updateColumn(col7_3)
          query7.updateColumn(col7_4)
          query7.updateColumn(col7_5)
          query7.updateColumn(col7_6)
          query7.updateColumn(col7_7)

          conn_mgr.setQuery(query7)

      elif type == 'ELQ':
          #Modify the Column in Q_Timestamp_PushToDante
          query4 = conn_mgr.getQuery("Q_Timestamp_PushToDante")
          exp4_1 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionConstant("Now", DataTypeDateTime)')
          col4_1 = liaison.QueryColumnVDBImpl('Time_OfCompletion_PushToDante',exp4_1)

          exp4_2 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Timestamp_PushToDante_Stage")), ContainerElementName("SFDC_Contact_ID"))), LatticeAddressSetFromFcnSupport(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Timestamp_PushToDante_Stage")), ContainerElementName("SFDC_Contact_ID")))), FunctionAggregationOperator("Max"))')
          col4_2 = liaison.QueryColumnVDBImpl('SFDC_Contact_ID',exp4_2)

          exp4_3 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Timestamp_PushToDante_Stage")), ContainerElementName("SFDC_Lead_ID"))), LatticeAddressSetFromFcnSupport(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Timestamp_PushToDante_Stage")), ContainerElementName("SFDC_Lead_ID")))), FunctionAggregationOperator("Max"))')
          col4_3 = liaison.QueryColumnVDBImpl('SFDC_Lead_ID',exp4_3)

          exp4_4 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), LatticeAddressSetFromFcnSupport(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate")))), FunctionAggregationOperator("Max"))')
          col4_4 = liaison.QueryColumnVDBImpl('ScoreDate',exp4_4)

          query4.appendColumn(col4_1)
          query4.appendColumn(col4_2)
          query4.appendColumn(col4_3)
          query4.appendColumn(col4_4)
          query4.removeColumn('Time_OfSubmission_PushToDante')

          conn_mgr.setQuery(query4)

          #Modify the Column in Timestamp_PushToDante
          table1 = conn_mgr.getTable( 'Timestamp_PushToDante')
          col1_1 = liaison.TableColumnVDBImpl( 'Time_OfCompletion_PushToDante', 'Timestamp_PushToDante', 'DateTime' )
          col1_2 = liaison.TableColumnVDBImpl( 'SFDC_Contact_ID', 'Timestamp_PushToDante', 'VarChar(18)' )
          col1_3 = liaison.TableColumnVDBImpl( 'SFDC_Lead_ID', 'Timestamp_PushToDante', 'VarChar(18)' )
          col1_4 = liaison.TableColumnVDBImpl( 'ScoreDate', 'Timestamp_PushToDante', 'DateTime' )

          table1.appendColumn( col1_1 )
          table1.appendColumn( col1_2 )
          table1.appendColumn( col1_3 )
          table1.appendColumn( col1_4 )
          table1.removeColumn( 'Time_OfSubmission_PushToDante' )

          conn_mgr.setTable( table1 )

          #Modify the Column in Q_ELQ_Contact_Score
          query5 = conn_mgr.getQuery("Q_ELQ_Contact_Score")
          exp5_1 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Percentile"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col5_1 = liaison.QueryColumnVDBImpl('C_Lattice_Predictive_Score1',exp5_1)

          query5.removeColumn('C_Lattice_Predictive_Score1')
          query5.removeColumn('SFDCLeadID')
          query5.appendColumn(col5_1)

          conn_mgr.setQuery(query5)

         #Modify the Column in Q_Dante_ContactSourceTable
          query6 = conn_mgr.getQuery("Q_Dante_ContactSourceTable")
          exp6_1 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Score"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col6_1 = liaison.QueryColumnVDBImpl('Score',exp6_1)

          exp6_2 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Bucket_Display_Name"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col6_2 = liaison.QueryColumnVDBImpl('Bucket_Display_Name',exp6_2)

          exp6_3 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Probability"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col6_3 = liaison.QueryColumnVDBImpl('Probability',exp6_3)

          exp6_4 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Lift"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col6_4 = liaison.QueryColumnVDBImpl('Lift',exp6_4)

          exp6_5 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Percentile"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col6_5 = liaison.QueryColumnVDBImpl('Percentile',exp6_5)

          exp6_6 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Play_Display_Name"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col6_6 = liaison.QueryColumnVDBImpl('ModelID',exp6_6)

          exp6_7 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Play_Display_Name"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col6_7 = liaison.QueryColumnVDBImpl('PlayDisplayName',exp6_7)

          query6.updateColumn(col6_1)
          query6.updateColumn(col6_2)
          query6.updateColumn(col6_3)
          query6.updateColumn(col6_4)
          query6.updateColumn(col6_5)
          query6.updateColumn(col6_6)
          query6.updateColumn(col6_7)

          conn_mgr.setQuery(query6)

          #Modify the Column in Q_Dante_LeadSourceTable
          query7 = conn_mgr.getQuery("Q_Dante_LeadSourceTable")
          exp7_1 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Score"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col7_1 = liaison.QueryColumnVDBImpl('Score',exp7_1)

          exp7_2 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Bucket_Display_Name"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col7_2 = liaison.QueryColumnVDBImpl('Bucket_Display_Name',exp7_2)

          exp7_3 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Probability"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col7_3 = liaison.QueryColumnVDBImpl('Probability',exp7_3)

          exp7_4 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Lift"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col7_4 = liaison.QueryColumnVDBImpl('Lift',exp7_4)

          exp7_5 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Percentile"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col7_5 = liaison.QueryColumnVDBImpl('Percentile',exp7_5)

          exp7_6 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Play_Display_Name"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col7_6 = liaison.QueryColumnVDBImpl('ModelID',exp7_6)

          exp7_7 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Play_Display_Name"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col7_7 = liaison.QueryColumnVDBImpl('PlayDisplayName',exp7_7)

          query7.updateColumn(col7_1)
          query7.updateColumn(col7_2)
          query7.updateColumn(col7_3)
          query7.updateColumn(col7_4)
          query7.updateColumn(col7_5)
          query7.updateColumn(col7_6)
          query7.updateColumn(col7_7)

          conn_mgr.setQuery(query7)
      else:
          #Modify the Column in Q_Timestamp_PushToDante
          query1 = conn_mgr.getQuery("Q_Timestamp_PushToDante")
          exp1_1 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionConstant("Now", DataTypeDateTime)')
          col1_1 = liaison.QueryColumnVDBImpl('Time_OfCompletion_PushToDante',exp1_1)

          exp1_2 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), LatticeAddressSetFromFcnSupport(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate")))), FunctionAggregationOperator("Max"))')
          col1_2 = liaison.QueryColumnVDBImpl('ScoreDate',exp1_2)

          query1.appendColumn(col1_1)
          query1.appendColumn(col1_2)
          query1.removeColumn('Time_OfSubmission_PushToDante')

          conn_mgr.setQuery(query1)

          #Modify the Column in Timestamp_PushToDante
          table1 = conn_mgr.getTable( 'Timestamp_PushToDante')
          col1_1 = liaison.TableColumnVDBImpl( 'Time_OfCompletion_PushToDante', 'Timestamp_PushToDante', 'DateTime' )
          col1_2 = liaison.TableColumnVDBImpl( 'ScoreDate', 'Timestamp_PushToDante', 'DateTime' )

          table1.appendColumn( col1_1 )
          table1.appendColumn( col1_2 )
          table1.removeColumn( 'Time_OfSubmission_PushToDante' )

          conn_mgr.setTable( table1 )

        #Add a Key Column into the Table SFDC_Opportunity
          table_key = []
          table2 = conn_mgr.getTable( 'SFDC_Opportunity')
          table2.setSourceTableSpecs('SpecKeys((SpecKey(LatticeAddress((LatticeAddressAtomicIdentifier(ContainerElementName("SFDC_Opportunity_ID")))),SpecKeyDomainEntire))),SpecDescription(""),SpecMaximalIsMaximal,SpecKeyAggregation(SpecColumnAggregationRuleMostRecent),SpecEquivalenceAggregation(SpecColumnAggregationRuleMostRecent)')
          conn_mgr.setTable( table2 )

        #Modify the Column in Q_Dante_LeadSourceTable
          query2 = conn_mgr.getQuery("Q_Dante_LeadSourceTable")
          exp2_1 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Score"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col2_1 = liaison.QueryColumnVDBImpl('Score',exp2_1)

          exp2_2 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Bucket_Display_Name"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col2_2 = liaison.QueryColumnVDBImpl('Bucket_Display_Name',exp2_2)

          exp2_3 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Probability"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col2_3 = liaison.QueryColumnVDBImpl('Probability',exp2_3)

          exp2_4 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Lift"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col2_4 = liaison.QueryColumnVDBImpl('Lift',exp2_4)

          exp2_5 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Percentile"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col2_5 = liaison.QueryColumnVDBImpl('Percentile',exp2_5)

          exp2_6 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Play_Display_Name"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col2_6 = liaison.QueryColumnVDBImpl('ModelID',exp2_6)

          exp2_7 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Play_Display_Name"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col2_7 = liaison.QueryColumnVDBImpl('PlayDisplayName',exp2_7)

          query2.updateColumn(col2_1)
          query2.updateColumn(col2_2)
          query2.updateColumn(col2_3)
          query2.updateColumn(col2_4)
          query2.updateColumn(col2_5)
          query2.updateColumn(col2_6)
          query2.updateColumn(col2_7)

          conn_mgr.setQuery(query2)


          #Modify the Column in Q_SFDC_Lead_Score
          query3 = conn_mgr.getQuery("Q_SFDC_Lead_Score")
          exp3 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Percentile"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col3 = liaison.QueryColumnVDBImpl('C_Lattice_Predictive_Score1',exp3)

          query3.removeColumn('C_Lattice_Predictive_Score1')
          query3.appendColumn(col3)

          conn_mgr.setQuery(query3)


          #Modify the Column in Q_SFDC_Contact_Score
          query4 = conn_mgr.getQuery("Q_SFDC_Contact_Score")
          exp4 = liaison.ExpressionVDBImplGeneric('LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("Percentile"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Bard_LeadScoreHistory")))), FunctionAggregationSelectWhere(FunctionAggregationOperator("Max"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Bard_LeadScoreHistory")), ContainerElementName("ScoreDate"))), FunctionAggregationOperator("Max")))')
          col4 = liaison.QueryColumnVDBImpl('C_Lattice_Predictive_Score1',exp4)

          query4.removeColumn('C_Lattice_Predictive_Score1')
          query4.appendColumn(col4)

          conn_mgr.setQuery(query4)

      success = True

      return success






