
#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import appsequence
import liaison

class LP_020500_VDB_ModifiedColumns( StepBase ):
  
  name        = 'LP_020500_VDB_ModifiedColumns'
  description = 'Upgrade Modified Specs from 2.4.4 to 2.5.0'
  version     = '$Rev: 70934 $'
  def __init__( self, forceApply = False ):
    super( LP_020500_VDB_ModifiedColumns, self ).__init__( forceApply )

  def getApplicability( self, appseq ):

      conn_mgr = appseq.getConnectionMgr()
      conn_mgr.getQuery("Q_Metadata_Source")
      if not conn_mgr.getQuery("Q_Metadata_Source"):
          return Applicability.cannotApplyPass
      return  Applicability.canApply

  def apply( self, appseq ):

      success = False

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )


      if type == 'MKTO':
          query1 = conn_mgr.getQuery("Q_Metadata_Source")
          exp1_1 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("MKTO_Activity_ClickEmail_cnt_1mo"))')
          col1_1 = liaison.QueryColumnVDBImpl('MKTO_Activity_ClickEmail_cnt_1mo', exp1_1)
          exp1_2 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("MKTO_Activity_ClickLink_cnt_1mo"))')
          col1_2 = liaison.QueryColumnVDBImpl('MKTO_Activity_ClickLink_cnt_1mo', exp1_2)
          exp1_3 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("MKTO_Activity_Webinar_1mo"))')
          col1_3 = liaison.QueryColumnVDBImpl('MKTO_Activity_Webinar_1mo', exp1_3)
          exp1_4 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("MKTO_Activity_Email_1mo"))')
          col1_4 = liaison.QueryColumnVDBImpl('MKTO_Activity_Email_1mo', exp1_4)
          exp1_5 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("MKTO_Activity_EmailBncedSft_cnt_1mo"))')
          col1_5 = liaison.QueryColumnVDBImpl('MKTO_Activity_EmailBncedSft_cnt_1mo', exp1_5)
          exp1_6 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("MKTO_Activity_Event_1mo"))')
          col1_6 = liaison.QueryColumnVDBImpl('MKTO_Activity_Event_1mo', exp1_6)
          exp1_7 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("MKTO_Activity_FillOutForm_cnt_1mo"))')
          col1_7 = liaison.QueryColumnVDBImpl('MKTO_Activity_FillOutForm_cnt_1mo', exp1_7)
          exp1_8 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("MKTO_Activity_InterestingMoment_cnt_1mo"))')
          col1_8 = liaison.QueryColumnVDBImpl('MKTO_Activity_InterestingMoment_cnt_1mo', exp1_8)
          exp1_9 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("MKTO_Activity_keywebpage_1mo"))')
          col1_9 = liaison.QueryColumnVDBImpl('MKTO_Activity_keywebpage_1mo', exp1_9)
          exp1_10 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("MKTO_Activity_Multiple_1mo"))')
          col1_10 = liaison.QueryColumnVDBImpl('MKTO_Activity_Multiple_1mo', exp1_10)
          exp1_11 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("MKTO_Activity_OpenEmail_cnt_1mo"))')
          col1_11 = liaison.QueryColumnVDBImpl('MKTO_Activity_OpenEmail_cnt_1mo', exp1_11)
          exp1_12 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("MKTO_Activity_Pricing_1mo"))')
          col1_12 = liaison.QueryColumnVDBImpl('MKTO_Activity_Pricing_1mo', exp1_12)
          exp1_13 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("MKTO_Activity_Search_1mo"))')
          col1_13 = liaison.QueryColumnVDBImpl('MKTO_Activity_Search_1mo', exp1_13)
          exp1_14 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("MKTO_Activity_UnsubscribeEmail_cnt_1mo"))')
          col1_14 = liaison.QueryColumnVDBImpl('MKTO_Activity_UnsubscribeEmail_cnt_1mo', exp1_14)
          exp1_15 = liaison.ExpressionVDBImplGeneric('LatticeFunctionIdentifier(ContainerElementName("MKTO_Activity_VisitWeb_cnt_1mo"))')
          col1_15 = liaison.QueryColumnVDBImpl('MKTO_Activity_VisitWeb_cnt_1mo', exp1_15)

          query1.appendColumn(col1_1)
          query1.appendColumn(col1_2)
          query1.appendColumn(col1_3)
          query1.appendColumn(col1_4)
          query1.appendColumn(col1_5)
          query1.appendColumn(col1_6)
          query1.appendColumn(col1_7)
          query1.appendColumn(col1_8)
          query1.appendColumn(col1_9)
          query1.appendColumn(col1_10)
          query1.appendColumn(col1_11)
          query1.appendColumn(col1_12)
          query1.appendColumn(col1_13)
          query1.appendColumn(col1_14)
          query1.appendColumn(col1_15)
          conn_mgr.setQuery(query1)
      else:
          pass

      success = True

      return success






