#
# $LastChangedBy: YTian $
# $LastChangedDate: 2015-12-24 01:50:33 +0800 (Fri, 20 Nov 2015) $
# $Rev: 71049 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import re
import appsequence
import liaison
import os


class LP_020101_PushLeadsLastScoredToDestination(StepBase):
  name = 'LP_020101_PushLeadsLastScoredToDestination'
  description = 'Create an new Load Group PushLeadsLastScoredToDestination'
  version = '$Rev: 71049 $'
  _scoreDateField = ''
  _scoreField = ''

  def __init__(self, forceApply=False):
    super(LP_020101_PushLeadsLastScoredToDestination, self).__init__(forceApply)

  def getApplicability(self, appseq):

    lgm      = appseq.getLoadGroupMgr()
    conn_mgr = appseq.getConnectionMgr()
    type = appseq.getText('template_type')

    if lgm.hasLoadGroup('PushLeadsLastScoredToDestination'):
      return Applicability.alreadyAppliedPass

    if type == 'MKTO':
          conn_mgr.getSpec("Q_LeadScoreForLeadDestination_Query")

          if not conn_mgr.getSpec("Q_LeadScoreForLeadDestination_Query"):
            return Applicability.cannotApplyFail

    elif type == 'ELQ':
          conn_mgr.getSpec("Q_ELQ_Contact_Score")

    return Applicability.canApply


  def apply(self, appseq):

    success = False

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText('template_type')

    conn_mgr = appseq.getConnectionMgr()
    customerId = appseq.getText('customer_id')
    scoreField = appseq.getText('score_field')
    scoreDateField = appseq.getText('score_date_field')

# Add New LG into DL
    pltd = lgm.getLoadGroup('PushLeadsInDanteToDestination')
    pltd = pltd.replace('Q_Timestamp_PushToDestination','Q_Timestamp_PushToDestination_LastScored')
    step1 = etree.fromstring(pltd.encode('ascii', 'xmlcharrefreplace') )

    step1.set( 'name', 'PushLeadsLastScoredToDestination' )
    step1.set( 'alias', 'PushLeadsLastScoredToDestination' )
    lgm.setLoadGroup( etree.tostring(step1) )


    conn_mgr = appseq.getConnectionMgr()
    type = appseq.getText( 'template_type' )

    filters_new1 = []
    filters_new2 = []
# Change Filters of the Specs into VisiDB
    if type == 'MKTO':
          #Modify the Filter in Q_LeadScoreForLeadDestination_Query
          Q_LeadScoreForLeadDestination_Query = conn_mgr.getQuery("Q_LeadScoreForLeadDestination_Query")
          filter1 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetIdentifier(ContainerElementName("SelectedForPushToDestination_MKTO"))')
          filters_new1.append(filter1)
          Q_LeadScoreForLeadDestination_Query.filters_ = filters_new1

          #print Q_LeadScoreForLeadDestination_Query.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_LeadScoreForLeadDestination_Query',Q_LeadScoreForLeadDestination_Query.SpecLatticeNamedElements())


    elif type =='ELQ':
          #Modify the Filter in Q_ELQ_Contact_Score
          Q_ELQ_Contact_Score = conn_mgr.getQuery("Q_ELQ_Contact_Score")
          filter3 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetIdentifier(ContainerElementName("SelectedForPushToDestination_ELQ"))')
          filters_new2.append(filter3)
          Q_ELQ_Contact_Score.filters_ = filters_new2

          #print Q_ELQ_Contact_Score.SpecLatticeNamedElements()
          conn_mgr.setSpec('Q_ELQ_Contact_Score',Q_ELQ_Contact_Score.SpecLatticeNamedElements())

# Add New Specs Into VisiDB
    newSpecsFileName = 'LP_' + type + '_NewSpecs_2.1.1_from_2.1.0.maude'
    newSpecsFileName = os.path.join('..','..','resources',newSpecsFileName)
    newSpecsFileName = os.path.join(os.path.dirname(__file__),newSpecsFileName)

    slnes = ''

    with open( newSpecsFileName, mode='r' ) as newSpecsFile:
      slnes = newSpecsFile.read()

    conn_mgr = appseq.getConnectionMgr()
    conn_mgr.setSpec( 'New Specs', slnes )

    success = True

    return success

