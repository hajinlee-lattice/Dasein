
#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import appsequence
import liaison

class LP_020400_VDB_ModifiedFilters( StepBase ):
  
  name        = 'LP_020100_VDB_ModifiedFilters'
  description = 'Upgrade Modified Specs from 2.0.1 to 2.1.0'
  version     = '$Rev: 70934 $'
  def __init__( self, forceApply = False ):
    super( LP_020400_VDB_ModifiedFilters, self ).__init__( forceApply )

  def getApplicability( self, appseq ):

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )
      conn_mgr.getSpec("Q_Timestamp_MatchToPD")

      if not conn_mgr.getSpec("Q_Timestamp_MatchToPD"):
              return Applicability.cannotApplyPass
      return  Applicability.canApply

  def apply( self, appseq ):

      success = False

      conn_mgr = appseq.getConnectionMgr()

      filters_new1 = []
      Q_Timestamp_MatchToPD    = conn_mgr.getQuery("Q_Timestamp_MatchToPD")
      filter1 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetIdentifier(ContainerElementName("HAVING_DownloadedLeadNotMatched_Timestamp")),LatticeAddressSetIdentifier(ContainerElementName("HAVING_LeadHasPropDataID"))')
      filters_new1.append(filter1)
      Q_Timestamp_MatchToPD.filters_ = filters_new1
      conn_mgr.setSpec('Q_Dante_LeadSourceTable',Q_Timestamp_MatchToPD.SpecLatticeNamedElements())

      success = True

      return success






