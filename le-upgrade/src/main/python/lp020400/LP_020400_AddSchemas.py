
#
# $LastChangedBy: lyan $
# $LastChangedDate: 2015-11-19 19:51:40 +0800 (Thu, 19 Nov 2015) $
# $Rev: 71042 $
#

from liaison import *
from appsequence import Applicability, StepBase


class LP_020400_AddSchemas( StepBase ):

  name        = 'LP_020100_AddDataProvider'
  description = 'Add a new DataProvider: PLS_MultiTenant'
  version     = '$Rev: 71042 $'


  def __init__( self, forceApply = False ):
    super( LP_020400_AddSchemas, self ).__init__( forceApply )


  def getApplicability( self, appseq ):
    return Applicability.canApply


  def apply( self, appseq ):

    lg_mgr = appseq.getLoadGroupMgr()
    lg_mgr.createSchemas( '<schema w="Workspace" name="User_LeadFileForScoring" type="0" useSQLStatement="False" expire="30" autoDeleteTemporaryData="False" em="True" emd="False"/>' )
    return True
