
#
# $LastChangedBy: lyan $
# $LastChangedDate: 2015-11-19 19:51:40 +0800 (Thu, 19 Nov 2015) $
# $Rev: 71042 $
#

from liaison import *
from appsequence import Applicability, StepBase


class LP_020600_AddDataProvider( StepBase ):

  name        = 'LP_020600_AddDataProvider'
  description = 'Add a new DataProvider: SFDC2_DataProvider'
  version     = '$Rev: 71042 $'


  def __init__( self, forceApply = False ):
    super( LP_020600_AddDataProvider, self ).__init__( forceApply )


  def getApplicability( self, appseq ):
    return Applicability.canApply


  def apply( self, appseq ):

    lg_mgr = appseq.getLoadGroupMgr()
    lg_mgr.createDataProvider( '<dataProvider name="SFDC2_DataProvider" autoMatch="False" connectionString="URL=https://login.salesforce.com/services/Soap/u/27.0;User=;Password=;SecurityToken=;Version=27.0;Timeout=100;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=2000;" dbType="1010" usedFor="31" e="False"/>' )
    return True
