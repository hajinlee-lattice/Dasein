
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from liaison import *
from appsequence import Applicability, StepBase


class LP_020100_AddDataProvider( StepBase ):

  name        = 'LP_020100_AddDataProvider'
  description = 'Add a new DataProvider: PLS_MultiTenant'
  version     = '$Rev$'


  def __init__( self, forceApply = False ):
    super( LP_020100_AddDataProvider, self ).__init__( forceApply )


  def getApplicability( self, appseq ):
    return Applicability.canApply


  def apply( self, appseq ):

    lg_mgr = appseq.getLoadGroupMgr()
    lg_mgr.createDataProvider( '<dataProvider name="SQL_MultiTenant" autoMatch="False" connectionString="ServerName=BODCPRODVSQL100.prod.lattice.local\SQL100;Database=PLS_MultiTenant;User=s-multitenant;Password=M51+Eye10ant;Authentication=SQL Server Authentication;Schema=dbo;Timeout=3600;RetryTimesForTimeout=10;BatchSize=2000" dbType="2" usedFor="10" e="False" />' )
    
    return True
