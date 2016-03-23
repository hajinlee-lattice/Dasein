
#
# $LastChangedBy: lyan $
# $LastChangedDate: 2015-12-24 17:00:02 +0800 (Thu, 24 Dec 2015) $
# $Rev: 71849 $
#

from liaison import *
from .Applicability import Applicability
from .StepBase      import StepBase
import traceback
import AppSequence

class AppSequence_MLR( AppSequence.AppSequence ):

  def __init__( self, tenantName,resultsFileName, sequence,checkOnly):
    AppSequence.AppSequence.__init__(self,tenantName, resultsFileName, sequence, checkOnly)
    #super(AppSequence_MLR, self).__init__(tenantName, resultsFileName, sequence, checkOnly)
    self._tenantFileName  = None
    self._tenants         = [tenantName]

  def beginJob( self ):
    if self._tenants is None:
      raise ValueError( 'The input tenant name should not be None' )

    self._resultsFile = open( self._resultsFileName, mode = 'w' )
    self._resultsFile.write( 'TenantName,Upgraded' )
    for step in self._sequence:
      self._resultsFile.write( ',{0}:{1}'.format( step.getName(), step.getVersion() ) )
    self._resultsFile.write( '\n' )
