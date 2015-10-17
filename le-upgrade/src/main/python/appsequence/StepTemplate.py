
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from liaison import *

from .Applicability import Applicability
from .AppSequence   import AppSequence
from .StepBase      import StepBase

class StepTemplate( StepBase ):

  name        = 'Undefined'
  description = 'Undefined'
  version     = '$Rev$'


  def __init__( self, forceApply = False ):
    super( ClassNameGoesHere, self ).__init__( forceApply )


  def getApplicability( self, appseq ):
    template_type = appseq.getText( 'template_type' )
    template_version = appseq.getText( 'template_version' )

    return Applicability.cannotApplyFail


  def apply( self, appseq ):
    template_type = appseq.getText( 'template_type' )
    template_version = appseq.getText( 'template_version' )

    conn_mgr = appseq.getConnectionMgr()
    lg_mgr = appseq.getLoadGroupMgr()
    
    return True
