
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from liaison import *

from .Applicability import Applicability
from .AppSequence   import AppSequence
from .StepBase      import StepBase

class LPSetVersion( StepBase ):

  name        = 'LPSetVersion'
  description = 'Set the LP template version to '
  version     = '$Rev$'


  def __init__( self, version, forceApply = False ):
    self._template_version_new = version
    self.description += self._template_version_new
    super( LPSetVersion, self ).__init__( forceApply )


  def getApplicability( self, appseq ):
    current_version = appseq.getText( 'template_version' )
    if current_version not in [ 'Unknown', 'Nonstandard version', 'No template version' ]:
      return Applicability.canApply

    return Applicability.cannotApplyFail


  def apply( self, appseq ):
    template_version = appseq.getText( 'template_version' )
    defn = appseq.getText( 'template_version_spec_defn' )
    exp_version_new = NamedExpressionVDBImpl( 'Version', ExpressionVDBImplGeneric(defn.replace(template_version,self._template_version_new)) )
    appseq.getConnectionMgr().SetNamedExpression( exp_version_new )
    
    return True
