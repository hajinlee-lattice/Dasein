
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re
import requests

from liaison import *

from .Applicability import Applicability
from .AppSequence   import AppSequence
from .StepBase      import StepBase

class LPCheckVersion( StepBase ):

  name        = 'LPCheckVersion'
  description = 'Check that the LP template version is '
  version     = '$Rev$'


  def __init__( self, version, forceApply = False ):
    self._template_version = version
    self.description += self._template_version
    super( LPCheckVersion, self ).__init__( forceApply )


  def getApplicability( self, appseq ):
    type    = 'Unknown'
    version = 'Unknown'
    defn    = 'Undefined'
    try:
      exp_version = appseq.getConnectionMgr().GetNamedExpression( 'Version' )
      defn = exp_version.Object().Definition()
      c = re.search( 'LatticeFunctionExpressionConstant\(\"PLS (.*?) Template:\".*LatticeFunctionExpressionConstant\(\"(.*?)\"', defn )
      if c:
        type = c.group(1)
        version = c.group(2)
      else:
        type = 'Nonstandard type'
        version = 'Nonstandard version'
    except requests.exceptions.SSLError:
      ## Not on a PROD DataLoader
      pass
    except TenantNotFoundAtURL:
      ## Not on a PROD DataLoader
      pass
    except UnknownVisiDBSpec:
      type = 'No template type'
      version = 'No template version'

    appseq.setText( 'template_type', type )
    appseq.setText( 'template_version', version )
    appseq.setText( 'template_version_spec_defn', defn )

    if version == self._template_version:
      return Applicability.canApply

    return Applicability.cannotApplyFail


  def apply( self, appseq ):
    return True
