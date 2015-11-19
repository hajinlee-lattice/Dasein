
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
      lgm = appseq.getLoadGroupMgr()
      exp_version = appseq.getConnectionMgr().getNamedExpression( 'Version' )
      defn = exp_version.Object().definition()
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

    if not lgm.hasLoadGroup('PushToLeadDestination_Step1'):
      return Applicability.cannotApplyFail

    ptld_lbo_xml = lgm.getLoadGroupFunctionality('PushToLeadDestination_Step1', 'lssbardouts')

    appseq.setText( 'score_field', self.parseScoreField(ptld_lbo_xml, type) )
    appseq.setText( 'score_date_field', self.parseScoreDateField(ptld_lbo_xml, type) )
    appseq.setText( 'customer_id', self.parseCustomerId(ptld_lbo_xml, appseq) )

    if version == self._template_version:
      return Applicability.canApply

    return Applicability.cannotApplyFail

  def parseScoreField(selfs, str, type):
    if type == 'MKTO':
      s = re.search('<cm scn=\"Score\" tcn=\"(.*?)\"/>', str)
    elif type == 'ELQ':
      s = re.search('<cm scn=\"C_Lattice_Predictive_Score1\" tcn=\"(.*?)\"/>', str)
    else:
      s = re.search('<cm scn=\"C_Lattice_Predictive_Score1\" tcn=\"(.*?)\"/>', str)

    if not s:
      raise ValueError( 'Get the Score Field failed' )
    return s.group(1)

  def parseScoreDateField(self, str, type):
    if type == 'MKTO':
      s = re.search('<cm scn=\"Score_Date_Time\" tcn=\"(.*?)\"/>', str)
    elif type == 'ELQ':
      s = re.search('<cm scn=\"C_Lattice_LastScoreDate1\" tcn=\"(.*?)\"/>', str)
    else:
      s = re.search('<cm scn=\"C_Lattice_LastScoreDate1\" tcn=\"(.*?)\"/>', str)

    if not s:
      raise ValueError( 'Get the ScoreDate Field failed' )
    return s.group(1)


  def parseCustomerId(selfs, str, appseq):
    s = re.search('deid=\"(.*?)\"', str)
    if not s:
      conn_mgr = appseq.getConnectionMgr()
      tenantName = conn_mgr.getTenantName()
      if not tenantName:
        raise ValueError( 'Get the Score Field failed' )
      return tenantName
    return s.group(1)

  def apply( self, appseq ):
    return True
