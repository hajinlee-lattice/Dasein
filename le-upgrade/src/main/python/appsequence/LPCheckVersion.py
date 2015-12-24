
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re
import requests
from lxml import etree

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
      d = re.search( 'LatticeFunctionExpressionConstant\(\"play(.*?) Template:\".*LatticeFunctionExpressionConstant\(\"(.*?)\"', defn )
      if c:
        type = c.group(1)
        version = c.group(2)
      elif d:
        type = d.group(1)
        version = d.group(2)
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

    if version < 2.1:
      if not lgm.hasLoadGroup('PushToLeadDestination_Step1'):
        return Applicability.cannotApplyFail
      if type in ('MKTO', 'ELQ'):
        ptld_lbo_xml = lgm.getLoadGroupFunctionality('PushToLeadDestination_Step1', 'lssbardouts')
      else:
        ptld_lbo_xml = lgm.getLoadGroupFunctionality('PushToLeadDestination_Step2', 'targetQueries')
    elif version < '2.1.2':
      if not lgm.hasLoadGroup('PushLeadsInDanteToDestination'):
        return Applicability.cannotApplyFail
      ptld_lbo_xml = lgm.getLoadGroupFunctionality('PushLeadsInDanteToDestination', 'targetQueries')
    else :
      if not lgm.hasLoadGroup('PushLeadsLastScoredToDestination'):
        return Applicability.cannotApplyFail
      ptld_lbo_xml = lgm.getLoadGroupFunctionality('PushLeadsLastScoredToDestination', 'targetQueries')

    try:
      if type in ('ELQ', 'MKTO'):
        appseq.setText( 'score_field', self.parseScoreField(ptld_lbo_xml, type, version) )
        appseq.setText( 'score_date_field', self.parseScoreDateField(ptld_lbo_xml, type, version) )
        appseq.setText( 'customer_id', self.parseCustomerId(ptld_lbo_xml, appseq) )
      else:
        appseq.setText( 'sfdc_lead_score_field', self.parseScoreField(ptld_lbo_xml, type+'Lead', version) )
        appseq.setText( 'sfdc_lead_score_date_field', self.parseScoreDateField(ptld_lbo_xml, type+'Lead', version) )
        appseq.setText( 'sfdc_contact_score_field', self.parseScoreField(ptld_lbo_xml, type+'Contact', version) )
        appseq.setText( 'sfdc_contact_score_date_field', self.parseScoreDateField(ptld_lbo_xml, type+'Contact', version) )
        appseq.setText( 'customer_id', self.parseCustomerId(ptld_lbo_xml, appseq) )

    except ValueError:
      print "Get the score/scoreDate Field failed"
      return Applicability.cannotApplyFail

    if version == self._template_version:
      return Applicability.canApply

    return Applicability.cannotApplyFail

  def getSFDCContactFromTargetQuery(self, str):
    s = re.search(r'<targetQuery w=\"Workspace\" t=\"2\" name=\"Q_SFDC_Contact_Score\".*?</targetQuery>', str)
    return s.group(1)

  def getSFDCLeadFromTargetQuery(self, str):
    s = re.search(r'<targetQuery w=\"Workspace\" t=\"2\" name=\"Q_SFDC_Lead_Score\".*?</targetQuery>', str)
    return s.group(1)

  def parseScoreField(self, str, type, version):
    if version < 2.1:
      if type == 'MKTO':
        s = re.search('<cm scn=\"Score\" tcn=\"(.*?)\"/>', str)
      elif type == 'ELQ':
        s = re.search('<cm scn=\"C_Lattice_Predictive_Score1\" tcn=\"(.*?)\"/>', str)
      elif type == 'SFDCLead':
        s = re.search('queryColumnName=\"C_Lattice_Predictive_Score1\" fsColumnName=\"(.*?)\"', self.getSFDCLeadFromTargetQuery(str))
      elif type == 'SFDCContact':
        s = re.search('queryColumnName=\"C_Lattice_Predictive_Score1\" fsColumnName=\"(.*?)\"', self.getSFDCContactFromTargetQuery(str))
    else:
      if type == 'MKTO':
        s = re.search('queryColumnName=\"Score\" fsColumnName=\"(.*?)\"', str)
      elif type == 'ELQ':
        s = re.search('queryColumnName=\"C_Lattice_Predictive_Score1\" fsColumnName=\"(.*?)\"', str)
      elif type == 'SFDCLead':
        s = re.search('queryColumnName=\"C_Lattice_Predictive_Score1\" fsColumnName=\"(.*?)\"', self.getSFDCLeadFromTargetQuery(str))
      elif type == 'SFDCContact':
        s = re.search('queryColumnName=\"C_Lattice_Predictive_Score1\" fsColumnName=\"(.*?)\"', self.getSFDCContactFromTargetQuery(str))

    if not s:
      raise ValueError( 'Get the Score Field failed' )
    return s.group(1)



  def parseScoreDateField(self, str, type, version):
    if type == 'MKTO':
      s = re.search('queryColumnName=\"Score_Date_Time\" fsColumnName=\"(.*?)\"', str)
    elif type == 'ELQ':
      s = re.search('queryColumnName=\"C_Lattice_LastScoreDate1\" fsColumnName=\"(.*?)\"', str)
    elif type == 'SFDCLead':
      s = re.search('queryColumnName=\"C_Lattice_LastScoreDate1\" fsColumnName=\"(.*?)\"', self.getSFDCLeadFromTargetQuery(str))
    elif type == 'SFDCLead':
      s = re.search('queryColumnName=\"C_Lattice_LastScoreDate1\" fsColumnName=\"(.*?)\"', self.getSFDCContactFromTargetQuery(str))

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
