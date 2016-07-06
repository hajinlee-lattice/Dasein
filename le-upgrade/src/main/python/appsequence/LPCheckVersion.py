
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re,sys
import requests
from AppArgs import AppArgs
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

    WriteBack_filed = []
    WriteBack_filed_Lead = []
    WriteBack_filed_Contact =[]
    try:
     if type in ('ELQ', 'MKTO'):
       WriteBack_filed = self.parseScore_and_DateField(ptld_lbo_xml, type, version, appseq)
       if WriteBack_filed.__len__()>0:
         appseq.setText( 'score_field', WriteBack_filed[0] )
         appseq.setText( 'score_date_field', WriteBack_filed[1])
       appseq.setText( 'customer_id', self.parseCustomerId(ptld_lbo_xml, appseq) )
     else:
       WriteBack_filed_Lead = self.parseScore_and_DateField(ptld_lbo_xml, type+'Lead', version, appseq)
       WriteBack_filed_Contact = self.parseScore_and_DateField(ptld_lbo_xml, type + 'Contact', version, appseq)
       if WriteBack_filed_Lead.__len__()>0:
         appseq.setText( 'sfdc_lead_score_field', WriteBack_filed_Lead[0] )
         appseq.setText( 'sfdc_lead_score_date_field', WriteBack_filed_Lead[1])
       if WriteBack_filed_Contact.__len__()>0:
           appseq.setText( 'sfdc_contact_score_field', WriteBack_filed_Contact[0] )
           appseq.setText( 'sfdc_contact_score_date_field', WriteBack_filed_Contact[1] )
       appseq.setText( 'customer_id', self.parseCustomerId(ptld_lbo_xml, appseq) )

    except ValueError:
     print "Get the score/scoreDate Field failed"
     return Applicability.cannotApplyFail

    if version == self._template_version:
      return Applicability.canApply
    elif AppArgs.getOptionByIndex(sys.argv,1) == '--missingLead':
      print AppArgs.getOptionByIndex(sys.argv,1)
      return Applicability.canApply
    return Applicability.cannotApplyFail

  def getSFDCContactFromTargetQuery(self, str):
    s = re.search(r'<targetQuery w=\"Workspace\" t=\"2\" name=\"Q_SFDC_Contact_Score\"(.*?)</targetQuery>', str)
    if not s:
      return ''
    return s.group(1)

  def getSFDCLeadFromTargetQuery(self, str):
    s = re.search(r'<targetQuery w=\"Workspace\" t=\"2\" name=\"Q_SFDC_Lead_Score\"(.*?)</targetQuery>', str)
    if not s:
      return ''
    return s.group(1)

  def parseScore_and_DateField(self, str, type, version, appseq):
    lgm = appseq.getLoadGroupMgr()
    s = None
    s_Date = ''
    s_Score= ''
    if version < 2.1:
      if type == 'MKTO':
        s = re.search('<cm scn=\"Score\" tcn=\"(.*?)\"/>', str)
      elif type == 'ELQ':
        s = re.search('<cm scn=\"C_Lattice_Predictive_Score1\" tcn=\"(.*?)\"/>', str)
      elif type == 'SFDCLead':
        s = re.search('queryColumnName=\"C_Lattice_Predictive_Score1\" fsColumnName=\"(.*?)\"',
                      self.getSFDCLeadFromTargetQuery(str))
      elif type == 'SFDCContact':
        s = re.search('queryColumnName=\"C_Lattice_Predictive_Score1\" fsColumnName=\"(.*?)\"',
                      self.getSFDCContactFromTargetQuery(str))
    else:
      if lgm.hasLoadGroup('PushLeadsLastScoredToDestination'):
        ptld_targetQueries_xml = lgm.getLoadGroupFunctionality('PushLeadsLastScoredToDestination', 'targetQueries')
        ptld_targetQueries = etree.fromstring(ptld_targetQueries_xml)
        if type in ('MKTO','ELQ'):
          for targetQuery in ptld_targetQueries.iter('targetQuery'):
            fsColumnMappings = targetQuery.find('fsColumnMappings')
            for fsColumnMapping in fsColumnMappings.iter('fsColumnMapping'):
              if fsColumnMapping.get('queryColumnName').find('Date') > 0 or fsColumnMapping.get('queryColumnName').find('Time') > 0:
                s_Date = fsColumnMapping.get('fsColumnName')
                # print s_Date
              elif fsColumnMapping.get('queryColumnName').find('Score') > 0 and fsColumnMapping.get('queryColumnName').find('Date') < 0 :
                s_Score = fsColumnMapping.get('fsColumnName')
                # print s_Score

        elif type == 'SFDCLead':
          for targetQuery in ptld_targetQueries.iter('targetQuery'):
            fsColumnMappings = targetQuery.find('fsColumnMappings')
            if targetQuery.get('name')=='Q_SFDC_Lead_Score':
              for fsColumnMapping in fsColumnMappings.iter('fsColumnMapping'):
                if fsColumnMapping.get('queryColumnName').find('Date') >0 or fsColumnMapping.get('queryColumnName').find('Time')>0 :
                  s_Date = fsColumnMapping.get('fsColumnName')
                  #print s_Date
                elif fsColumnMapping.get('queryColumnName').find('Score') > 0 and fsColumnMapping.get('queryColumnName').find('Date') < 0 :
                  s_Score = fsColumnMapping.get('fsColumnName')
                  #print s_Score
            else:
              print ('No SFDC_Lead Writing Back')
        elif type == 'SFDCContact':
          for targetQuery in ptld_targetQueries.iter('targetQuery'):
            fsColumnMappings = targetQuery.find('fsColumnMappings')
            if targetQuery.get('name')=='Q_SFDC_Contact_Score':
              for fsColumnMapping in fsColumnMappings.iter('fsColumnMapping'):
                if fsColumnMapping.get('queryColumnName').find('Date') >0 or fsColumnMapping.get('queryColumnName').find('Time')>0 :
                  s_Date = fsColumnMapping.get('fsColumnName')
                  #print s_Date
                elif fsColumnMapping.get('queryColumnName').find('Score') > 0 and fsColumnMapping.get('queryColumnName').find('Date') < 0:
                  s_Score = fsColumnMapping.get('fsColumnName')
                  #print s_Score
            else:
              print ('No SFDC_Contact Writing Back')


    if not s_Date and not s_Score:
      print ('Get the Score and ScoreDate Field failed')
      return ''
    return s_Score, s_Date

  # def parseScoreField(self, str, type, version,appseq):
  #   lgm = appseq.getLoadGroupMgr()
  #   s = None
  #   if version < 2.1:
  #     if type == 'MKTO':
  #       s = re.search('<cm scn=\"Score\" tcn=\"(.*?)\"/>', str)
  #     elif type == 'ELQ':
  #       s = re.search('<cm scn=\"C_Lattice_Predictive_Score1\" tcn=\"(.*?)\"/>', str)
  #     elif type == 'SFDCLead':
  #       s = re.search('queryColumnName=\"C_Lattice_Predictive_Score1\" fsColumnName=\"(.*?)\"', self.getSFDCLeadFromTargetQuery(str))
  #     elif type == 'SFDCContact':
  #       s = re.search('queryColumnName=\"C_Lattice_Predictive_Score1\" fsColumnName=\"(.*?)\"', self.getSFDCContactFromTargetQuery(str))
  #   else:
  #     if type == 'MKTO':
  #       if lgm.hasLoadGroup('PushLeadsLastScoredToDestination'):
  #         ptld_targetQueries_xml = lgm.getLoadGroupFunctionality('PushLeadsLastScoredToDestination', 'targetQueries')
  #         ptld_targetQueries = etree.fromstring(ptld_targetQueries_xml)
  #
  #         for targetQuery in ptld_targetQueries.iter('targetQuery'):
  #           fsColumnMappings = targetQuery.find('fsColumnMappings')
  #           for fsColumnMapping in fsColumnMappings.iter('fsColumnMapping'):
  #             if fsColumnMapping.get('queryColumnName').index('Date'):
  #               fsColumnMappings.remove(fsColumnMapping)
  #     elif type == 'ELQ':
  #       #s = re.search('queryColumnName=\"C_Lattice_Predictive_Score1\" fsColumnName=\"(.*?)\"', str)
  #       elqs_d = re.search('queryColumnName=\"(.*?)Date(.*?)\" fsColumnName=\"(.*?)\" formatter=', str)
  #       if elqs_d:
  #        Sub_elqstr_cnt = str.find('fsColumnName=\"'+elqs_d.group(3)+'\"')
  #        Sub_elqstr_cut = str[Sub_elqstr_cnt-30:Sub_elqstr_cnt+30]
  #        str = str.replace(Sub_elqstr_cut,'')
  #        s = re.search('queryColumnName=\"(.*?)Score(.*?)\" fsColumnName=\"(.*?)\" formatter=', str)
  #     elif type == 'SFDCLead':
  #       #s = re.search('queryColumnName=\"C_Lattice_Predictive_Score1\" fsColumnName=\"(.*?)\"', self.getSFDCLeadFromTargetQuery(str))
  #       sfdc_l = re.search('queryColumnName=\"(.*?)TimeStamp(.*?)\" fsColumnName=\"(.*?)\" formatter=', self.getSFDCLeadFromTargetQuery(str))
  #       if sfdc_l:
  #        Sub_sfdcstr_cnt = self.getSFDCLeadFromTargetQuery(str).find('fsColumnName=\"'+sfdc_l.group(3)+'\"')
  #        Sub_sfdcstr_cut = self.getSFDCLeadFromTargetQuery(str)[Sub_sfdcstr_cnt-30:Sub_sfdcstr_cnt+30]
  #        str = self.getSFDCLeadFromTargetQuery(str).replace(Sub_sfdcstr_cut,'')
  #        s = re.search('queryColumnName=\"(.*?)Score(.*?)\" fsColumnName=\"(.*?)\" formatter=', str)
  #     elif type == 'SFDCContact':
  #       #s = re.search('queryColumnName=\"C_Lattice_Predictive_Score1\" fsColumnName=\"(.*?)\"', self.getSFDCContactFromTargetQuery(str))
  #       sfdc_c = re.search('queryColumnName=\"(.*?)Date(.*?)\" fsColumnName=\"(.*?)\" formatter=', self.getSFDCContactFromTargetQuery(str))
  #       if sfdc_c:
  #        Sub_sfdcstr_cnt = self.getSFDCContactFromTargetQuery(str).find('fsColumnName=\"'+sfdc_c.group(3)+'\"')
  #        Sub_sfdcstr_cut = self.getSFDCContactFromTargetQuery(str)[Sub_sfdcstr_cnt-30:Sub_sfdcstr_cnt+30]
  #        str = self.getSFDCContactFromTargetQuery(str).replace(Sub_sfdcstr_cut,'')
  #        s = re.search('queryColumnName=\"(.*?)Score(.*?)\" fsColumnName=\"(.*?)\" formatter=', str)
  #
  #   if not s:
  #     print ( 'Get the Score Field failed' )
  #     return ''
  #   return s.group(3)

  # def parseScoreDateField(self, str, type, version):
  #
  #   s = None
  #
  #   if type == 'MKTO':
  #     #s = re.search('queryColumnName=\"Score_Date_Time\" fsColumnName=\"(.*?)\"', str)
  #     s = re.search('queryColumnName=\"(.*?)Date(.*?)\" fsColumnName=\"(.*?)\" formatter=', str)
  #   elif type == 'ELQ':
  #     #s = re.search('queryColumnName=\"C_Lattice_LastScoreDate1\" fsColumnName=\"(.*?)\"', str)
  #     s = re.search('queryColumnName=\"(.*?)Date(.*?)\" fsColumnName=\"(.*?)\" formatter=', str)
  #   elif type == 'SFDCLead':
  #     #s = re.search('queryColumnName=\"C_Lattice_LastScoreDate1\" fsColumnName=\"(.*?)\"', self.getSFDCLeadFromTargetQuery(str))
  #     s = re.search('queryColumnName=\"(.*?)TimeStamp(.*?)\" fsColumnName=\"(.*?)\" formatter=', self.getSFDCLeadFromTargetQuery(str))
  #     print s.group(1)
  #     print s.group(2), s.group(3)
  #   # elif type == 'SFDCContact':
  #   #   #s = re.search('queryColumnName=\"C_Lattice_LastScoreDate1\" fsColumnName=\"(.*?)\"', self.getSFDCContactFromTargetQuery(str))
  #   #    s = re.search('queryColumnName=\"(.*?)TimeStamp(.*?)\" fsColumnName=\"(.*?)\" formatter=', self.getSFDCContactFromTargetQuery(str))
  #
  #
  #     if not s:
  #       print('Get the ScoreDate Field failed')
  #     return ''
  #
  #   print s.group(3)
  #   return s.group(3)


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
