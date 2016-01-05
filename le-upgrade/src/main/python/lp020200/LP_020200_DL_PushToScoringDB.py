#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase


class LP_020200_DL_PushToScoringDB(StepBase):
  name = 'LP_020200_DL_PushToScoringDB'
  description = 'Separate into 2 LGs fro the Load Group:PushToScoringDB'
  version = '$Rev: 70934 $'

  def __init__(self, forceApply=False):
    super(LP_020200_DL_PushToScoringDB, self).__init__(forceApply)

  def getApplicability(self, appseq):

    lgm = appseq.getLoadGroupMgr()
    if not lgm.hasLoadGroup('PushToScoringDB_Step1') \
      and not lgm.hasLoadGroup('PushToScoringDB_Step2') \
      and not lgm.hasLoadGroup('PushToScoringDB'):

      return Applicability.cannotApplyPass
    return Applicability.canApply

  def apply(self, appseq):

    success = False

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText('template_type')

    psdb  = etree.fromstring( lgm.getLoadGroup('PushToScoringDB_Step2').encode('ascii', 'xmlcharrefreplace') )
    step1 = etree.fromstring( lgm.getLoadGroup('PushToScoringDB_Step2').encode('ascii', 'xmlcharrefreplace') )

    step1.set( 'name', 'PushToScoringDB_Step1' )
    step1.set( 'alias', 'PushToScoringDB_Step1' )
    lgm.setLoadGroup( etree.tostring(step1))

    if type == 'MKTO':
      psdb_eqs_xml = lgm.getLoadGroupFunctionality('PushToScoringDB_Step1','extractQueries')
      psdb_eqs = etree.fromstring(psdb_eqs_xml)
      psdb_lbi_xml = lgm.getLoadGroupFunctionality('PushToScoringDB_Step2','lssbardins')
      psdb_lbi = etree.fromstring(psdb_lbi_xml)

      for eq in psdb_eqs:
        if eq.get('queryAlias') == 'Q_Timestamp_PushToScoringIncr':
          psdb_eqs.remove(eq)

      for lbi in psdb_lbi:
        if lbi.get('n') == 'PushToScoringDB_LSS_Bard_In':
          psdb_lbi.remove(lbi)

    elif type == 'ELQ':
      psdb_eqs_xml = lgm.getLoadGroupFunctionality('PushToScoringDB_Step1','extractQueries')
      psdb_eqs = etree.fromstring(psdb_eqs_xml)
      psdb_lbi_xml = lgm.getLoadGroupFunctionality('PushToScoringDB_Step2','lssbardins')
      psdb_lbi = etree.fromstring(psdb_lbi_xml)

      for eq in psdb_eqs:
        if eq.get('queryAlias') == 'Q_Timestamp_PushToScoringIncr':
          psdb_eqs.remove(eq)
        if eq.get('queryAlias') == 'Q_Summary_DownloadsBeforeScoring':
          psdb_eqs.remove(eq)

      for lbi in psdb_lbi:
        if lbi.get('n') == 'PushToScoringDB_LSS_Bard_In':
          psdb_lbi.remove(lbi)


    else:
      psdb_eqs_xml = lgm.getLoadGroupFunctionality('PushToScoringDB_Step1','extractQueries')
      psdb_eqs = etree.fromstring(psdb_eqs_xml)
      psdb_lbi_xml = lgm.getLoadGroupFunctionality('PushToScoringDB_Step2','lssbardins')
      psdb_lbi = etree.fromstring(psdb_lbi_xml)

      for eq in psdb_eqs:
        if eq.get('queryAlias') == 'Q_Timestamp_PushToScoringIncr':
          psdb_eqs.remove(eq)
        if eq.get('queryAlias') == 'Q_Summary_DownloadsBeforeScoring':
          psdb_eqs.remove(eq)

      for lbi in psdb_lbi:
        if lbi.get('n') == 'PushToScoringDB_LSS_Bard_In':
          psdb_lbi.remove(lbi)

    lgm.setLoadGroupFunctionality( 'PushToScoringDB_Step1', etree.tostring(psdb_eqs) )
    lgm.setLoadGroupFunctionality( 'PushToScoringDB_Step2', etree.tostring(psdb_lbi) )

    ptld = etree.fromstring(lgm.getLoadGroup('PushToScoringDB').encode('ascii', 'xmlcharrefreplace'))

    ptld.set('ng', 'True')
    lgm.setLoadGroup(etree.tostring(ptld))
    ngsxml = '<ngs><ng n="PushToScoringDB_Step1"/><ng n="PushToScoringDB_Step2"/></ngs>'
    lgm.setLoadGroupFunctionality('PushToScoringDB', ngsxml)

    success = True

    return success
