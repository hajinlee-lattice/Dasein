#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase


class LP_020200_DL_PropDataMatch(StepBase):
  name = 'LP_020200_DL_PropDataMatch'
  description = 'Separate into 2 LGs fro the Load Group:PropDataMatch'
  version = '$Rev: 70934 $'

  def __init__(self, forceApply=False):
    super(LP_020200_DL_PropDataMatch, self).__init__(forceApply)

  def getApplicability(self, appseq):

    lgm = appseq.getLoadGroupMgr()
    if lgm.hasLoadGroup('PropDataMatch_Step1') and lgm.hasLoadGroup('PropDataMatch_Step2'):
      return Applicability.alreadyAppliedPass
    return Applicability.canApply

  def apply(self, appseq):

    success = False

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText('template_type')

    ppdm  = etree.fromstring( lgm.getLoadGroup('PropDataMatch').encode('ascii', 'xmlcharrefreplace') )
    step1 = etree.fromstring( lgm.getLoadGroup('PropDataMatch').encode('ascii', 'xmlcharrefreplace') )
    step2 = etree.fromstring( lgm.getLoadGroup('PropDataMatch').encode('ascii', 'xmlcharrefreplace') )

    step1.set( 'name', 'PropDataMatch_Step1' )
    step1.set( 'alias', 'PropDataMatch_Step1' )
    lgm.setLoadGroup( etree.tostring(step1) )

    step2.set( 'name', 'PropDataMatch_Step2' )
    step2.set( 'alias', 'PropDataMatch_Step2' )
    lgm.setLoadGroup( etree.tostring(step2) )

  #1.  Create PropDataMatch_Step1 and PropDataMatch_Step1 from PropDataMatch
    ppdm  = etree.fromstring( lgm.getLoadGroup('PropDataMatch').encode('ascii', 'xmlcharrefreplace') )
    step1 = etree.fromstring( lgm.getLoadGroup('PropDataMatch').encode('ascii', 'xmlcharrefreplace') )
    step2 = etree.fromstring( lgm.getLoadGroup('PropDataMatch').encode('ascii', 'xmlcharrefreplace') )

    step1.set( 'name', 'PropDataMatch_Step1')
    step1.set( 'alias', 'PropDataMatch_Step1')
    lgm.setLoadGroup( etree.tostring(step1) )

    step2.set( 'name', 'PropDataMatch_Step2')
    step2.set( 'alias', 'PropDataMatch_Step2')
    lgm.setLoadGroup( etree.tostring(step2) )

  #2.  Modify PropDataMatch_Step1
    if type == 'MKTO':
      ppdm_tqs_xml_step1 = lgm.getLoadGroupFunctionality( 'PropDataMatch_Step1', 'extractQueries')
      ppdm_tqs_step1 = etree.fromstring( ppdm_tqs_xml_step1)

      for tq in ppdm_tqs_step1:
        if tq.get('queryName') == 'Q_Timestamp_MatchToPD':
          ppdm_tqs_step1.remove( tq )

      lgm.setLoadGroupFunctionality( 'PropDataMatch_Step1', etree.tostring(ppdm_tqs_step1) )

    #3.  Modify PropDataMatch_Step2
      ppdm_tqs_xml_step21 = lgm.getLoadGroupFunctionality( 'PropDataMatch_Step2', 'extractQueries')
      ppdm_pds_xml_step22 = lgm.getLoadGroupFunctionality( 'PropDataMatch_Step2', 'pdmatches')
      ppdm_tqs_step21 = etree.fromstring( ppdm_tqs_xml_step21)
      ppdm_pds_step22 = etree.fromstring( ppdm_pds_xml_step22)

      for tq in ppdm_tqs_step21:
        if tq.get('queryName') == 'Q_Map_LeadID_PropDataID':
          ppdm_tqs_step21.remove( tq )
          break

      for pd in ppdm_pds_step22:
        if pd.get('n') == 'PD':
          ppdm_pds_step22.remove( pd )
          break

      lgm.setLoadGroupFunctionality( 'PropDataMatch_Step2', etree.tostring(ppdm_tqs_step21) )
      lgm.setLoadGroupFunctionality( 'PropDataMatch_Step2', etree.tostring(ppdm_pds_step22) )

    if type == 'ELQ':
    #2.  Modify PropDataMatch_Step1
      ppdm_tqs_xml_step1 = lgm.getLoadGroupFunctionality( 'PropDataMatch_Step1', 'extractQueries')
      ppdm_tqs_step1 = etree.fromstring( ppdm_tqs_xml_step1)

      for tq in ppdm_tqs_step1:
        if tq.get('queryName') == 'Q_Timestamp_MatchToPD':
          ppdm_tqs_step1.remove( tq )

      lgm.setLoadGroupFunctionality( 'PropDataMatch_Step1', etree.tostring(ppdm_tqs_step1) )

    #3.  Modify PropDataMatch_Step2
      ppdm_tqs_xml_step21 = lgm.getLoadGroupFunctionality( 'PropDataMatch_Step2', 'extractQueries')
      ppdm_pds_xml_step22 = lgm.getLoadGroupFunctionality( 'PropDataMatch_Step2', 'pdmatches')
      ppdm_tqs_step21 = etree.fromstring( ppdm_tqs_xml_step21)
      ppdm_pds_step22 = etree.fromstring( ppdm_pds_xml_step22)

      for tq in ppdm_tqs_step21:
        if tq.get('queryName') == 'Q_Map_ContactID_PropDataID':
          ppdm_tqs_step21.remove( tq )
          break

      for pd in ppdm_pds_step22:
        if pd.get('n') == 'PD':
          ppdm_pds_step22.remove( pd )
          break

      lgm.setLoadGroupFunctionality( 'PropDataMatch_Step2', etree.tostring(ppdm_tqs_step21) )
      lgm.setLoadGroupFunctionality( 'PropDataMatch_Step2', etree.tostring(ppdm_pds_step22) )

    else:
    #2.  Modify PropDataMatch_Step1
      ppdm_tqs_xml_step1 = lgm.getLoadGroupFunctionality( 'PropDataMatch_Step1', 'extractQueries')
      ppdm_tqs_step1 = etree.fromstring( ppdm_tqs_xml_step1)

      for tq in ppdm_tqs_step1:
        if tq.get('queryName') == 'Q_Timestamp_MatchToPD':
          ppdm_tqs_step1.remove( tq )

      lgm.setLoadGroupFunctionality( 'PropDataMatch_Step1', etree.tostring(ppdm_tqs_step1) )

    #3.  Modify PropDataMatch_Step2
      ppdm_tqs_xml_step21 = lgm.getLoadGroupFunctionality( 'PropDataMatch_Step2', 'extractQueries')
      ppdm_pds_xml_step22 = lgm.getLoadGroupFunctionality( 'PropDataMatch_Step2', 'pdmatches')
      ppdm_tqs_step21 = etree.fromstring( ppdm_tqs_xml_step21)
      ppdm_pds_step22 = etree.fromstring( ppdm_pds_xml_step22)

      for tq in ppdm_tqs_step21:
        if tq.get('queryName') == 'Q_Map_Lead_Contact_ID_PropDataID':
          ppdm_tqs_step21.remove( tq )
          break

      for pd in ppdm_pds_step22:
        if pd.get('n') == 'PD':
          ppdm_pds_step22.remove( pd )
          break

      lgm.setLoadGroupFunctionality( 'PropDataMatch_Step2', etree.tostring(ppdm_tqs_step21) )
      lgm.setLoadGroupFunctionality( 'PropDataMatch_Step2', etree.tostring(ppdm_pds_step22) )

    ptld = etree.fromstring(lgm.getLoadGroup('PropDataMatch').encode('ascii', 'xmlcharrefreplace'))

    ptld.set('ng', 'True')
    lgm.setLoadGroup(etree.tostring(ptld))
    ngsxml = '<ngs><ng n="PropDataMatch_Step1"/><ng n="PropDataMatch_Step2"/></ngs>'
    lgm.setLoadGroupFunctionality('PropDataMatch', ngsxml)

    ptld_2 = etree.fromstring(lgm.getLoadGroup('ModelBuild_PropDataMatch').encode('ascii', 'xmlcharrefreplace'))

    ptld_2.set('ng', 'True')
    lgm.setLoadGroup(etree.tostring(ptld_2))
    ngsxml2 = '<ngs><ng n="PropDataMatch"/></ngs>'
    lgm.setLoadGroupFunctionality('ModelBuild_PropDataMatch', ngsxml2)

    success = True

    return success
