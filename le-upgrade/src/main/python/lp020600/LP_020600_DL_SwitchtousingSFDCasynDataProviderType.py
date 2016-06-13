
#
# $LastChangedBy: lyan $
# $LastChangedDate: 2015-11-20 01:50:33 +0800 (Fri, 20 Nov 2015) $
# $Rev: 71049 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import liaison

class LP_020600_DL_SwitchtousingSFDCasynDataProviderType( StepBase ):
  
  name        = 'Switch to using SFDC asynchronous DataProvider Type'
  description = 'Upgrade Modified Specs from 2.5.1 to 2.6.0:LP_020600_DL_SwitchtousingSFDCasynDataProviderType'
  version     = '$Rev: 71049 $'
  def __init__( self, forceApply = False ):
    super( LP_020600_DL_SwitchtousingSFDCasynDataProviderType, self ).__init__( forceApply )

  def getApplicability( self, appseq ):
    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText('template_type')
    if  type in('MKTO','ELQ') :
      if not lgm.hasLoadGroup('ForModeling_LoadCRMDataAdditionalOpportunityTables')\
        and lgm.hasLoadGroup('LoadCRMData')\
        and lgm.hasLoadGroup('LoadCRMData_2Days')\
        and lgm.hasLoadGroup('LoadCRMData_Extended')\
        and lgm.hasLoadGroup('LoadCRMDataAdditionalOpportunityTables')\
        and lgm.hasLoadGroup('LoadCRMDataForModeling'):
        return Applicability.cannotApplyPass
    else:
      if not lgm.hasLoadGroup('ForModeling_LoadCRMDataAdditionalOpportunityTables')\
        and lgm.hasLoadGroup('Diagnostic_LoadLeads')\
        and lgm.hasLoadGroup('Diagnostic_LoadLeads_Bulk')\
        and lgm.hasLoadGroup('LoadCRMData_ModLeads')\
        and lgm.hasLoadGroup('LoadCRMData_NewLeads')\
        and lgm.hasLoadGroup('LoadCRMData_Others')\
        and lgm.hasLoadGroup('LoadCRMData_2Days')\
        and lgm.hasLoadGroup('LoadCRMData_Extended')\
        and lgm.hasLoadGroup('LoadCRMDataAdditionalOpportunityTables')\
        and lgm.hasLoadGroup('LoadScoredLeads_Step1')\
        and lgm.hasLoadGroup('PushLeadsInDanteToDestination')\
        and lgm.hasLoadGroup('PushLeadsLastScoredToDestination')\
        and lgm.hasLoadGroup('LoadCRMDataForModeling'):
        return Applicability.cannotApplyPass
    return Applicability.canApply


  def apply( self, appseq ):

    success = False
    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText('template_type')
#1.SwitchtousingSFDCasynDataProviderType for the LG: ForModeling_LoadCRMDataAdditionalOpportunityTables
    fmlt_tq_xml = lgm.getLoadGroupFunctionality('ForModeling_LoadCRMDataAdditionalOpportunityTables','rdss')
    fmlt_tq = etree.fromstring( fmlt_tq_xml )
    for rd in fmlt_tq:
      if rd.get('cn') == 'SFDC_DataProvider':
        rd.set('cn','SFDC2_DataProvider')

    lgm.setLoadGroupFunctionality( 'ForModeling_LoadCRMDataAdditionalOpportunityTables', etree.tostring(fmlt_tq))

#2.SwitchtousingSFDCasynDataProviderType for the LG: LoadCRMData
    if  type in('MKTO','ELQ') :
      lcrm_tq_xml = lgm.getLoadGroupFunctionality('LoadCRMData','rdss')
      lcrm_tq = etree.fromstring( lcrm_tq_xml )
      for rd in lcrm_tq:
        if rd.get('cn') == 'SFDC_DataProvider':
          rd.set('cn','SFDC2_DataProvider')
      lgm.setLoadGroupFunctionality( 'LoadCRMData', etree.tostring(lcrm_tq))
    else:
      pass

#3.SwitchtousingSFDCasynDataProviderType for the LG: LoadCRMData_2Days
    lcrm2_tq_xml = lgm.getLoadGroupFunctionality('LoadCRMData_2Days','rdss')
    lcrm2_tq = etree.fromstring( lcrm2_tq_xml )
    for rd in lcrm2_tq:
      if rd.get('cn') == 'SFDC_DataProvider':
        rd.set('cn','SFDC2_DataProvider')

    lgm.setLoadGroupFunctionality( 'LoadCRMData_2Days', etree.tostring(lcrm2_tq))

#4.SwitchtousingSFDCasynDataProviderType for the LG: LoadCRMData_Extended
    lcrme_tq_xml = lgm.getLoadGroupFunctionality('LoadCRMData_Extended','rdss')
    lcrme_tq = etree.fromstring( lcrme_tq_xml )
    for rd in lcrme_tq:
      if rd.get('cn') == 'SFDC_DataProvider':
        rd.set('cn','SFDC2_DataProvider')

    lgm.setLoadGroupFunctionality( 'LoadCRMData_Extended', etree.tostring(lcrme_tq))

#5.SwitchtousingSFDCasynDataProviderType for the LG: LoadCRMDataAdditionalOpportunityTables
    lcrmo_tq_xml = lgm.getLoadGroupFunctionality('LoadCRMDataAdditionalOpportunityTables','rdss')
    lcrmo_tq = etree.fromstring( lcrmo_tq_xml )
    for rd in lcrmo_tq:
      if rd.get('cn') == 'SFDC_DataProvider':
        rd.set('cn','SFDC2_DataProvider')

    lgm.setLoadGroupFunctionality( 'LoadCRMDataAdditionalOpportunityTables', etree.tostring(lcrmo_tq))

#6.SwitchtousingSFDCasynDataProviderType for the LG: LoadCRMDataForModeling
    lcrmm_tq_xml = lgm.getLoadGroupFunctionality('LoadCRMDataForModeling','rdss')
    lcrmm_tq = etree.fromstring( lcrmm_tq_xml )
    for rd in lcrmm_tq:
      if rd.get('cn') == 'SFDC_DataProvider':
        rd.set('cn','SFDC2_DataProvider')

    lgm.setLoadGroupFunctionality( 'LoadCRMDataForModeling', etree.tostring(lcrmm_tq))

#7.SwitchtousingSFDCasynDataProviderType for the SFDC LG
    #7.1 SwitchtousingSFDCasynDataProviderType for the SFDC LG: LoadCRMData_ModLeads
    if  type == 'SFDC':
      lcrm_ml_tq_xml = lgm.getLoadGroupFunctionality('LoadCRMData_ModLeads','rdss')
      lcrm_ml_tq = etree.fromstring( lcrm_ml_tq_xml )
      for rd in lcrm_ml_tq:
        if rd.get('cn') == 'SFDC_DataProvider':
          rd.set('cn','SFDC2_DataProvider')
      lgm.setLoadGroupFunctionality( 'LoadCRMData_ModLeads', etree.tostring(lcrm_ml_tq))
    #7.2 SwitchtousingSFDCasynDataProviderType for the SFDC LG: LoadCRMData_NewLeads
      lcrm_nl_tq_xml = lgm.getLoadGroupFunctionality('LoadCRMData_NewLeads','rdss')
      lcrm_nl_tq = etree.fromstring( lcrm_nl_tq_xml )
      for rd in lcrm_nl_tq:
        if rd.get('cn') == 'SFDC_DataProvider':
          rd.set('cn','SFDC2_DataProvider')
      lgm.setLoadGroupFunctionality( 'LoadCRMData_NewLeads', etree.tostring(lcrm_nl_tq))
    #7.3 SwitchtousingSFDCasynDataProviderType for the SFDC LG: LoadCRMData_Others
      lcrm_ot_tq_xml = lgm.getLoadGroupFunctionality('LoadCRMData_Others','rdss')
      lcrm_ot_tq = etree.fromstring( lcrm_ot_tq_xml )
      for rd in lcrm_ot_tq:
        if rd.get('cn') == 'SFDC_DataProvider':
          rd.set('cn','SFDC2_DataProvider')
      lgm.setLoadGroupFunctionality( 'LoadCRMData_Others', etree.tostring(lcrm_ot_tq))
    #7.4 SwitchtousingSFDCasynDataProviderType for the SFDC LG: Adhoc_LoadCRMData_FromSpecificDay
      if lgm.hasLoadGroup('Adhoc_LoadCRMData_FromSpecificDay'):
        alfs_tq_xml = lgm.getLoadGroupFunctionality('Adhoc_LoadCRMData_FromSpecificDay','rdss')
        alfs_tq = etree.fromstring( alfs_tq_xml )
        for rd in alfs_tq:
          if rd.get('cn') == 'SFDC_DataProvider':
            rd.set('cn','SFDC2_DataProvider')
          lgm.setLoadGroupFunctionality( 'Adhoc_LoadCRMData_FromSpecificDay', etree.tostring(alfs_tq))
    #7.5 SwitchtousingSFDCasynDataProviderType for the SFDC LG: Diagnostic_LoadLeads
      dill_tq_xml = lgm.getLoadGroupFunctionality('Diagnostic_LoadLeads','rdss')
      dill_tq = etree.fromstring( dill_tq_xml )
      for rd in dill_tq:
        if rd.get('cn') == 'SFDC_DataProvider':
          rd.set('cn','SFDC2_DataProvider')
      lgm.setLoadGroupFunctionality( 'Diagnostic_LoadLeads', etree.tostring(dill_tq))
    #7.6 SwitchtousingSFDCasynDataProviderType for the SFDC LG: Diagnostic_LoadLeads_Bulk
      dllb_tq_xml = lgm.getLoadGroupFunctionality('Diagnostic_LoadLeads_Bulk','rdss')
      dllb_tq = etree.fromstring( dllb_tq_xml )
      for rd in dllb_tq:
        if rd.get('cn') == 'SFDC_DataProvider':
          rd.set('cn','SFDC2_DataProvider')
      lgm.setLoadGroupFunctionality( 'Diagnostic_LoadLeads_Bulk', etree.tostring(dllb_tq))

      #7.7 SwitchtousingSFDCasynDataProviderType for the SFDC LG: LoadScoredLeads_Step1
      lsls_tq_xml = lgm.getLoadGroupFunctionality('LoadScoredLeads_Step1','lssbardouts')
      lsls_tq = etree.fromstring( lsls_tq_xml )
      for rd in lsls_tq:
        if rd.get('dp') == 'SFDC_DataProvider':
          rd.set('dp','SFDC2_DataProvider')
      lgm.setLoadGroupFunctionality( 'LoadScoredLeads_Step1', etree.tostring(lsls_tq))

      #7.8 SwitchtousingSFDCasynDataProviderType for the SFDC LG: PushLeadsInDanteToDestination
      pldd_tq_xml = lgm.getLoadGroupFunctionality('PushLeadsInDanteToDestination','targetQueries')
      pldd_tq = etree.fromstring( pldd_tq_xml )
      for rd in pldd_tq:
        if rd.get('destDataProvider') == 'SFDC_DataProvider':
          rd.set('destDataProvider','SFDC2_DataProvider')
          rd.set('destType','SFDC2')
      lgm.setLoadGroupFunctionality( 'PushLeadsInDanteToDestination', etree.tostring(pldd_tq))

      #7.9 SwitchtousingSFDCasynDataProviderType for the SFDC LG: PushLeadsLastScoredToDestination
      pltd_tq_xml = lgm.getLoadGroupFunctionality('PushLeadsLastScoredToDestination','targetQueries')
      pltd_tq = etree.fromstring( pltd_tq_xml )
      for rd in pltd_tq:
        if rd.get('destDataProvider') == 'SFDC_DataProvider':
          rd.set('destDataProvider','SFDC2_DataProvider')
          rd.set('destType','SFDC2')
      lgm.setLoadGroupFunctionality( 'PushLeadsLastScoredToDestination', etree.tostring(pltd_tq))
    else:
      pass
    success = True

    return success