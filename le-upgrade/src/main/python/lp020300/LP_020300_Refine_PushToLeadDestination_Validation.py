#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import appsequence
import liaison


class LP_020300_Refine_PushToLeadDestination_Validation(StepBase):
  name = 'LP_020300_Refine_PushToLeadDestination_Validation'
  description = 'Refine PushToLeadDestination_Validation'
  version = '$Rev: 70934 $'

  def __init__(self, forceApply=False):
    super(LP_020300_Refine_PushToLeadDestination_Validation, self).__init__(forceApply)

  def getApplicability(self, appseq):

    lgm = appseq.getLoadGroupMgr()
    conn_mgr = appseq.getConnectionMgr()

    if not lgm.hasLoadGroup('PushToLeadDestination_Validation') \
      and not conn_mgr.getQuery("Q_Summary_Lead_Status"):

      return Applicability.cannotApplyPass
    return Applicability.canApply

  def apply(self, appseq):

    success = False
# Begin DL LG Upgrade
    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText( 'template_type' )
    lgm.getLoadGroup( 'PushToLeadDestination_Validation')
    kcs_xml = ''
    if type == 'MKTO':
      kcs_xml = '''<targetQueries>
      <targetQuery w="Workspace" t="2" name="Q_Summary_Lead_Status" alias="Q_Summary_Lead_Status" isc="False" threshold="10000" fsTableName="Summary_Lead_Status" sourceType="1" jobType="20" ignoreOptionsValue="0" exportToDestDirectly="True" exportRule="4" fileExt="bcp" rowTerminator="\\0\\r" columnTerminator="\\0" edts="False" destType="SQL" destDataProvider="SQL_LeadValidation_DataProvider" cto="0">
          <schemas />
          <specs />
          <fsColumnMappings>
            <fsColumnMapping queryColumnName="Const_TenantName" fsColumnName="TenantName" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
            <fsColumnMapping queryColumnName="Lead_Status" fsColumnName="Lead_Status" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
            <fsColumnMapping queryColumnName="Time_OfSubmission_PushToScoring" fsColumnName="Time_OfSubmission_PushToScoring" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
            <fsColumnMapping queryColumnName="Time_OfCompletion_LoadScoredLeads" fsColumnName="Time_OfCompletion_LoadScoredLeads" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
            <fsColumnMapping queryColumnName="Time_OfCompletion_PushToDestination" fsColumnName="Time_OfCompletion_PushToDestination" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
            <fsColumnMapping queryColumnName="MKTO_LeadRecord_ID" fsColumnName="LeadID" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
          </fsColumnMappings>
          <excludeColumns />
          <validationQueries>
            <validationQuery name="Q_Validation_Lead_Status" alias="Q_Validation_Lead_Status" type="2">
              <schemas />
              <specs />
            </validationQuery>
          </validationQueries>
          <constantRows />
          <kcs>
            <kc n="LeadID" />
            <kc n="TenantName" />
          </kcs>
          <fut dn="" d="" n="" iet="False" iets="False" t="1" />
        </targetQuery>
        </targetQueries>'''
    elif type =='ELQ':
      kcs_xml = '''<targetQueries>
      <targetQuery w="Workspace" t="2" name="Q_Summary_Lead_Status" alias="Q_Summary_Lead_Status" isc="False" threshold="10000" fsTableName="Summary_Lead_Status" sourceType="1" jobType="20" ignoreOptionsValue="0" exportToDestDirectly="True" exportRule="4" fileExt="bcp" rowTerminator="\\0\\r" columnTerminator="\\0" edts="False" destType="SQL" destDataProvider="SQL_LeadValidation_DataProvider" cto="1">
          <schemas />
          <specs />
          <fsColumnMappings>
            <fsColumnMapping queryColumnName="Const_TenantName" fsColumnName="TenantName" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
            <fsColumnMapping queryColumnName="Lead_Status" fsColumnName="Lead_Status" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
            <fsColumnMapping queryColumnName="Time_OfSubmission_PushToScoring" fsColumnName="Time_OfSubmission_PushToScoring" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
            <fsColumnMapping queryColumnName="Time_OfCompletion_LoadScoredLeads" fsColumnName="Time_OfCompletion_LoadScoredLeads" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
            <fsColumnMapping queryColumnName="Time_OfCompletion_PushToDestination" fsColumnName="Time_OfCompletion_PushToDestination" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
            <fsColumnMapping queryColumnName="ELQ_Contact_ContactID" fsColumnName="LeadID" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
          </fsColumnMappings>
          <excludeColumns />
          <validationQueries>
            <validationQuery name="Q_Validation_Lead_Status" alias="Q_Validation_Lead_Status" type="2">
              <schemas />
              <specs />
            </validationQuery>
          </validationQueries>
          <constantRows />
          <kcs>
            <kc n="LeadID" />
            <kc n="TenantName" />
          </kcs>
          <fut dn="" d="" n="" iet="False" iets="False" t="1" />
        </targetQuery>
        </targetQueries>'''
    else:
      kcs_xml = '''<targetQueries>
      <targetQuery w="Workspace" t="2" name="Q_Summary_Lead_Status" alias="Q_Summary_Lead_Status" isc="False" threshold="10000" fsTableName="Summary_Lead_Status" sourceType="1" jobType="20" ignoreOptionsValue="0" exportToDestDirectly="True" exportRule="4" fileExt="bcp" rowTerminator="\\0\\r" columnTerminator="\\0" edts="False" destType="SQL" destDataProvider="SQL_LeadValidation_DataProvider" cto="0">
          <schemas />
          <specs />
          <fsColumnMappings>
            <fsColumnMapping queryColumnName="Const_TenantName" fsColumnName="TenantName" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
            <fsColumnMapping queryColumnName="Lead_Status" fsColumnName="Lead_Status" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
            <fsColumnMapping queryColumnName="Time_OfSubmission_PushToScoring" fsColumnName="Time_OfSubmission_PushToScoring" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
            <fsColumnMapping queryColumnName="Time_OfCompletion_LoadScoredLeads" fsColumnName="Time_OfCompletion_LoadScoredLeads" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
            <fsColumnMapping queryColumnName="Time_OfCompletion_PushToDestination" fsColumnName="Time_OfCompletion_PushToDestination" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
            <fsColumnMapping queryColumnName="SFDC_Lead_Contact_ID" fsColumnName="LeadID" formatter="" type="0" ignoreType="0" ignoreOptions="0" formatColumnName="False" charsToFormat="" />
          </fsColumnMappings>
          <excludeColumns />
          <validationQueries>
            <validationQuery name="Q_Validation_Lead_Status" alias="Q_Validation_Lead_Status" type="2">
              <schemas />
              <specs />
            </validationQuery>
          </validationQueries>
          <constantRows />
          <kcs>
            <kc n="LeadID" />
            <kc n="TenantName" />
          </kcs>
          <fut dn="" d="" n="" iet="False" iets="False" t="1" />
        </targetQuery>
        </targetQueries>'''
    lgm.setLoadGroupFunctionality( 'PushToLeadDestination_Validation', kcs_xml )
  # Set LG PushToLeadDestination_Validation back to PushToLeadDestination
    ptld = etree.fromstring(lgm.getLoadGroup('PushToLeadDestination').encode('ascii', 'xmlcharrefreplace'))

    ptld.set('ng', 'True')
    lgm.setLoadGroup(etree.tostring(ptld))
    ngsxml = '<ngs><ng n="LoadScoredLeads_Step1"/><ng n="LoadScoredLeads_Step2"/><ng n="PushLeadsLastScoredToDestination"/><ng n="PushToLeadDestination_TimeStamp"/><ng n="PushToLeadDestination_Validation"/></ngs>'
    lgm.setLoadGroupFunctionality('PushToLeadDestination', ngsxml)

# Begin visiDB Query Upgrade
    conn_mgr = appseq.getConnectionMgr()
    filters_new1 = []
    filters_new2 = []
    if type == 'MKTO':
          #Modify the Filter in Q_Validation_Lead_Status
          Q_Validation_Lead_Status    = conn_mgr.getQuery("Q_Validation_Lead_Status")
          filter1 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetIdentifier(ContainerElementName("Filter_FailedScoring"))')
          filters_new1.append(filter1)
          Q_Validation_Lead_Status.filters_ = filters_new1

          #Modify the Filter in Q_Summary_Lead_Status
          Q_Summary_Lead_Status    = conn_mgr.getQuery("Q_Summary_Lead_Status")
          filter2 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetIdentifier(ContainerElementName("Filter_FailedScoring"))')
          filters_new2.append(filter2)
          Q_Summary_Lead_Status.filters_ = filters_new2

          conn_mgr.setSpec('Q_Validation_Lead_Status',Q_Validation_Lead_Status.SpecLatticeNamedElements())
          conn_mgr.setSpec('Q_Summary_Lead_Status',Q_Summary_Lead_Status.SpecLatticeNamedElements())
    elif type == 'ELQ':
          #Modify the Filter in Q_Validation_Lead_Status
          Q_Validation_Lead_Status    = conn_mgr.getQuery("Q_Validation_Lead_Status")
          filter1 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetIdentifier(ContainerElementName("Filter_FailedScoring"))')
          filters_new1.append(filter1)
          Q_Validation_Lead_Status.filters_ = filters_new1

          #Modify the Filter in Q_Summary_Lead_Status
          Q_Summary_Lead_Status    = conn_mgr.getQuery("Q_Summary_Lead_Status")
          filter2 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetIdentifier(ContainerElementName("Filter_FailedScoring"))')
          filters_new2.append(filter2)
          Q_Summary_Lead_Status.filters_ = filters_new2

          conn_mgr.setSpec('Q_Validation_Lead_Status',Q_Validation_Lead_Status.SpecLatticeNamedElements())
          conn_mgr.setSpec('Q_Summary_Lead_Status',Q_Summary_Lead_Status.SpecLatticeNamedElements())
    else:
          #Modify the Filter in Q_Validation_Lead_Status
          Q_Validation_Lead_Status    = conn_mgr.getQuery("Q_Validation_Lead_Status")
          filter1 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetIdentifier(ContainerElementName("Filter_FailedScoring"))')
          filters_new1.append(filter1)
          Q_Validation_Lead_Status.filters_ = filters_new1

          #Modify the Filter in Q_Summary_Lead_Status
          Q_Summary_Lead_Status    = conn_mgr.getQuery("Q_Summary_Lead_Status")
          filter2 = liaison.ExpressionVDBImplGeneric('LatticeAddressSetIdentifier(ContainerElementName("Filter_FailedScoring"))')
          filters_new2.append(filter2)
          Q_Summary_Lead_Status.filters_ = filters_new2

          conn_mgr.setSpec('Q_Validation_Lead_Status',Q_Validation_Lead_Status.SpecLatticeNamedElements())
          conn_mgr.setSpec('Q_Summary_Lead_Status',Q_Summary_Lead_Status.SpecLatticeNamedElements())

  # Add the 3 new tables into the consolidation
    ce_ces_xml = lgm.getLoadGroupFunctionality('ConsolidateExtracts', 'ces')
    ce_ces = etree.fromstring(ce_ces_xml)
    ce_ws = ce_ces.find('ws')
    ce_ce_xml = '''
    <ws w="Workspace" rt="3" tbr="5">
          <ce itn="PD_HGData_Pivoted_Source_Import" etr="1" sei="0"/>
          <ce itn="PD_BuiltWith_Pivoted_Source_Import" etr="1" sei="0"/>
          <ce itn="PD_OrbIntelligence_Source_Import" etr="1" sei="0"/>
    </ws>
  '''
    ce = etree.fromstring(ce_ce_xml)
    ce_list = list(ce.iter('ce'))
    for ce in ce_list:
      ce_ws.append(ce)
      lgm.setLoadGroupFunctionality( 'ConsolidateExtracts', etree.tostring(ce_ces) )

    success = True

    return success
