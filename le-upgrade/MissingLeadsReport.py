#!/usr/bin/python

import os, sys, re
from lxml import etree
from liaison import *
from appsequence import Applicability, AppSequence, StepBase


class MissingLeadsReport(StepBase):
  name = 'LP_020100_VDB_ModifiedColumns'
  description = 'Upgrade Modified Specs from 2.0.1 to 2.1.0'
  version = '$Rev: 70934 $'

  def __init__(self, forceApply=False):
    super(MissingLeadsReport, self).__init__(forceApply)

  def getApplicability(self, appseq):
    conn_mgr = appseq.getConnectionMgr()
    if not conn_mgr.getQuery("Q_PLS_Scoring_Incremental") and not conn_mgr.getQuery(
      "Q_Diagnostic_DownloadedUnscoredLeads") and not conn_mgr.getQuery("Q_Diagnostic_MissingDownloadedLeads"):
      return Applicability.cannotApplyPass
    return Applicability.canApply

  def apply(self, appseq):

    # Apply the obtained filters to the missing lead queries: Q_Diagnostic_DownloadedUnscoredLeads and Q_Diagnostic_MissingDownloadedLeads
    success = False
    conn_mgr = appseq.getConnectionMgr()
    type = appseq.getText('template_type')
    lgm = conn_mgr.getLoadGroupMgr()
    q_scoring = conn_mgr.getQuery('Q_PLS_Scoring_Incremental')
    q_unscored = conn_mgr.getQuery('Q_Diagnostic_DownloadedUnscoredLeads')
    q_missing = conn_mgr.getQuery('Q_Diagnostic_MissingDownloadedLeads')

    for filter in q_scoring.getFilters():
      if filter.definition() not in [i.definition() for i in q_unscored.getFilters()]:
        q_unscored.filters_.append(filter)
      else:
        pass
    conn_mgr.setSpec('Q_Diagnostic_DownloadedUnscoredLeads', q_unscored.SpecLatticeNamedElements())

    for filter in q_scoring.getFilters():
      if filter.definition() not in [i.definition() for i in q_missing.getFilters()]:
        q_missing.filters_.append(filter)
      else:
        pass
    conn_mgr.setSpec('Q_Diagnostic_MissingDownloadedLeadss', q_missing.SpecLatticeNamedElements())

    # Get the customized score/score-date fields from the the LG PushLeadsLastScoredToDestination and then replace the fields in the related specs and LGs
    customerId = appseq.getText('customer_id')
    scoreField = appseq.getText('score_field')
    scoreDateField = appseq.getText('score_date_field')
    sfdcLeadScoreField = appseq.getText('sfdc_lead_score_field')
    sfdcContactScoreField = appseq.getText('sfdc_contact_score_field')
    sfdcLeadScoreDateField = appseq.getText('sfdc_lead_score_date_field')
    sfdcContactScoreDateField = appseq.getText('sfdc_contact_score_date_field')

    # Modified score/score-date fields of specs Templete_LeadRecord_Validation
    if type == 'MKTO':
      specs = 'SpecLatticeNamedElements((' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeImportTable(' \
              '		SpecSourceAggregationRuleMostRecent(SpecTotalOrderEffectiveDate),' \
              '		SpecColumnBindings(' \
              '			(' \
              '				SpecColumnBinding(' \
              '					ContainerElementName("Id"),' \
              '					DataTypeInt' \
              '				),' \
              '				SpecColumnBinding(' \
              '					ContainerElementName("__SCORE_DATE_FIELD__"),' \
              '					DataTypeDateTime' \
              '				),' \
              '				SpecColumnBinding(' \
              '					ContainerElementName("__SCORE_FIELD__"),' \
              '					DataTypeInt' \
              '				)' \
              '			)' \
              '		),' \
              '		SpecSourceFilterNone' \
              '	),' \
              '	ContainerElementName("MKTO_LeadRecord_Validation_Import")' \
              '),' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeBinder(' \
              '		SpecBoundName(' \
              '			ContainerElementName("MKTO_LeadRecord_Validation_Import"),' \
              '			NameTypeImportTable' \
              '		),' \
              '		SpecBoundName(' \
              '			ContainerElementName("MKTO_LeadRecord_Validation"),' \
              '			NameTypeSourceTable' \
              '		)' \
              '	),' \
              '	ContainerElementName("Binder_I2S_T_MKTO_LeadRecord_Validation_Import_MKTO_LeadRecord_Validation")' \
              '),' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeSourceTable(' \
              '		SpecLatticeSourceTableColumnSet(' \
              '			(' \
              '				SpecLatticeSourceTableColumn(' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementNameTableQualifiedName(' \
              '							LatticeSourceTableIdentifier(' \
              '								ContainerElementName("MKTO_LeadRecord_Validation")' \
              '							),' \
              '							ContainerElementName("Id")' \
              '						)' \
              '					),' \
              '					DataTypeLong,' \
              '					SpecFieldTypeAttribute(' \
              '						LatticeAddressAtomicIdentifier(' \
              '							ContainerElementName("MKTO_LeadRecord_ID")' \
              '						),' \
              '						SpecConstructiveIsConstructive,' \
              '						FunctionAggregationOperator("Sum")' \
              '					),' \
              '					SpecColumnContentContainerElementName(' \
              '						ContainerElementName("Id")' \
              '					),' \
              '					SpecEndpointTypeNone,' \
              '					SpecDefaultValueNull,' \
              '					SpecKeyAggregation(' \
              '						SpecColumnAggregationRuleFunction("Sum")' \
              '					),' \
              '					SpecEquivalenceAggregation(' \
              '						SpecColumnAggregationRuleFunction("Sum")' \
              '					)' \
              '				),' \
              '				SpecLatticeSourceTableColumn(' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementNameTableQualifiedName(' \
              '							LatticeSourceTableIdentifier(' \
              '								ContainerElementName("MKTO_LeadRecord_Validation")' \
              '							),' \
              '							ContainerElementName("__SCORE_DATE_FIELD__")' \
              '						)' \
              '					),' \
              '					DataTypeDateTime,' \
              '					SpecFieldTypeMetric(' \
              '						FunctionAggregationOperator("Max")' \
              '					),' \
              '					SpecColumnContentContainerElementName(' \
              '						ContainerElementName("__SCORE_DATE_FIELD__")' \
              '					),' \
              '					SpecEndpointTypeNone,' \
              '					SpecDefaultValueNull,' \
              '					SpecKeyAggregation(' \
              '						SpecColumnAggregationRuleFunction("Max")' \
              '					),' \
              '					SpecEquivalenceAggregation(' \
              '						SpecColumnAggregationRuleFunction("Max")' \
              '					)' \
              '				),' \
              '				SpecLatticeSourceTableColumn(' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementNameTableQualifiedName(' \
              '							LatticeSourceTableIdentifier(' \
              '								ContainerElementName("MKTO_LeadRecord_Validation")' \
              '							),' \
              '							ContainerElementName("__SCORE_FIELD__")' \
              '						)' \
              '					),' \
              '					DataTypeInt,' \
              '					SpecFieldTypeMetric(' \
              '						FunctionAggregationOperator("Sum")' \
              '					),' \
              '					SpecColumnContentContainerElementName(' \
              '						ContainerElementName("__SCORE_FIELD__")' \
              '					),' \
              '					SpecEndpointTypeNone,' \
              '					SpecDefaultValueNull,' \
              '					SpecKeyAggregation(' \
              '						SpecColumnAggregationRuleFunction("Sum")' \
              '					),' \
              '					SpecEquivalenceAggregation(' \
              '						SpecColumnAggregationRuleFunction("Sum")' \
              '					)' \
              '				)' \
              '			)' \
              '		),' \
              '		SpecKeys(empty),' \
              '		SpecDescription(""),' \
              '		SpecMaximalIsMaximal,' \
              '		SpecKeyAggregation(SpecColumnAggregationRuleMostRecent),' \
              '		SpecEquivalenceAggregation(SpecColumnAggregationRuleMostRecent)' \
              '	),' \
              '	ContainerElementName("MKTO_LeadRecord_Validation")' \
              ')))'
      specs = specs.replace('__SCORE_FIELD__', scoreField)
      specs = specs.replace('__SCORE_DATE_FIELD__', scoreDateField)
      conn_mgr.setSpec('MKTO_LeadRecord_Validation', specs)

    elif type == 'ELQ':
      specs = 'SpecLatticeNamedElements((' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeImportTable(' \
              '		SpecSourceAggregationRuleMostRecent(SpecTotalOrderEffectiveDate),' \
              '		SpecColumnBindings(' \
              '			(' \
              '				SpecColumnBinding(' \
              '					ContainerElementName("C_DateCreated"),' \
              '					DataTypeDateTime' \
              '				),' \
              '				SpecColumnBinding(' \
              '					ContainerElementName("C_DateModified"),' \
              '					DataTypeDateTime' \
              '				),' \
              '				SpecColumnBinding(' \
              '					ContainerElementName("__SCORE_DATE_FIELD__"),' \
              '					DataTypeDateTime' \
              '				),' \
              '				SpecColumnBinding(' \
              '					ContainerElementName("__SCORE_FIELD__"),' \
              '					DataTypeDouble' \
              '				),' \
              '				SpecColumnBinding(' \
              '					ContainerElementName("ContactID"),' \
              '					DataTypeInt' \
              '				)' \
              '			)' \
              '		),' \
              '		SpecSourceFilterNone' \
              '	),' \
              '	ContainerElementName("ELQ_Contact_Validation_Import")' \
              '),' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeBinder(' \
              '		SpecBoundName(' \
              '			ContainerElementName("ELQ_Contact_Validation_Import"),' \
              '			NameTypeImportTable' \
              '		),' \
              '		SpecBoundName(' \
              '			ContainerElementName("ELQ_Contact_Validation"),' \
              '			NameTypeSourceTable' \
              '		)' \
              '	),' \
              '	ContainerElementName("Binder_I2S_T_ELQ_Contact_Validation_Import_ELQ_Contact_Validation")' \
              '),' \
              'SpecLatticeNamedElement(' \
              '	SpecLatticeSourceTable(' \
              '		SpecLatticeSourceTableColumnSet(' \
              '			(' \
              '				SpecLatticeSourceTableColumn(' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementNameTableQualifiedName(' \
              '							LatticeSourceTableIdentifier(' \
              '								ContainerElementName("ELQ_Contact_Validation")' \
              '							),' \
              '							ContainerElementName("C_DateCreated")' \
              '						)' \
              '					),' \
              '					DataTypeDateTime,' \
              '					SpecFieldTypeMetric(' \
              '						FunctionAggregationOperator("Max")' \
              '					),' \
              '					SpecColumnContentContainerElementName(' \
              '						ContainerElementName("C_DateCreated")' \
              '					),' \
              '					SpecEndpointTypeNone,' \
              '					SpecDefaultValueNull,' \
              '					SpecKeyAggregation(' \
              '						SpecColumnAggregationRuleFunction("Max")' \
              '					),' \
              '					SpecEquivalenceAggregation(' \
              '						SpecColumnAggregationRuleFunction("Max")' \
              '					)' \
              '				),' \
              '				SpecLatticeSourceTableColumn(' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementNameTableQualifiedName(' \
              '							LatticeSourceTableIdentifier(' \
              '								ContainerElementName("ELQ_Contact_Validation")' \
              '							),' \
              '							ContainerElementName("C_DateModified")' \
              '						)' \
              '					),' \
              '					DataTypeDateTime,' \
              '					SpecFieldTypeMetric(' \
              '						FunctionAggregationOperator("Max")' \
              '					),' \
              '					SpecColumnContentContainerElementName(' \
              '						ContainerElementName("C_DateModified")' \
              '					),' \
              '					SpecEndpointTypeNone,' \
              '					SpecDefaultValueNull,' \
              '					SpecKeyAggregation(' \
              '						SpecColumnAggregationRuleFunction("Max")' \
              '					),' \
              '					SpecEquivalenceAggregation(' \
              '						SpecColumnAggregationRuleFunction("Max")' \
              '					)' \
              '				),' \
              '				SpecLatticeSourceTableColumn(' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementNameTableQualifiedName(' \
              '							LatticeSourceTableIdentifier(' \
              '								ContainerElementName("ELQ_Contact_Validation")' \
              '							),' \
              '							ContainerElementName("__SCORE_FIELD__")' \
              '						)' \
              '					),' \
              '					DataTypeDouble,' \
              '					SpecFieldTypeMetric(' \
              '						FunctionAggregationOperator("Sum")' \
              '					),' \
              '					SpecColumnContentContainerElementName(' \
              '						ContainerElementName("__SCORE_FIELD__")' \
              '					),' \
              '					SpecEndpointTypeNone,' \
              '					SpecDefaultValueNull,' \
              '					SpecKeyAggregation(' \
              '						SpecColumnAggregationRuleFunction("Sum")' \
              '					),' \
              '					SpecEquivalenceAggregation(' \
              '						SpecColumnAggregationRuleFunction("Sum")' \
              '					)' \
              '				),' \
              '				SpecLatticeSourceTableColumn(' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementNameTableQualifiedName(' \
              '							LatticeSourceTableIdentifier(' \
              '								ContainerElementName("ELQ_Contact_Validation")' \
              '							),' \
              '							ContainerElementName("__SCORE_DATE_FIELD__")' \
              '						)' \
              '					),' \
              '					DataTypeDateTime,' \
              '					SpecFieldTypeMetric(' \
              '						FunctionAggregationOperator("Max")' \
              '					),' \
              '					SpecColumnContentContainerElementName(' \
              '						ContainerElementName("__SCORE_DATE_FIELD__")' \
              '					),' \
              '					SpecEndpointTypeNone,' \
              '					SpecDefaultValueNull,' \
              '					SpecKeyAggregation(' \
              '						SpecColumnAggregationRuleFunction("Max")' \
              '					),' \
              '					SpecEquivalenceAggregation(' \
              '						SpecColumnAggregationRuleFunction("Max")' \
              '					)' \
              '				),' \
              '				SpecLatticeSourceTableColumn(' \
              '					LatticeFunctionIdentifier(' \
              '						ContainerElementNameTableQualifiedName(' \
              '							LatticeSourceTableIdentifier(' \
              '								ContainerElementName("ELQ_Contact_Validation")' \
              '							),' \
              '							ContainerElementName("ContactID")' \
              '						)' \
              '					),' \
              '					DataTypeVarChar(50),' \
              '					SpecFieldTypeAttribute(' \
              '						LatticeAddressAtomicIdentifier(' \
              '							ContainerElementName("ELQ_Contact_ContactID")' \
              '						),' \
              '						SpecConstructiveIsConstructive,' \
              '						FunctionAggregationOperator("Combine")' \
              '					),' \
              '					SpecColumnContentContainerElementName(' \
              '						ContainerElementName("ContactID")' \
              '					),' \
              '					SpecEndpointTypeNone,' \
              '					SpecDefaultValueNullNoRTrim,' \
              '					SpecKeyAggregation(' \
              '						SpecColumnAggregationRuleFunction("Combine")' \
              '					),' \
              '					SpecEquivalenceAggregation(' \
              '						SpecColumnAggregationRuleFunction("Combine")' \
              '					)' \
              '				)' \
              '			)' \
              '		),' \
              '		SpecKeys(empty),' \
              '		SpecDescription(""),' \
              '		SpecMaximalIsMaximal,' \
              '		SpecKeyAggregation(SpecColumnAggregationRuleMostRecent),' \
              '		SpecEquivalenceAggregation(SpecColumnAggregationRuleMostRecent)' \
              '	),' \
              '	ContainerElementName("ELQ_Contact_Validation")' \
              ')))'
      specs = specs.replace('__SCORE_FIELD__', scoreField)
      specs = specs.replace('__SCORE_DATE_FIELD__', scoreDateField)
      conn_mgr.setSpec('ELQ_Contact_Validation', specs)
    else:
      specs_lead = 'SpecLatticeNamedElements((' \
                   'SpecLatticeNamedElement(' \
                   '	SpecLatticeImportTable(' \
                   '		SpecSourceAggregationRuleMostRecent(SpecTotalOrderEffectiveDate),' \
                   '		SpecColumnBindings(' \
                   '			(' \
                   '				SpecColumnBinding(' \
                   '					ContainerElementName("CreatedDate"),' \
                   '					DataTypeDateTime' \
                   '				),' \
                   '				SpecColumnBinding(' \
                   '					ContainerElementName("Id"),' \
                   '					DataTypeNVarChar(36)' \
                   '				),' \
                   '				SpecColumnBinding(' \
                   '					ContainerElementName("LastModifiedDate"),' \
                   '					DataTypeDateTime' \
                   '				),' \
                   '				SpecColumnBinding(' \
                   '					ContainerElementName("__LEAD_SCORE_DATE_FIELD__"),' \
                   '					DataTypeDateTime' \
                   '				),' \
                   '				SpecColumnBinding(' \
                   '					ContainerElementName("__LEAD_SCORE_FIELD__"),' \
                   '					DataTypeInt' \
                   '				)' \
                   '			)' \
                   '		),' \
                   '		SpecSourceFilterNone' \
                   '	),' \
                   '	ContainerElementName("SFDC_Lead_Validation_Import")' \
                   '),' \
                   'SpecLatticeNamedElement(' \
                   '	SpecLatticeBinder(' \
                   '		SpecBoundName(' \
                   '			ContainerElementName("SFDC_Lead_Validation_Import"),' \
                   '			NameTypeImportTable' \
                   '		),' \
                   '		SpecBoundName(' \
                   '			ContainerElementName("SFDC_Lead_Validation"),' \
                   '			NameTypeSourceTable' \
                   '		)' \
                   '	),' \
                   '	ContainerElementName("Binder_I2S_T_SFDC_Lead_Validation_Import_SFDC_Lead_Validation")' \
                   '),' \
                   'SpecLatticeNamedElement(' \
                   '	SpecLatticeSourceTable(' \
                   '		SpecLatticeSourceTableColumnSet(' \
                   '			(' \
                   '				SpecLatticeSourceTableColumn(' \
                   '					LatticeFunctionIdentifier(' \
                   '						ContainerElementNameTableQualifiedName(' \
                   '							LatticeSourceTableIdentifier(' \
                   '								ContainerElementName("SFDC_Lead_Validation")' \
                   '							),' \
                   '							ContainerElementName("Id")' \
                   '						)' \
                   '					),' \
                   '					DataTypeVarChar(50),' \
                   '					SpecFieldTypeAttribute(' \
                   '						LatticeAddressAtomicIdentifier(' \
                   '							ContainerElementName("SFDC_Lead_Contact_ID")' \
                   '						),' \
                   '						SpecConstructiveIsConstructive,' \
                   '						FunctionAggregationOperator("Combine")' \
                   '					),' \
                   '					SpecColumnContentContainerElementName(' \
                   '						ContainerElementName("Id")' \
                   '					),' \
                   '					SpecEndpointTypeNone,' \
                   '					SpecDefaultValueNullNoRTrim,' \
                   '					SpecKeyAggregation(' \
                   '						SpecColumnAggregationRuleFunction("Combine")' \
                   '					),' \
                   '					SpecEquivalenceAggregation(' \
                   '						SpecColumnAggregationRuleFunction("Combine")' \
                   '					)' \
                   '				),' \
                   '				SpecLatticeSourceTableColumn(' \
                   '					LatticeFunctionIdentifier(' \
                   '						ContainerElementNameTableQualifiedName(' \
                   '							LatticeSourceTableIdentifier(' \
                   '								ContainerElementName("SFDC_Lead_Validation")' \
                   '							),' \
                   '							ContainerElementName("CreatedDate")' \
                   '						)' \
                   '					),' \
                   '					DataTypeDateTime,' \
                   '					SpecFieldTypeMetric(' \
                   '						FunctionAggregationOperator("Max")' \
                   '					),' \
                   '					SpecColumnContentContainerElementName(' \
                   '						ContainerElementName("CreatedDate")' \
                   '					),' \
                   '					SpecEndpointTypeNone,' \
                   '					SpecDefaultValueNull,' \
                   '					SpecKeyAggregation(' \
                   '						SpecColumnAggregationRuleFunction("Max")' \
                   '					),' \
                   '					SpecEquivalenceAggregation(' \
                   '						SpecColumnAggregationRuleFunction("Max")' \
                   '					)' \
                   '				),' \
                   '				SpecLatticeSourceTableColumn(' \
                   '					LatticeFunctionIdentifier(' \
                   '						ContainerElementNameTableQualifiedName(' \
                   '							LatticeSourceTableIdentifier(' \
                   '								ContainerElementName("SFDC_Lead_Validation")' \
                   '							),' \
                   '							ContainerElementName("LastModifiedDate")' \
                   '						)' \
                   '					),' \
                   '					DataTypeDateTime,' \
                   '					SpecFieldTypeMetric(' \
                   '						FunctionAggregationOperator("Max")' \
                   '					),' \
                   '					SpecColumnContentContainerElementName(' \
                   '						ContainerElementName("LastModifiedDate")' \
                   '					),' \
                   '					SpecEndpointTypeNone,' \
                   '					SpecDefaultValueNull,' \
                   '					SpecKeyAggregation(' \
                   '						SpecColumnAggregationRuleFunction("Max")' \
                   '					),' \
                   '					SpecEquivalenceAggregation(' \
                   '						SpecColumnAggregationRuleFunction("Max")' \
                   '					)' \
                   '				),' \
                   '				SpecLatticeSourceTableColumn(' \
                   '					LatticeFunctionIdentifier(' \
                   '						ContainerElementNameTableQualifiedName(' \
                   '							LatticeSourceTableIdentifier(' \
                   '								ContainerElementName("SFDC_Lead_Validation")' \
                   '							),' \
                   '							ContainerElementName("__LEAD_SCORE_FIELD__")' \
                   '						)' \
                   '					),' \
                   '					DataTypeInt,' \
                   '					SpecFieldTypeMetric(' \
                   '						FunctionAggregationOperator("Sum")' \
                   '					),' \
                   '					SpecColumnContentContainerElementName(' \
                   '						ContainerElementName("__LEAD_SCORE_FIELD__")' \
                   '					),' \
                   '					SpecEndpointTypeNone,' \
                   '					SpecDefaultValueNull,' \
                   '					SpecKeyAggregation(' \
                   '						SpecColumnAggregationRuleFunction("Sum")' \
                   '					),' \
                   '					SpecEquivalenceAggregation(' \
                   '						SpecColumnAggregationRuleFunction("Sum")' \
                   '					)' \
                   '				),' \
                   '				SpecLatticeSourceTableColumn(' \
                   '					LatticeFunctionIdentifier(' \
                   '						ContainerElementNameTableQualifiedName(' \
                   '							LatticeSourceTableIdentifier(' \
                   '								ContainerElementName("SFDC_Lead_Validation")' \
                   '							),' \
                   '							ContainerElementName("__LEAD_SCORE_DATE_FIELD__")' \
                   '						)' \
                   '					),' \
                   '					DataTypeDateTime,' \
                   '					SpecFieldTypeMetric(' \
                   '						FunctionAggregationOperator("Max")' \
                   '					),' \
                   '					SpecColumnContentContainerElementName(' \
                   '						ContainerElementName("__LEAD_SCORE_DATE_FIELD__")' \
                   '					),' \
                   '					SpecEndpointTypeNone,' \
                   '					SpecDefaultValueNull,' \
                   '					SpecKeyAggregation(' \
                   '						SpecColumnAggregationRuleFunction("Max")' \
                   '					),' \
                   '					SpecEquivalenceAggregation(' \
                   '						SpecColumnAggregationRuleFunction("Max")' \
                   '					)' \
                   '				)' \
                   '			)' \
                   '		),' \
                   '		SpecKeys(empty),' \
                   '		SpecDescription(""),' \
                   '		SpecMaximalIsMaximal,' \
                   '		SpecKeyAggregation(SpecColumnAggregationRuleMostRecent),' \
                   '		SpecEquivalenceAggregation(SpecColumnAggregationRuleMostRecent)' \
                   '	),' \
                   '	ContainerElementName("SFDC_Lead_Validation")' \
                   ')))'
      specs_lead = specs_lead.replace('__LEAD_SCORE_FIELD__', sfdcLeadScoreField)
      specs_lead = specs_lead.replace('__LEAD_SCORE_DATE_FIELD__', sfdcLeadScoreDateField)
      conn_mgr.setSpec('SFDC_Lead_Validation', specs_lead)

      specs_contact = 'SpecLatticeNamedElements((' \
                      'SpecLatticeNamedElement(' \
                      '	SpecLatticeImportTable(' \
                      '		SpecSourceAggregationRuleMostRecent(SpecTotalOrderEffectiveDate),' \
                      '		SpecColumnBindings(' \
                      '			(' \
                      '				SpecColumnBinding(' \
                      '					ContainerElementName("CreatedDate"),' \
                      '					DataTypeDateTime' \
                      '				),' \
                      '				SpecColumnBinding(' \
                      '					ContainerElementName("Id"),' \
                      '					DataTypeNVarChar(36)' \
                      '				),' \
                      '				SpecColumnBinding(' \
                      '					ContainerElementName("LastModifiedDate"),' \
                      '					DataTypeDateTime' \
                      '				),' \
                      '				SpecColumnBinding(' \
                      '					ContainerElementName("__CONTACT_SCORE_DATE_FIELD__"),' \
                      '					DataTypeDateTime' \
                      '				),' \
                      '				SpecColumnBinding(' \
                      '					ContainerElementName("__CONTACT_SCORE_FIELD__"),' \
                      '					DataTypeInt' \
                      '				)' \
                      '			)' \
                      '		),' \
                      '		SpecSourceFilterNone' \
                      '	),' \
                      '	ContainerElementName("SFDC_Contact_Validation_Import")' \
                      '),' \
                      'SpecLatticeNamedElement(' \
                      '	SpecLatticeBinder(' \
                      '		SpecBoundName(' \
                      '			ContainerElementName("SFDC_Contact_Validation_Import"),' \
                      '			NameTypeImportTable' \
                      '		),' \
                      '		SpecBoundName(' \
                      '			ContainerElementName("SFDC_Contact_Validation"),' \
                      '			NameTypeSourceTable' \
                      '		)' \
                      '	),' \
                      '	ContainerElementName("Binder_I2S_T_SFDC_Contact_Validation_Import_SFDC_Contact_Validation")' \
                      '),' \
                      'SpecLatticeNamedElement(' \
                      '	SpecLatticeSourceTable(' \
                      '		SpecLatticeSourceTableColumnSet(' \
                      '			(' \
                      '				SpecLatticeSourceTableColumn(' \
                      '					LatticeFunctionIdentifier(' \
                      '						ContainerElementNameTableQualifiedName(' \
                      '							LatticeSourceTableIdentifier(' \
                      '								ContainerElementName("SFDC_Contact_Validation")' \
                      '							),' \
                      '							ContainerElementName("Id")' \
                      '						)' \
                      '					),' \
                      '					DataTypeVarChar(50),' \
                      '					SpecFieldTypeAttribute(' \
                      '						LatticeAddressAtomicIdentifier(' \
                      '							ContainerElementName("SFDC_Lead_Contact_ID")' \
                      '						),' \
                      '						SpecConstructiveIsConstructive,' \
                      '						FunctionAggregationOperator("Combine")' \
                      '					),' \
                      '					SpecColumnContentContainerElementName(' \
                      '						ContainerElementName("Id")' \
                      '					),' \
                      '					SpecEndpointTypeNone,' \
                      '					SpecDefaultValueNullNoRTrim,' \
                      '					SpecKeyAggregation(' \
                      '						SpecColumnAggregationRuleFunction("Combine")' \
                      '					),' \
                      '					SpecEquivalenceAggregation(' \
                      '						SpecColumnAggregationRuleFunction("Combine")' \
                      '					)' \
                      '				),' \
                      '				SpecLatticeSourceTableColumn(' \
                      '					LatticeFunctionIdentifier(' \
                      '						ContainerElementNameTableQualifiedName(' \
                      '							LatticeSourceTableIdentifier(' \
                      '								ContainerElementName("SFDC_Contact_Validation")' \
                      '							),' \
                      '							ContainerElementName("CreatedDate")' \
                      '						)' \
                      '					),' \
                      '					DataTypeDateTime,' \
                      '					SpecFieldTypeMetric(' \
                      '						FunctionAggregationOperator("Max")' \
                      '					),' \
                      '					SpecColumnContentContainerElementName(' \
                      '						ContainerElementName("CreatedDate")' \
                      '					),' \
                      '					SpecEndpointTypeNone,' \
                      '					SpecDefaultValueNull,' \
                      '					SpecKeyAggregation(' \
                      '						SpecColumnAggregationRuleFunction("Max")' \
                      '					),' \
                      '					SpecEquivalenceAggregation(' \
                      '						SpecColumnAggregationRuleFunction("Max")' \
                      '					)' \
                      '				),' \
                      '				SpecLatticeSourceTableColumn(' \
                      '					LatticeFunctionIdentifier(' \
                      '						ContainerElementNameTableQualifiedName(' \
                      '							LatticeSourceTableIdentifier(' \
                      '								ContainerElementName("SFDC_Contact_Validation")' \
                      '							),' \
                      '							ContainerElementName("LastModifiedDate")' \
                      '						)' \
                      '					),' \
                      '					DataTypeDateTime,' \
                      '					SpecFieldTypeMetric(' \
                      '						FunctionAggregationOperator("Max")' \
                      '					),' \
                      '					SpecColumnContentContainerElementName(' \
                      '						ContainerElementName("LastModifiedDate")' \
                      '					),' \
                      '					SpecEndpointTypeNone,' \
                      '					SpecDefaultValueNull,' \
                      '					SpecKeyAggregation(' \
                      '						SpecColumnAggregationRuleFunction("Max")' \
                      '					),' \
                      '					SpecEquivalenceAggregation(' \
                      '						SpecColumnAggregationRuleFunction("Max")' \
                      '					)' \
                      '				),' \
                      '				SpecLatticeSourceTableColumn(' \
                      '					LatticeFunctionIdentifier(' \
                      '						ContainerElementNameTableQualifiedName(' \
                      '							LatticeSourceTableIdentifier(' \
                      '								ContainerElementName("SFDC_Contact_Validation")' \
                      '							),' \
                      '							ContainerElementName("__CONTACT_SCORE_FIELD__")' \
                      '						)' \
                      '					),' \
                      '					DataTypeInt,' \
                      '					SpecFieldTypeMetric,' \
                      '					SpecColumnContentContainerElementName(' \
                      '						ContainerElementName("")' \
                      '					),' \
                      '					SpecEndpointTypeNone,' \
                      '					SpecDefaultValue(""),' \
                      '					SpecKeyAggregation(SpecColumnAggregationRuleDefault),' \
                      '					SpecEquivalenceAggregation(SpecColumnAggregationRuleDefault)' \
                      '				),' \
                      '				SpecLatticeSourceTableColumn(' \
                      '					LatticeFunctionIdentifier(' \
                      '						ContainerElementNameTableQualifiedName(' \
                      '							LatticeSourceTableIdentifier(' \
                      '								ContainerElementName("SFDC_Contact_Validation")' \
                      '							),' \
                      '							ContainerElementName("__CONTACT_SCORE_DATE_FIELD__")' \
                      '						)' \
                      '					),' \
                      '					DataTypeDateTime,' \
                      '					SpecFieldTypeMetric,' \
                      '					SpecColumnContentContainerElementName(' \
                      '						ContainerElementName("")' \
                      '					),' \
                      '					SpecEndpointTypeNone,' \
                      '					SpecDefaultValue(""),' \
                      '					SpecKeyAggregation(SpecColumnAggregationRuleDefault),' \
                      '					SpecEquivalenceAggregation(SpecColumnAggregationRuleDefault)' \
                      '				)' \
                      '			)' \
                      '		),' \
                      '		SpecKeys(empty),' \
                      '		SpecDescription(""),' \
                      '		SpecMaximalIsMaximal,' \
                      '		SpecKeyAggregation(SpecColumnAggregationRuleMostRecent),' \
                      '		SpecEquivalenceAggregation(SpecColumnAggregationRuleMostRecent)' \
                      '	),' \
                      '	ContainerElementName("SFDC_Contact_Validation")' \
                      ')))'
      specs_contact = specs_contact.replace('__CONTACT_SCORE_FIELD__', sfdcContactScoreField)
      specs_contact = specs_contact.replace('__CONTACT_SCORE_DATE_FIELD__', sfdcContactScoreDateField)
      conn_mgr.setSpec('SFDC_Contact_Validation', specs_contact)

    # Modified score/score-date fields of specs Diagnostic_Alias_Score & Diagnostic_Alias_ScoreDate
    if type == 'MKTO':
      specs_Alias_Score = 'SpecLatticeNamedElements((' \
                          'SpecLatticeNamedElement(' \
                          '	SpecLatticeFunction(' \
                          '		LatticeFunctionIdentifier(' \
                          '			ContainerElementNameTableQualifiedName(' \
                          '				LatticeSourceTableIdentifier(' \
                          '					ContainerElementName("MKTO_LeadRecord_Validation")' \
                          '				),' \
                          '				ContainerElementName("__SCORE_FIELD__")' \
                          '			)' \
                          '		),' \
                          '		DataTypeUnknown,' \
                          '		SpecFunctionTypeMetric,' \
                          '		SpecFunctionSourceTypeCalculation,' \
                          '		SpecDefaultValueNull,' \
                          '		SpecDescription("")' \
                          '	),' \
                          '	ContainerElementName("Diagnostic_Alias_Score")' \
                          ')))'
      specs_Alias_Score = specs_Alias_Score.replace('__SCORE_FIELD__', scoreField)
      conn_mgr.setSpec('Diagnostic_Alias_Score', specs_Alias_Score)

      specs_Alias_Date = 'SpecLatticeNamedElements((' \
                         'SpecLatticeNamedElement(' \
                         '	SpecLatticeFunction(' \
                         '		LatticeFunctionIdentifier(' \
                         '			ContainerElementNameTableQualifiedName(' \
                         '				LatticeSourceTableIdentifier(' \
                         '					ContainerElementName("MKTO_LeadRecord_Validation")' \
                         '				),' \
                         '				ContainerElementName("__SCORE_DATE_FIELD__")' \
                         '			)' \
                         '		),' \
                         '		DataTypeUnknown,' \
                         '		SpecFunctionTypeMetric,' \
                         '		SpecFunctionSourceTypeCalculation,' \
                         '		SpecDefaultValueNull,' \
                         '		SpecDescription("")' \
                         '	),' \
                         '	ContainerElementName("Diagnostic_Alias_ScoreDate")' \
                         ')))'
      specs_Alias_Date = specs_Alias_Date.replace('__SCORE_DATE_FIELD__', scoreDateField)
      conn_mgr.setSpec('Diagnostic_Alias_ScoreDate', specs_Alias_Date)
    elif type == 'ELQ':
      specs_Alias_Score = 'SpecLatticeNamedElements((' \
                          'SpecLatticeNamedElement(' \
                          '	SpecLatticeFunction(' \
                          '		LatticeFunctionIdentifier(' \
                          '			ContainerElementNameTableQualifiedName(' \
                          '				LatticeSourceTableIdentifier(' \
                          '					ContainerElementName("ELQ_Contact_Validation")' \
                          '				),' \
                          '				ContainerElementName("__SCORE_FIELD__")' \
                          '			)' \
                          '		),' \
                          '		DataTypeUnknown,' \
                          '		SpecFunctionTypeMetric,' \
                          '		SpecFunctionSourceTypeCalculation,' \
                          '		SpecDefaultValueNull,' \
                          '		SpecDescription("")' \
                          '	),' \
                          '	ContainerElementName("Diagnostic_Alias_Score")' \
                          ')))'
      specs_Alias_Score = specs_Alias_Score.replace('__SCORE_FIELD__', scoreField)
      conn_mgr.setSpec('Diagnostic_Alias_Score', specs_Alias_Score)

      specs_Alias_Date = 'SpecLatticeNamedElements((' \
                         'SpecLatticeNamedElement(' \
                         '	SpecLatticeFunction(' \
                         '		LatticeFunctionIdentifier(' \
                         '			ContainerElementNameTableQualifiedName(' \
                         '				LatticeSourceTableIdentifier(' \
                         '					ContainerElementName("ELQ_Contact_Validation")' \
                         '				),' \
                         '				ContainerElementName("__SCORE_DATE_FIELD__")' \
                         '			)' \
                         '		),' \
                         '		DataTypeUnknown,' \
                         '		SpecFunctionTypeMetric,' \
                         '		SpecFunctionSourceTypeCalculation,' \
                         '		SpecDefaultValueNull,' \
                         '		SpecDescription("")' \
                         '	),' \
                         '	ContainerElementName("Diagnostic_Alias_ScoreDate")' \
                         ')))'
      specs_Alias_Date = specs_Alias_Date.replace('__SCORE_DATE_FIELD__', scoreDateField)
      conn_mgr.setSpec('Diagnostic_Alias_ScoreDate', specs_Alias_Date)

    else:
      specs_Alias_Score = 'SpecLatticeNamedElements((' \
                          'SpecLatticeNamedElement(' \
                          '	SpecLatticeFunction(' \
                          '		LatticeFunctionExpressionTransform(' \
                          '			LatticeFunctionExpression(' \
                          '				LatticeFunctionOperatorIdentifier("IsNull"),' \
                          '				LatticeFunctionIdentifier(' \
                          '					ContainerElementNameTableQualifiedName(' \
                          '						LatticeSourceTableIdentifier(' \
                          '							ContainerElementName("SFDC_Contact_Validation")' \
                          '						),' \
                          '						ContainerElementName("__CONTACT_SCORE_FIELD__")' \
                          '					)' \
                          '				),' \
                          '				LatticeFunctionIdentifier(' \
                          '					ContainerElementNameTableQualifiedName(' \
                          '						LatticeSourceTableIdentifier(' \
                          '							ContainerElementName("SFDC_Lead_Validation")' \
                          '						),' \
                          '						ContainerElementName("__LEAD_SCORE_FIELD__")' \
                          '					)' \
                          '				)' \
                          '			),' \
                          '			LatticeAddressSetPi(' \
                          '				LatticeAddressExpressionAtomic(' \
                          '					LatticeAddressAtomicIdentifier(' \
                          '						ContainerElementName("SFDC_Lead_Contact_ID")' \
                          '					)' \
                          '				)' \
                          '			),' \
                          '			FunctionAggregationOperator("None")' \
                          '		),' \
                          '		DataTypeUnknown,' \
                          '		SpecFunctionTypeMetric,' \
                          '		SpecFunctionSourceTypeCalculation,' \
                          '		SpecDefaultValueNull,' \
                          '		SpecDescription("")' \
                          '	),' \
                          '	ContainerElementName("Diagnostic_Alias_Score")' \
                          ')))'
      specs_Alias_Score = specs_Alias_Score.replace('__CONTACT_SCORE_FIELD__', sfdcContactScoreField)
      specs_Alias_Score = specs_Alias_Score.replace('__LEAD_SCORE_FIELD__', sfdcLeadScoreField)
      conn_mgr.setSpec('Diagnostic_Alias_Score', specs_Alias_Score)

      specs_Alias_Date = 'SpecLatticeNamedElements((' \
                         'SpecLatticeNamedElement(' \
                         '	SpecLatticeFunction(' \
                         '		LatticeFunctionExpressionTransform(' \
                         '			LatticeFunctionExpression(' \
                         '				LatticeFunctionOperatorIdentifier("IsNull"),' \
                         '				LatticeFunctionIdentifier(' \
                         '					ContainerElementNameTableQualifiedName(' \
                         '						LatticeSourceTableIdentifier(' \
                         '							ContainerElementName("SFDC_Contact_Validation")' \
                         '						),' \
                         '						ContainerElementName("__CONTACT_SCORE_DATE_FIELD__")' \
                         '					)' \
                         '				),' \
                         '				LatticeFunctionIdentifier(' \
                         '					ContainerElementNameTableQualifiedName(' \
                         '						LatticeSourceTableIdentifier(' \
                         '							ContainerElementName("SFDC_Lead_Validation")' \
                         '						),' \
                         '						ContainerElementName("__LEAD_SCORE_DATE_FIELD__")' \
                         '					)' \
                         '				)' \
                         '			),' \
                         '			LatticeAddressSetPi(' \
                         '				LatticeAddressExpressionAtomic(' \
                         '					LatticeAddressAtomicIdentifier(' \
                         '						ContainerElementName("SFDC_Lead_Contact_ID")' \
                         '					)' \
                         '				)' \
                         '			),' \
                         '			FunctionAggregationOperator("None")' \
                         '		),' \
                         '		DataTypeUnknown,' \
                         '		SpecFunctionTypeMetric,' \
                         '		SpecFunctionSourceTypeCalculation,' \
                         '		SpecDefaultValueNull,' \
                         '		SpecDescription("")' \
                         '	),' \
                         '	ContainerElementName("Diagnostic_Alias_ScoreDate")' \
                         ')))'
      specs_Alias_Date = specs_Alias_Date.replace('__CONTACT_SCORE_DATE_FIELD__', sfdcContactScoreDateField)
      specs_Alias_Date = specs_Alias_Date.replace('__LEAD_SCORE_DATE_FIELD__', sfdcLeadScoreDateField)
      conn_mgr.setSpec('Diagnostic_Alias_ScoreDate', specs_Alias_Date)

    # Modified score/score-date fields of Diagnostic_LoadLeads
    if type == 'MKTO':
      step1_xml = '''
      <group name="Diagnostic_LoadLeads" alias="Diagnostic_LoadLeads" w="Workspace" type="1" scheduleType="0" allowUserChangeScheduleType="False" visibleForEndUser="True" threshold="10000" launchExpiredDays="7" createdBy="richard.liu@lattice-engines.com" path="Diagnostic" autoGenerated="False" validationValidMinutes="120" autoClearOnFailure="False" mergeRulesSaved="True" ng="False">
        <schemas />
        <visiDBConfigurationWithMacros />
        <targetQueries />
        <targetQuerySequences />
        <rdss>
          <rds n="MKTO_ActivityRecord_Validation" w="Workspace" sn="MKTO_ActivityRecord_Validation" cn="Marketo_DataProvider" u="False" ss="" tn="ActivityRecord" nmo="1" f="@recordCOUNT(5000) AND&#xD;&#xA;activityDateTime &gt; #Diagnostic_LowerLimitTime AND activityDateTime &lt; #Diagnostic_UpperLimitTime AND&#xD;&#xA;activityType in ('New Lead','Click Link','Visit Webpage','Interesting Moment','Open Email','Email Bounced Soft','Fill Out Form','Unsubscribe Email','Click Email')" ad="False" em="False" td="False" ic="" dd="" l="1000" tw="False" sr="50000" htw="24" mtw="60" emt="False" acd="False" mgf="False" eo="2" emd="False" eo_sftp="1">
            <ts>
              <t n="Diagnostic_LowerLimitTime" t="1" qn="Q_Diagnostic_ExtractLeadsRange" cn="Diagnostic_LowerLimitTime" m="0">
                <schemas />
                <specs />
              </t>
              <t n="Diagnostic_UpperLimitTime" t="1" qn="Q_Diagnostic_ExtractLeadsRange" cn="Diagnostic_UpperLimitTime" m="0">
                <schemas />
                <specs />
              </t>
            </ts>
            <mcs>
              <mc cn="activityDateTime" />
              <mc cn="activityType" />
              <mc cn="campaign" />
              <mc cn="foreignSysId" />
              <mc cn="foreignSysOrgId" />
              <mc cn="id" />
              <mc cn="mktgAssetName" />
              <mc cn="mktPersonId" />
              <mc cn="orgName" />
              <mc cn="personName" />
            </mcs>
          </rds>
          <rds n="MKTO_LeadRecord_Validation" w="Workspace" sn="MKTO_LeadRecord_Validation" cn="Marketo_DataProvider" u="False" ss="" tn="LeadRecord" nmo="1" f="id in #IDs" ad="False" em="False" td="False" ic="" dd="" l="1000" tw="False" sr="50000" htw="24" mtw="60" emt="False" acd="False" mgf="False" eo="2" emd="False" eo_sftp="1">
            <ts>
              <t n="IDs" t="2" qn="Q_Diagnostic_GetLeadRecordID" cn="MKTO_LeadRecord_ID" m="100">
                <schemas />
                <specs />
              </t>
            </ts>
            <mcs>
              <mc cn="Id" />
              <mc cn="__SCORE_DATE_FIELD__" />
              <mc cn="__SCORE_FIELD__" />
            </mcs>
          </rds>
        </rdss>
        <validationExtracts />
        <ces />
        <extractQueries />
        <extractQuerySequences />
        <leafExtracts />
        <launchExtracts />
        <jobs />
        <pdmatches />
        <leadscroings />
        <lssbardins />
        <lssbardouts />
        <lds />
        <ecs />
        <gCs />
      </group>
      '''
    elif type == 'ELQ':
      step1_xml = '''
          <group name="Diagnostic_LoadLeads" alias="Diagnostic_LoadLeads" w="Workspace" type="1" scheduleType="0" allowUserChangeScheduleType="False" visibleForEndUser="True" threshold="10000" launchExpiredDays="7" createdBy="richard.liu@lattice-engines.com" path="Diagnostic" autoGenerated="False" validationValidMinutes="120" autoClearOnFailure="False" mergeRulesSaved="True" ng="False">
      <schemas />
      <visiDBConfigurationWithMacros />
      <targetQueries />
      <targetQuerySequences />
      <rdss>
        <rds n="ELQ_Contact_Validation" w="Workspace" sn="ELQ_Contact_Validation" cn="Eloqua_Bulk_DataProvider" u="False" ss="" tn="Contact" nmo="1" f="@recordCOUNT(1000) AND&#xD;&#xA;(C_DateModified &gt; #Diagnostic_LowerLimitTime AND C_DateModified &lt; #Diagnostic_UpperLimitTime AND (__SCORE_DATE_FIELD__ &lt; #Diagnostic_RescoreThreshold OR __SCORE_FIELD__ = 0 ))" ad="False" em="False" td="False" ic="" dd="" l="1000" tw="False" sr="50000" htw="24" mtw="60" emt="False" acd="False" mgf="False" eo="2" emd="False" eo_sftp="1">
          <ts>
            <t n="Diagnostic_LowerLimitTime" t="1" qn="Q_Diagnostic_ExtractLeadsRange" cn="Diagnostic_LowerLimitTime" m="0">
              <schemas />
              <specs />
            </t>
            <t n="Diagnostic_UpperLimitTime" t="1" qn="Q_Diagnostic_ExtractLeadsRange" cn="Diagnostic_UpperLimitTime" m="0">
              <schemas />
              <specs />
            </t>
            <t n="Diagnostic_RescoreThreshold" t="1" qn="Q_Diagnostic_ExtractLeadsRange" cn="Diagnostic_RescoreThreshold" m="0">
              <schemas />
              <specs />
            </t>
          </ts>
          <mcs>
            <mc cn="C_DateCreated" />
            <mc cn="C_DateModified" />
            <mc cn="__SCORE_DATE_FIELD__" />
            <mc cn="__SCORE_FIELD__" />
            <mc cn="ContactID" />
          </mcs>
        </rds>
      </rdss>
      <validationExtracts />
      <ces />
      <extractQueries />
      <extractQuerySequences />
      <leafExtracts />
      <launchExtracts />
      <jobs />
      <pdmatches />
      <leadscroings />
      <lssbardins />
      <lssbardouts />
      <lds />
      <ecs />
      <gCs />
    </group>
    '''
    else:
      step1_xml = '''
      <group name="Diagnostic_LoadLeads" alias="Diagnostic_LoadLeads" w="Workspace" type="1" scheduleType="0" allowUserChangeScheduleType="False" visibleForEndUser="True" threshold="10000" launchExpiredDays="7" createdBy="richard.liu@lattice-engines.com" path="Diagnostic" autoGenerated="False" validationValidMinutes="120" autoClearOnFailure="False" mergeRulesSaved="True" ng="False">
        <schemas />
        <visiDBConfigurationWithMacros />
        <targetQueries />
        <targetQuerySequences />
        <rdss>
          <rds n="SFDC_Contact_Validation" w="Workspace" sn="SFDC_Contact_Validation" cn="SFDC_DataProvider" u="False" ss="" tn="Contact" nmo="1" f="LastModifiedDate&gt; #Diagnostic_LowerLimitTime_Contact AND LastModifiedDate&lt; #Diagnostic_UpperLimitTime_Contact AND ( __CONTACT_SCORE_DATE_FIELD__ &lt; #Diagnostic_RescoreThreshold_Contact OR __CONTACT_SCORE_DATE_FIELD__ = null ) AND @RecordCOUNT(1000)" ad="False" em="False" td="False" ic="" dd="" l="1000" tw="False" sr="50000" htw="24" mtw="60" emt="False" acd="False" mgf="False" eo="2" emd="False" eo_sftp="2">
            <ts>
              <t n="Diagnostic_LowerLimitTime_Contact" t="1" qn="Q_Diagnostic_ExtractLeadsRange" cn="Diagnostic_LowerLimitTime_Contact" m="0">
                <schemas />
                <specs />
              </t>
              <t n="Diagnostic_UpperLimitTime_Contact" t="1" qn="Q_Diagnostic_ExtractLeadsRange" cn="Diagnostic_UpperLimitTime_Contact" m="0">
                <schemas />
                <specs />
              </t>
              <t n="Diagnostic_RescoreThreshold_Contact" t="1" qn="Q_Diagnostic_ExtractLeadsRange" cn="Diagnostic_RescoreThreshold_Contact" m="0">
                <schemas />
                <specs />
              </t>
            </ts>
            <mcs>
              <mc cn="CreatedDate" />
              <mc cn="Id" />
              <mc cn="LastModifiedDate" />
              <mc cn="__CONTACT_SCORE_DATE_FIELD__" />
              <mc cn="__CONTACT_SCORE_FIELD__" />
            </mcs>
          </rds>
          <rds n="SFDC_Lead_Validation" w="Workspace" sn="SFDC_Lead_Validation" cn="SFDC_DataProvider" u="False" ss="" tn="Lead" nmo="1" f="LastModifiedDate&gt; #Diagnostic_LowerLimitTime_Lead AND LastModifiedDate&lt; #Diagnostic_UpperLimitTime_Lead AND ( __LEAD_SCORE_DATE_FIELD__ &lt; #Diagnostic_RescoreThreshold_Lead OR __LEAD_SCORE_DATE_FIELD__  = null ) AND @RecordCOUNT(1000)" ad="False" em="False" td="False" ic="" dd="" l="1000" tw="False" sr="50000" htw="24" mtw="60" emt="False" acd="False" mgf="False" eo="2" emd="False" eo_sftp="2">
            <ts>
              <t n="Diagnostic_LowerLimitTime_Lead" t="1" qn="Q_Diagnostic_ExtractLeadsRange" cn="Diagnostic_LowerLimitTime_Lead" m="0">
                <schemas />
                <specs />
              </t>
              <t n="Diagnostic_UpperLimitTime_Lead" t="1" qn="Q_Diagnostic_ExtractLeadsRange" cn="Diagnostic_UpperLimitTime_Lead" m="0">
                <schemas />
                <specs />
              </t>
              <t n="Diagnostic_RescoreThreshold_Lead" t="1" qn="Q_Diagnostic_ExtractLeadsRange" cn="Diagnostic_RescoreThreshold_Lead" m="0">
                <schemas />
                <specs />
              </t>
            </ts>
            <mcs>
              <mc cn="CreatedDate" />
              <mc cn="Id" />
              <mc cn="LastModifiedDate" />
              <mc cn="__LEAD_SCORE_DATE_FIELD__" />
              <mc cn="__LEAD_SCORE_FIELD__" />
            </mcs>
          </rds>
        </rdss>
        <validationExtracts />
        <ces />
        <extractQueries />
        <extractQuerySequences />
        <leafExtracts />
        <launchExtracts />
        <jobs />
        <pdmatches />
        <leadscroings />
        <lssbardins />
        <lssbardouts />
        <lds />
        <ecs />
        <gCs />
      </group>
      '''
    step1_xml = step1_xml.replace('__CONTACT_SCORE_DATE_FIELD__', sfdcContactScoreDateField)
    step1_xml = step1_xml.replace('__LEAD_SCORE_DATE_FIELD__', sfdcLeadScoreDateField)
    step1_xml = step1_xml.replace('__CONTACT_SCORE_FIELD__', sfdcContactScoreField)
    step1_xml = step1_xml.replace('__LEAD_SCORE_FIELD__', sfdcLeadScoreField)
    step1_xml = step1_xml.replace('__SCORE_DATE_FIELD__', scoreDateField)
    step1_xml = step1_xml.replace('__SCORE_FIELD__', scoreField)

    lgm.setLoadGroup(step1_xml)

    # Modified score/score-date fields of Diagnostic_LoadLeads_Bulk
    step1 = etree.fromstring(lgm.getLoadGroup('Diagnostic_LoadLeads').encode('ascii', 'xmlcharrefreplace'))

    step1.set('name', 'Diagnostic_LoadLeads_Bulk')
    step1.set('alias', 'Diagnostic_LoadLeads_Bulk')

    lgm.setLoadGroup(etree.tostring(step1))
    diag_rdss_xml = lgm.getLoadGroupFunctionality('Diagnostic_LoadLeads_Bulk', "rdss")

    if type == 'MKTO':
      diag_rdss_xml = re.sub(r' f=\"@.*?\"', ' f="@datediff(now, month(6)) AND&#xD;&#xA;activityType in (\'New Lead\','
                                             '\'Click Link\',\'Visit Webpage\',\'Interesting Moment\',\'Open Email\','
                                             '\'Email Bounced Soft\',\'Fill Out Form\',\'Unsubscribe Email\','
                                             '\'Click Email\')"',
                             diag_rdss_xml)
    elif type == 'ELQ':
      diag_rdss_xml = re.sub(r' f=\"@.*?\"',
                             ' f="@datediff(now, month(6)) AND (__SCORE_DATE_FIELD__ &lt; '
                             '#Diagnostic_RescoreThreshold OR __SCORE_FIELD__ = 0 )"',
                             diag_rdss_xml)
      diag_rdss_xml=diag_rdss_xml.replace('__SCORE_DATE_FIELD__ ', scoreDateField)
      diag_rdss_xml=diag_rdss_xml.replace('__SCORE_FIELD__ ', scoreField)

    elif type == 'SFDC':
      diag_rdss_xml = re.sub(r'<rds n=\"SFDC_Contact_Validation\".*f=\"@.*?\"',
                             ' f="@datediff(now, month(6)) AND ( __CONTACT_SCORE_DATE_FIELD__ &lt; '
                             '#Diagnostic_RescoreThreshold_Contact OR __CONTACT_SCORE_FIELD__ = null )"',
                             diag_rdss_xml)
      diag_rdss_xml.replace('__CONTACT_SCORE_DATE_FIELD__', sfdcContactScoreDateField)
      diag_rdss_xml.replace('__CONTACT_SCORE_FIELD__', sfdcContactScoreField)
      diag_rdss_xml = re.sub(r'<rds n=\"SFDC_Lead_Validation\".*f=\"@.*?\"',
                             ' f="@datediff(now, month(6)) AND ( __LEAD_SCORE_DATE_FIELD__ &lt; '
                             '#Diagnostic_RescoreThreshold_Contact OR __LEAD_SCORE_DATE_FIELD__ = null )"',
                             diag_rdss_xml)
      diag_rdss_xml.replace('__LEAD_SCORE_DATE_FIELD__', sfdcLeadScoreDateField)

    lgm.setLoadGroupFunctionality('Diagnostic_LoadLeads_Bulk', diag_rdss_xml)

    success = True

    return success
