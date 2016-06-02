
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import os, sys, re
from copy import deepcopy
from liaison import *
from appsequence import Applicability, AppSequence, StepBase

class LPMigration_LP3ScoringRecentAllRowsQuery(StepBase):

    name        = 'LPMigration_LP3ScoringRecentAllRowsQuery'
    description = 'Adds query for extracting an event table for LP3 scoring: recent out-of-sample leads.'
    version     = '$Rev$'


    def __init__(self, forceApply=False):
        super(LPMigration_LP3ScoringRecentAllRowsQuery, self).__init__(forceApply)


    def getApplicability(self, appseq):
        try:
            q = appseq.getConnectionMgr().getQuery('Q_LP3_ModelingLead_OneLeadPerDomain')
        except UnknownVisiDBSpec:
            return Applicability.cannotApplyFail

        return Applicability.canApply


    def apply( self, appseq ):
        print '\n    * Installing query (Q_LP3_ScoringLead_RecentAllRows) for LP3 scoring tests . .',
        sys.stdout.flush()

        conn_mgr = appseq.getConnectionMgr()
        template_type = appseq.getText('template_type')

        q_lp3_modeling = conn_mgr.getQuery('Q_LP3_ModelingLead_OneLeadPerDomain')

        spec_email = None
        if template_type == 'MKTO':
            spec_email = 'LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")), ContainerElementName("Email")))'
            leadEntity = 'MKTO_LeadRecord_ID'
        elif template_type == 'SFDC':
            spec_email = 'LatticeFunctionIdentifier(ContainerElementName("SFDC_Email"))'
            leadEntity = 'SFDC_Lead_Contact_ID'
        else:
            print '\n      => NOT SUPPORTED for template type {0}'.format(template_type),
            return False

        q_lp3_modeling.getColumn('Email').setExpression(ExpressionVDBImplFactory.create(spec_email))

        filtersToRemove = []
        for f in q_lp3_modeling.getFilters():
            c = re.search('.*?ContainerElementName\("SelectedForModeling_(SFDC|MKTO|ELQ)"\).*?', f.definition())
            if c:
                filtersToRemove.append(f)

        for f in filtersToRemove:
            q_lp3_modeling.getFilters().remove(f)

        ## Create "Lead_InDateRangeForLastMonth"
        spec_Lead_InDateRangeForLastMonth = 'SpecLatticeFunction(LatticeFunctionExpressionTransform(LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("AND"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("Greater"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("AddDay"), LatticeFunctionIdentifier(ContainerElementName("Alias_ModifiedDate_Lead")), LatticeFunctionIdentifier(ContainerElementName("Const_DaysOfDataForModeling"))), LatticeFunctionExpressionTransform(LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementName("Alias_ModifiedDate_Lead")), LatticeAddressSetIdentifier(ContainerElementName("Alias_AllLeadTable")), FunctionAggregationOperator("Max")), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicAll)), FunctionAggregationOperator("None"))), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("Less"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("Minus"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("ConvertToInt"), LatticeFunctionExpressionTransform(LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementName("Alias_CreatedDate_Lead")), LatticeAddressSetIdentifier(ContainerElementName("Alias_AllLeadTable")), FunctionAggregationOperator("Max")), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicAll)), FunctionAggregationOperator("None"))), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("IsNull"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("ConvertToInt"), LatticeFunctionIdentifier(ContainerElementName("Alias_CreatedDate_Lead"))), LatticeFunctionExpressionConstantScalar("0", DataTypeInt))), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("Multiply"), LatticeFunctionExpressionConstant("86400", DataTypeInt), LatticeFunctionIdentifier(ContainerElementName("Const_DaysFromLeadCreationDate"))))), LatticeAddressSetIdentifier(ContainerElementName("Alias_AllLeadTable")), FunctionAggregationOperator("Max")), DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription(""))'
        conn_mgr.setNamedExpression(NamedExpressionVDBImpl.InitFromDefn('Lead_InDateRangeForLastMonth', spec_Lead_InDateRangeForLastMonth))

        ## Create "AppData_Lead_IsCreatedSinceModeling"
        exp_modeling = conn_mgr.getNamedExpression('AppData_Lead_IsConsideredForModeling')
        defn_modeling = exp_modeling.Object().definition().replace('Lead_InDateRangeForModeling', 'Lead_InDateRangeForLastMonth')
        exp_scoring = NamedExpressionVDBImpl(
                'AppData_Lead_IsCreatedSinceModeling', ExpressionVDBImplFactory.create(defn_modeling), exp_modeling.OtherSpecs())
        conn_mgr.setNamedExpression(exp_scoring)

        ## Create "Lead_IsSelectedForModeling_CreatedSinceModeling"
        exp_selmodeling = conn_mgr.getNamedExpression('Lead_IsSelectedForModeling')
        defn_selmodeling = exp_selmodeling.Object().definition().replace(
                'AppData_Lead_IsConsideredForModeling', 'AppData_Lead_IsCreatedSinceModeling')
        exp_selcoring = NamedExpressionVDBImpl(
                'Lead_IsSelectedForModeling_CreatedSinceModeling', ExpressionVDBImplFactory.create(defn_selmodeling), exp_selmodeling.OtherSpecs())
        conn_mgr.setNamedExpression(exp_selcoring)


        filter_AppData_Lead_IsCreatedSinceModeling = QueryFilterVDBImpl('LatticeAddressSetFcn(LatticeFunctionIdentifier(ContainerElementName("AppData_Lead_IsCreatedSinceModeling")), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("{0}")))))'.format(leadEntity))
        q_lp3_modeling.getFilters().append(filter_AppData_Lead_IsCreatedSinceModeling)

        col_IsPublicDomain = QueryColumnVDBImpl('IsPublicDomain', ExpressionVDBImplFactory.parse('Email_Domain_IsPublic'))
        col_IsMatched = QueryColumnVDBImpl('IsMatched', ExpressionVDBImplFactory.parse('PD_DerivedColumns.IsMatched'))
        col_IsSelectedLeadForDomain = QueryColumnVDBImpl(
                'IsSelectedLeadForDomain', ExpressionVDBImplFactory.parse('Lead_IsSelectedForModeling_CreatedSinceModeling'))

        q_lp3_modeling.appendColumn(col_IsPublicDomain)
        q_lp3_modeling.appendColumn(col_IsMatched)
        q_lp3_modeling.appendColumn(col_IsSelectedLeadForDomain)

        q_lp3_modeling.setName('Q_LP3_ScoringLead_RecentAllRows')
        conn_mgr.setQuery(q_lp3_modeling)

        return True
