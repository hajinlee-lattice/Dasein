#!/usr/bin/python

#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import sys, requests, re
from lxml import etree
from liaison import *


def enableDerivedColumnsCache(tenants):

    for t in tenants:

        print '\nEnabling DerivedColumnsCache for \'{0}\':'.format(t)

        try:
            conn_mgr = ConnectionMgrFactory.Create('visiDB', tenant_name=t, verbose=True)
        except TenantNotMappedToURL:
            print 'Not on LP DataLoader; skipping'
            continue

        template_type = 'Unknown'
        template_version = 'Unknown'

        try:
            exp_version = conn_mgr.getNamedExpression('Version')
            defn = exp_version.Object().definition()
            c = re.search('.*?LatticeFunctionExpressionConstant\(\"PLS (.*?) Template:\".*LatticeFunctionExpressionConstant\(\"(.*?)\".*?', defn)
            if c:
                template_type = c.group(1)
                template_version = c.group(2)
            else:
                template_type = 'Nonstandard type'
                template_version = 'Nonstandard type'
        except requests.exceptions.SSLError:
            ## Not on a PROD DataLoader
            print 'Exception getting version: SSLError; skipping'
            continue
        except TenantNotFoundAtURL:
            ## Not on a PROD DataLoader
            print 'Exception getting version: not on LP DataLoader; skipping'
            continue
        except UnknownVisiDBSpec:
            print 'Exception getting version: Version not found; skipping'
            continue

        try:
            q_pls_modeling_spec = conn_mgr.getSpec('Q_PLS_Modeling')
            q_pls_scoring_bulk_spec = conn_mgr.getSpec('Q_PLS_Scoring_Bulk')
            q_pd = conn_mgr.getQuery('PD')
        except UnknownVisiDBSpec:
            print 'Not an initialized LP tenant'
            continue

        createPDCQueries(conn_mgr, q_pd, q_pls_modeling_spec, q_pls_scoring_bulk_spec)
        #createPDCTable(conn_mgr)


def createPDCQueries(conn_mgr, q_pd, q_pls_modeling_spec, q_pls_scoring_bulk_spec):

    q_pd.setName('PDC')

    filtersToRemove = []
    for f in q_pd.getFilters():
        c = re.search('.*?ContainerElementName\("HAVING_DownloadedLeadNotMatched_PD"\).*?', f.definition())
        if c:
            filtersToRemove.append(f)

    for f in filtersToRemove:
        q_pd.getFilters().remove(f)

    q_pls_modeling_spec = q_pls_modeling_spec.replace('ContainerElementName("PD_DerivedColumns")' ,'ContainerElementName("PDC_DerivedColumnsCache")')
    q_pls_scoring_bulk_spec = q_pls_scoring_bulk_spec.replace('ContainerElementName("PD_DerivedColumns")' ,'ContainerElementName("PDC_DerivedColumnsCache")')

    q_pls_modeling_cache = QueryVDBImpl.initFromDefn('Q_PLS_Modeling_DerivedColumnsCache', q_pls_modeling_spec)
    q_pls_scoring_bulk_cache = QueryVDBImpl.initFromDefn('Q_PLS_Scoring_Bulk_DerivedColumnsCache', q_pls_scoring_bulk_spec)

    conn_mgr.setQuery(q_pd)
    conn_mgr.setQuery(q_pls_modeling_cache)
    conn_mgr.setQuery(q_pls_scoring_bulk_cache)


def createPDCTable(conn_mgr):
    emailDomainSpecs = 'SpecFieldTypeAttribute(LatticeAddressAtomicIdentifier(ContainerElementName("Email_Domain")), SpecConstructiveIsConstructive, FunctionAggregationOperator("Combine")), SpecColumnContentContainerElementName(ContainerElementName("Source_Domain")), SpecEndpointTypeNone, SpecDefaultValueNullNoRTrim, SpecKeyAggregation(SpecColumnAggregationRuleFunction("Combine")), SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction("Combine"))'
    aggRuleSpecs = 'SpecSourceAggregationRuleMostRecentByKey(SpecTotalOrderEffectiveDate, SpecSourceKey((ContainerElementName("Source_Domain"))))'
    cols = [ TableColumnVDBImpl('Source_Domain', 'PDC_DerivedColumnsCache', 'VarChar(150)', emailDomainSpecs) ]
    pdc_dcc = TableVDBImpl('PDC_DerivedColumnsCache', cols, aggrule_spec=aggRuleSpecs)
    conn_mgr.setTable(pdc_dcc)


def usage(cmd, exit_code):
    path = ''
    i = cmd.rfind('\\')
    j = cmd.rfind('/')
    if( i != -1 ):
        path = cmd[:i]
        cmd = cmd[i+1:]
    elif( j != -1 ):
        path = cmd[:j]
        cmd = cmd[j+1:]

    print ''
    print 'PATH: {0}'.format(path)
    print 'Usage: {0} --tenantlist <tenant_list.csv>'.format(cmd)
    print 'Usage: {0} --tenant <tenant_name>'.format(cmd)
    print ''

    exit(exit_code)


if __name__ == "__main__":

    if len(sys.argv) == 1:
        usage(sys.argv[0], 0)

    option = sys.argv[1]

    if option not in ['--tenant','--tenantlist']:
        usage(sys.argv[0], 1)

    if len(sys.argv) != 3:
        usage(sys.argv[0], 2)

    tenants = []

    if option == '--tenant':
        tenants.append(sys.argv[2])
    else:
        with open(sys.argv[2]) as tenantFile:
            for line in tenantFile:
                cols = line.strip().split(',')
                tenants.append(cols[0])

    enableDerivedColumnsCache(tenants)
