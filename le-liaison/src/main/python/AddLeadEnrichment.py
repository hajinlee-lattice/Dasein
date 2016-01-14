#!/usr/bin/python

import sys, requests, re
from copy import deepcopy
from lxml import etree
from liaison import *


def AddLeadEnrichment(tenants, attributes):

    for ten in tenants:

        source = 'EnrichmentColumns'

        try:
            conn_mgr = ConnectionMgrFactory.Create('visiDB', tenant_name=ten)
        except TenantNotMappedToURL:
            print 'Tenant \'{0}\' not on LP DataLoader'.format(ten)
            continue

        (lp_template_type, lp_template_version) = getLPTemplateTypeAndVersion(conn_mgr)

        addLDCMatch(conn_mgr, source, lp_template_version)

        setLDCWritebackAttributes(conn_mgr, source, attributes, lp_template_version)

        conn_mgr.getLoadGroupMgr().commit()


def addLDCMatch(conn_mgr, source, lp_template_version):

    lg_mgr = conn_mgr.getLoadGroupMgr()

    if lg_mgr.hasLoadGroup('PropDataMatch_Step1'):

        pdm1_pdmatches_xml = lg_mgr.getLoadGroupFunctionality('PropDataMatch_Step1', 'pdmatches')
        pdm1_pdmatches = etree.fromstring(pdm1_pdmatches_xml)

        enrichment_match_xml = ''
        for pdmatch in pdm1_pdmatches.iter('pdmatch'):
            if pdmatch.get('n') == 'PD':
                enrichment_match_xml = etree.tostring(pdmatch)
            elif pdmatch.get('n') == 'PD_Enrichment':
                pdm1_pdmatches.remove(pdmatch)

        if enrichment_match_xml != '':
            enrichment_match = etree.fromstring(enrichment_match_xml)
            enrichment_match.set('n', 'PD_Enrichment')
            scs = enrichment_match.find('scs')
            for c in scs.iter('c'):
                if c.get('mcn') in ['Name','City','State','Country']:
                    scs.remove(c)
            luos = enrichment_match.find('luos')
            for luo in luos.iter('luo'):
               luos.remove(luo)
            luo = etree.SubElement(luos, 'luo')
            luo.set('n', source)

            pdm1_pdmatches.append(enrichment_match)

        pdm1_pdmatches_xml = etree.tostring(pdm1_pdmatches)
        lg_mgr.setLoadGroupFunctionality('PropDataMatch_Step1', pdm1_pdmatches_xml)


def setLDCWritebackAttributes(conn_mgr, source, attributes, lp_template_version):

    lg_mgr = conn_mgr.getLoadGroupMgr()

    ##DataLoader Config
    
    queryNames = []

    for ptld in ['PushLeadsInDanteToDestination','PushLeadsLastScoredToDestination']:

        if lg_mgr.hasLoadGroup(ptld):

            ptld_targetQueries_xml = lg_mgr.getLoadGroupFunctionality(ptld, 'targetQueries')
            ptld_targetQueries = etree.fromstring(ptld_targetQueries_xml)

            for targetQuery in ptld_targetQueries.iter('targetQuery'):
                if ptld == 'PushLeadsLastScoredToDestination':
                    queryNames.append(targetQuery.get('name'))
                fsColumnMappings = targetQuery.find('fsColumnMappings')
                for fsColumnMapping in fsColumnMappings.iter('fsColumnMapping'):
                    if fsColumnMapping.get('queryColumnName')[0:4] == 'LDC_':
                        fsColumnMappings.remove(fsColumnMapping)
                for (ldcCol,customerCol) in attributes.iteritems():
                    fsColumnMapping = etree.SubElement(fsColumnMappings, 'fsColumnMapping')
                    fsColumnMapping.set('queryColumnName', 'LDC_'+ldcCol)
                    fsColumnMapping.set('fsColumnName', customerCol)
                    fsColumnMapping.set('formatter', '')
                    fsColumnMapping.set('type', '0')
                    fsColumnMapping.set('ignoreType', '0')
                    fsColumnMapping.set('ignoreOptions', '0')
                    fsColumnMapping.set('formatColumnName', 'False')
                    fsColumnMapping.set('charsToFormat', '')

            ptld_targetQueries_xml = etree.tostring(ptld_targetQueries)
            lg_mgr.setLoadGroupFunctionality(ptld, ptld_targetQueries_xml)

    ## visiDB Config

    for queryName in queryNames:

        q = conn_mgr.getQuery(queryName)
        columns = deepcopy(q.getColumnNames())
        for c in columns:
            if c[0:4] == 'LDC_':
                q.removeColumn(c)

        for (ldcCol,customerCol) in attributes.iteritems():
            specstr = 'LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("{0}")), ContainerElementName("{1}")))'.format('PD_'+source,ldcCol)
            e = ExpressionVDBImplGeneric(specstr)
            c = QueryColumnVDBImpl('LDC_'+ldcCol,e)
            c.setApprovedUsage('None')
            c.setTags('External')
            c.setCategory('Lead Enrichment')
            q.appendColumn(c)

        conn_mgr.setQuery(q)


def getLPTemplateTypeAndVersion(conn_mgr):

    type    = 'Unknown'
    version = 'Unknown'
    defn    = 'Undefined'
    try:
        exp_version = conn_mgr.getNamedExpression('Version')
        defn = exp_version.Object().definition()
        c = re.search('LatticeFunctionExpressionConstant\(\"PLS (.*?) Template:\".*LatticeFunctionExpressionConstant\(\"(.*?)\"', defn)
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

    return (type,version)


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
    print 'Usage: {0} --tenantlist <tenant_list.csv> <attributes.csv>'.format(cmd)
    print 'Usage: {0} --tenant <tenant_name> <attributes.csv>'.format(cmd)
    print ''
    print '<attributes.csv> is a CSV file with two columns:'
    print 'LDCColumnName,CustomerColumnName'
    print ''
    
    exit(exit_code)


if __name__ == "__main__":

    if len(sys.argv) == 1:
        usage(sys.argv[0], 0)

    option = sys.argv[1]

    if option not in ['--tenant','--tenantlist']:
        usage(sys.argv[0], 1)

    if len(sys.argv) != 4:
        usage(sys.argv[0], 2)
    
    tenants = []

    if option == '--tenant':
        tenants.append(sys.argv[2])
    else:
        with open(sys.argv[2]) as tenantFile:
            for line in tenantFile:
                cols = line.strip().split(',')
                tenants.append(cols[0])

    attributes = {}

    attributeFileName = sys.argv[3]
    with open(attributeFileName) as attributeFile:
        isHeaderLine = True
        for line in attributeFile:
            if not isHeaderLine:
                cols = line.strip().split(',')
                attributes[cols[0]] = cols[1]
            isHeaderLine = False
    
    AddLeadEnrichment(tenants, attributes)
