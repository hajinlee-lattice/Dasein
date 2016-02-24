#!/usr/bin/python

import sys, datetime, time, requests, re
import requests
from lxml import etree
import appsequence
from liaison import *

def SetRuntimeConfig_BuyerInsights(tenants, period, hasStaticAttributes):
    
    for ten in tenants:

        try:
            conn_mgr = ConnectionMgrFactory.Create('visiDB', tenant_name=ten)
            lg_mgr = conn_mgr.getLoadGroupMgr()
        except TenantNotMappedToURL:
            print 'Tenant \'{0}\' not on LP DataLoader'.format(ten)
            continue

        (type,version) = getLPTemplateTypeAndVersion(conn_mgr)

        if version in ['2.1.1','2.1.2']:

            setIAS_21(lg_mgr, type, hasStaticAttributes)

            if period == 'disabled':
                setPTLD_Disabled_21(lg_mgr)
                setFDT_Disabled_21(lg_mgr)

            elif period == 'hourly':
                setPTLD_Enabled_21(lg_mgr)
                setFDT_Disabled_21(lg_mgr)

            elif period == 'daily':
                setPTLD_Disabled_21(lg_mgr)
                setFDT_Enabled_21(lg_mgr)

            lg_mgr.commit()

        elif version in ['2.2.0','2.3.0']:

            setIAS_21(lg_mgr, type, hasStaticAttributes)

            if period == 'disabled':
                setPTLD_Disabled_22(lg_mgr)
                setFDT_Disabled_21(lg_mgr)

            elif period == 'hourly':
                setPTLD_Enabled_22(lg_mgr)
                setFDT_Disabled_21(lg_mgr)

            elif period == 'daily':
                setPTLD_Disabled_22(lg_mgr)
                setFDT_Enabled_21(lg_mgr)

            lg_mgr.commit()

        else:
            print 'Version \'{0}\' is not supported'.format(version)



def setIAS_21(lgm, type, hasStaticAttributes):

    ngsxml = '<ngs>'
    if not hasStaticAttributes:
        ngsxml += '<ng n="CreateBIQueries"/>'
    ngsxml += '<ng n="ExtractDanteLeadsIntoSourceTable"/>'
    if type in ['ELQ','MKTO']:
        ngsxml += '<ng n="ExtractDanteContactsIntoSourceTable"/>'
    ngsxml += '<ng n="ExtractAnalyticAttributesIntoSourceTable"/>'
    if type in ['ELQ','MKTO']:
        ngsxml += '<ng n="PushDanteContactsAndAnalyticAttributesToDante"/>'
    ngsxml += '<ng n="PushDanteLeadsAndAnalyticAttributesToDante"/>'
    ngsxml += '<ng n="UpdateDanteTimestamp_Step1"/>'
    ngsxml += '<ng n="UpdateDanteTimestamp_Step2"/>'
    ngsxml += '</ngs>'

    lgm.setLoadGroupFunctionality('InsightsAllSteps', ngsxml)


def setPTLD_Disabled_21(lgm):

    ngsxml = '<ngs>'
    ngsxml += '<ng n="LoadScoredLeads_Step1"/>'
    ngsxml += '<ng n="LoadScoredLeads_Step2"/>'
    ngsxml += '<ng n="PushLeadsLastScoredToDestination"/>'
    ngsxml += '</ngs>'

    lgm.setLoadGroupFunctionality('PushToLeadDestination', ngsxml)


def setPTLD_Enabled_21(lgm):

    ngsxml = '<ngs>'
    ngsxml += '<ng n="LoadScoredLeads_Step1"/>'
    ngsxml += '<ng n="LoadScoredLeads_Step2"/>'
    ngsxml += '<ng n="PushDataToDante_Hourly"/>'
    ngsxml += '<ng n="InsightsAllSteps"/>'
    ngsxml += '<ng n="PushLeadsLastScoredToDestination"/>'
    ngsxml += '</ngs>'

    lgm.setLoadGroupFunctionality('PushToLeadDestination', ngsxml)


def setPTLD_Disabled_22(lgm):

    ngsxml = '<ngs>'
    ngsxml += '<ng n="LoadScoredLeads_Step1"/>'
    ngsxml += '<ng n="LoadScoredLeads_Step2"/>'
    ngsxml += '<ng n="PushLeadsLastScoredToDestination"/>'
    ngsxml += '<ng n="PushToLeadDestination_TimeStamp"/>'
    ngsxml += '<ng n="PushToLeadDestination_Validation"/>'
    ngsxml += '</ngs>'

    lgm.setLoadGroupFunctionality('PushToLeadDestination', ngsxml)


def setPTLD_Enabled_22(lgm):

    ngsxml = '<ngs>'
    ngsxml += '<ng n="LoadScoredLeads_Step1"/>'
    ngsxml += '<ng n="LoadScoredLeads_Step2"/>'
    ngsxml += '<ng n="PushDataToDante_Hourly"/>'
    ngsxml += '<ng n="InsightsAllSteps"/>'
    ngsxml += '<ng n="PushLeadsLastScoredToDestination"/>'
    ngsxml += '<ng n="PushToLeadDestination_TimeStamp"/>'
    ngsxml += '<ng n="PushToLeadDestination_Validation"/>'
    ngsxml += '</ngs>'

    lgm.setLoadGroupFunctionality('PushToLeadDestination', ngsxml)


def setFDT_Disabled_21(lgm):

    fdt_ngs_xml = lgm.getLoadGroupFunctionality('FinalDailyTasks', 'ngs')
    fdt_ngs = etree.fromstring( fdt_ngs_xml )
    for ng in fdt_ngs:
        if ng.get('n') == 'PushDataToDante_Hourly' or ng.get('n') == 'InsightsAllSteps':
            fdt_ngs.remove( ng )
    lgm.setLoadGroupFunctionality('FinalDailyTasks', etree.tostring(fdt_ngs))


def setFDT_Enabled_21(lgm):

    setFDT_Disabled_21(lgm)

    fdt_ngs_xml = lgm.getLoadGroupFunctionality('FinalDailyTasks', 'ngs')
    fdt_ngs = etree.fromstring(fdt_ngs_xml)
    fdt_ngs.insert(0, etree.Element('ng', n='PushDataToDante_Hourly'))
    fdt_ngs.insert(1, etree.Element('ng', n='InsightsAllSteps'))
    lgm.setLoadGroupFunctionality('FinalDailyTasks', etree.tostring(fdt_ngs))


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
    print 'Usage: {0} --tenantlist <tenant_list.csv> <disabled|hourly|daily> <updateAttributes|fixedAttributes>'.format(cmd)
    print 'Usage: {0} --tenant <tenant_name> <disabled|hourly|daily> <updateAttributes|fixedAttributes>'.format(cmd)
    print ''
    
    exit(exit_code)


if __name__ == "__main__":

    if len(sys.argv) == 1:
        usage(sys.argv[0], 0)

    option = sys.argv[1]

    if option not in ['--tenant','--tenantlist']:
        usage(sys.argv[0], 1)

    if len(sys.argv) != 5:
        usage(sys.argv[0], 2)
    
    tenants = []

    if option == '--tenant':
        tenants.append(sys.argv[2])
    else:
        with open(sys.argv[2]) as tenantFile:
            for line in tenantFile:
                cols = line.strip().split(',')
                tenants.append(cols[0])

    period = sys.argv[3]
    if period not in ['disabled','hourly','daily']:
        print 'ERROR: Unknown period \'{0}\''.format(period)
        usage(sys.argv[0], 3)

    attributes =  sys.argv[4]
    if attributes not in ['updateAttributes','fixedAttributes']:
        print 'ERROR: Unknown attribute setting \'{0}\''.format(attributes)
        usage(sys.argv[0], 4)

    hasStaticAttributes = False
    if attributes == 'fixedAttributes':
        hasStaticAttributes = True
    
    SetRuntimeConfig_BuyerInsights(tenants, period, hasStaticAttributes)
