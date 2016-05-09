#!/usr/bin/python

#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import sys, requests, multiprocessing, time, datetime
from lxml import etree
from liaison import *


def executeScoringAnalysis(tenants):


    tasks = []
    resultsContainer = []

    for t in tenants:

        print 'Executing scoring analysis for \'{0}\':'.format(t)

        try:
            conn_mgr = ConnectionMgrFactory.Create('visiDB', tenant_name=t, verbose=False)
        except TenantNotMappedToURL:
            print 'Not on LP DataLoader; skipping'
            continue

        tdata = (t,conn_mgr)
        tasks.append(tdata)

    pool = multiprocessing.Pool()
    r = pool.map_async(executeForTenant, tasks, callback=resultsContainer.append)
    r.wait()
    if len(resultsContainer) == 0:
        print 'No results returned'
    else:
        results = resultsContainer[0]
        with open('scoringAnalysisResults.csv', mode='w') as resultFile:
            resultFile.write('Tenant,Succes\n')
            for d in results:
                for (key,value) in d.iteritems():
                    resultFile.write('{0},{1}\n'.format(key, value))


def executeForTenant(tdata):
    (tenant, conn_mgr) = tdata

    setMonitorTenant(tenant, conn_mgr, False)
    executeAll = False

    executeNext = installValidationQuery(tenant, conn_mgr)
    if executeNext:
        executeAll = waitForLastPushToScoring(tenant, conn_mgr)
    
    if executeAll:

        executeNext = True
        print '{0:20s}: Waiting 5 Minutes'.format(tenant)
        time.sleep(300)

        attempts = 5
        while attempts > 0:
            attempts -= 1
            executeNext = execPTLD(tenant, conn_mgr, executeNext)
            executeNext = execValidationQuery(tenant, conn_mgr, executeNext)
            if executeNext:
                break

        executeNext = execExtractScores(tenant, conn_mgr, 'reference', executeNext)
        executeNext = execScoresInDateRange(tenant, conn_mgr, executeNext)

        if executeNext:
            print '{0:20s}: Waiting 10 Minutes'.format(tenant)
            time.sleep(600)

            attempts = 5
            while attempts > 0:
                attempts -= 1
                executeNext = execPullFromScoring(tenant, conn_mgr, executeNext)
                executeNext = execValidationQuery(tenant, conn_mgr, executeNext)
                if executeNext:
                    break

        executeNext = execExtractScores(tenant, conn_mgr, 'test', executeNext)
        executeNext = execExportSummary(tenant, conn_mgr, executeNext)
    else:
        executeNext = False

    setMonitorTenant(tenant, conn_mgr, True)
    print '{0:20s}: Completed'.format(tenant)
    result = {tenant:executeNext}
    return result


def waitForLastPushToScoring(tenant, conn_mgr):

    print '{0:20s}: Waiting for Last PushToScoringDB To Finish'.format(tenant)

    attempts = 180
    while attempts > 0:
        ptstimestr = conn_mgr.getLoadGroupStatus('PushToScoringDB')['LastSuccessDate']
        ptstime = datetime.datetime.strptime(ptstimestr[:19], '%Y-%m-%dT%H:%M:%S') - datetime.timedelta(hours=3)

        crmstatus = conn_mgr.getLoadGroupStatus('Nested_LoadCRMData')
        crmproc = crmstatus['ProcessStatus']
        crmtimestr = crmstatus['LastSuccessDate']
        crmtime = datetime.datetime.strptime(crmtimestr[:19], '%Y-%m-%dT%H:%M:%S') - datetime.timedelta(hours=3)

        ptldstatus = conn_mgr.getLoadGroupStatus('PushToLeadDestination')
        ptldproc = ptldstatus['ProcessStatus']
        ptldtimestr = ptldstatus['LastSuccessDate']
        ptldtime = datetime.datetime.strptime(ptldtimestr[:19], '%Y-%m-%dT%H:%M:%S') - datetime.timedelta(hours=3)
        if ptstime > ptldtime and ptstime > crmtime and ptldproc != 'InProcess' and crmproc != 'InProcess':
            return True
        time.sleep(60)
        attempts -= 1

    return False


def installValidationQuery(tenant, conn_mgr):

    print '{0:20s}: Installing Validation Query'.format(tenant)

    e1 = ExpressionVDBImplFactory.create('LatticeFunctionExpressionTransform(LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("IF"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("Equal"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Timestamp_PushToScoring")), ContainerElementName("Time_OfSubmission_PushToScoring"))), LatticeFunctionIdentifier(ContainerElementName("Time_OfMostRecentScore"))), LatticeFunctionExpressionConstant("1", DataTypeInt), LatticeFunctionExpressionConstantNull(DataTypeInt)), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToScoring")))), FunctionAggregationOperator("Count"))')
    e2 = ExpressionVDBImplFactory.create('LatticeFunctionExpressionTransform(LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("IF"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("Equal"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("Timestamp_PushToDestination")), ContainerElementName("Time_OfCompletion_PushToDestination"))), LatticeFunctionIdentifier(ContainerElementName("Time_OfMostRecentPushToDestination"))), LatticeFunctionExpressionConstant("1", DataTypeInt), LatticeFunctionExpressionConstantNull(DataTypeInt)), LatticeAddressSetSourceTable(LatticeSourceTableIdentifier(ContainerElementName("Timestamp_PushToDestination")), LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDestination")))), FunctionAggregationOperator("Count"))')

    c1 = QueryColumnVDBImpl('N_LastScored', e1)
    c2 = QueryColumnVDBImpl('N_LastPulledFromScoring', e2)

    q = QueryVDBImpl('Q_Validate_ScoresDownloaded', [c1, c2])

    try:
        conn_mgr.setQuery(q)
    except:
        return False

    return True


def setMonitorTenant(tenant, conn_mgr, enabled):

    status = 'Off'
    if enabled:
        status = 'On'

    print '{0:20s}: Setting JAMS {1}'.format(tenant, status)

    conn_mgr.setMonitorTenant(enabled)


def execPTLD(tenant, conn_mgr, execute):

    if execute:
        print '{0:20s}: Executing PushToLeadDestination'.format(tenant)

        try:
            launchid = conn_mgr.executeGroup('PushToLeadDestination', 'mwilson@lattice-engines.com')
        except:
            print '{0:20s}: ERROR Cannot Execute PushToLeadDestination'.format(tenant)
            return False

        while True:
            status = conn_mgr.getLaunchStatus(launchid)
            if status['Running'] == 'False':
                break
            time.sleep(60)
        if status['RunSucceed'] == 'True':
            return True

    return False


def execValidationQuery(tenant, conn_mgr, execute):

    success = False
    if execute:
        print '{0:20s}: Executing Validation Query'.format(tenant)

        queryResult = conn_mgr.executeQuery('Q_Validate_ScoresDownloaded')
        while(not queryResult.isReady()):
            time.sleep(5)

        queryRows = queryResult.fetchAll()
        for row in queryRows:
            if row[0] == row[1]:
                success = True

    return success


def execExtractScores(tenant, conn_mgr, mode, execute):

    if execute:
        loadGroup = ''
        if mode == 'reference':
            loadGroup = 'Diagnostic_ExtractScoresInDateRange'
        elif mode == 'test':
            loadGroup = 'Diagnostic_ExtractScoresInDateRange_Test'

        print '{0:20s}: Executing {1}'.format(tenant, loadGroup)

        try:
            launchid = conn_mgr.executeGroup(loadGroup, 'mwilson@lattice-engines.com')
        except:
            print '{0:20s}: ERROR Cannot Execute {1}'.format(tenant, loadGroup)
            return False

        while True:
            status = conn_mgr.getLaunchStatus(launchid)
            if status['Running'] == 'False':
                break
            time.sleep(60)
        if status['RunSucceed'] == 'True':
            return True

    return False


def execScoresInDateRange(tenant, conn_mgr, execute):

    if execute:
        print '{0:20s}: Executing BulkScoring_ScoresInDateRange'.format(tenant)

        try:
            launchid = conn_mgr.executeGroup('BulkScoring_ScoresInDateRange', 'mwilson@lattice-engines.com')
        except:
            print '{0:20s}: ERROR Cannot Execute BulkScoring_ScoresInDateRange'.format(tenant)
            return False

        while True:
            status = conn_mgr.getLaunchStatus(launchid)
            if status['Running'] == 'False':
                break
            time.sleep(60)
        if status['RunSucceed'] == 'True':
            return True

    return False


def execPullFromScoring(tenant, conn_mgr, execute):

    if execute:
        print '{0:20s}: Executing BulkScoring_PullFromScoringDB'.format(tenant)

        try:
            launchid = conn_mgr.executeGroup('BulkScoring_PullFromScoringDB', 'mwilson@lattice-engines.com')
        except:
            print '{0:20s}: ERROR Cannot Execute BulkScoring_PullFromScoringDB'.format(tenant)
            return False

        while True:
            status = conn_mgr.getLaunchStatus(launchid)
            if status['Running'] == 'False':
                break
            time.sleep(60)
        if status['RunSucceed'] == 'True':
            return True

    return False


def execExportSummary(tenant, conn_mgr, execute):

    if execute:
        print '{0:20s}: Executing Diagnostic_Summary_ScoresInDateRange'.format(tenant)

        try:
            launchid = conn_mgr.executeGroup('Diagnostic_Summary_ScoresInDateRange', 'mwilson@lattice-engines.com')
        except:
            print '{0:20s}: ERROR Cannot Execute Diagnostic_Summary_ScoresInDateRange'.format(tenant)
            return False

        while True:
            status = conn_mgr.getLaunchStatus(launchid)
            if status['Running'] == 'False':
                break
            time.sleep(60)
        if status['RunSucceed'] == 'True':
            return True

    return False


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

    executeScoringAnalysis(tenants)
