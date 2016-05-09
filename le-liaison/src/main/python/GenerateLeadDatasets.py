#!/usr/bin/python

#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import sys, requests, re, codecs, time, datetime
from lxml import etree
from copy import deepcopy
from liaison import *

STD_REPORT_COLS = ['LeadID','Email','CreationDate','Company','LastName','FirstName','P1_Event']

STD_TMPL_COLS =      ['ModelingID']

STD_DS_COLS =        ['Website_Age_Months','Uses_Public_Email_Provider','CompanyName_Length','Domain_Length'
                     ,'Title_Length','Title_Level','RelatedLinks_Count','FundingStage1'
                     ,'JobsTrendString1','ModelAction1','Title_IsAcademic','FirstName_SameAs_LastName'
                     ,'CompanyName_Entropy','Phone_Entropy','Domain_IsClient','Industry_Group'
                     ,'Title_IsTechRelated','SpamIndicator']

STD_ACTIVITY_COLS =  ['Activity_Count_Click_Email','Activity_Count_Click_Link','Activity_Count_Email_Bounced_Soft'
                     ,'Activity_Count_Fill_Out_Form','Activity_Count_Interesting_Moment_Any'
                     ,'Activity_Count_Open_Email','Activity_Count_Unsubscribe_Email','Activity_Count_Visit_Webpage'
                     ,'Activity_Count_Interesting_Moment_Email'
                     ,'Activity_Count_Interesting_Moment_Event'
                     ,'Activity_Count_Interesting_Moment_Multiple'
                     ,'Activity_Count_Interesting_Moment_Pricing'
                     ,'Activity_Count_Interesting_Moment_Search'
                     ,'Activity_Count_Interesting_Moment_Webinar'
                     ,'Activity_Count_Interesting_Moment_key_web_page']

STD_CUSTOMER_DERIVED_COLS =  ['Number_of_Contacts_for_Domain','Most_Recent_Lead_Source']

STD_CUSTOMER_COLS_SFDC = ['SFDC_Company','SFDC_Title','SFDC_Phone','SFDC_Industry']


def generateLeadDatasets(tenants):

    for t in tenants:

        print '\nGenerating lead datasets for \'{0}\':'.format(t)

        try:
            conn_mgr = ConnectionMgrFactory.Create('visiDB', tenant_name=t, verbose=False)
        except TenantNotMappedToURL:
            print 'Not on LP DataLoader; skipping'
            continue
        dt = datetime.datetime.now()
        writeQueryToCSV(conn_mgr, t, 'Q_LP3_ScoringLead', dt)
        exit(0)
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
            q_pls_modeling = conn_mgr.getQuery('Q_PLS_Modeling')
            colnames_modeling = deepcopy(q_pls_modeling.getColumnNames())
            t_derivedcolumns = conn_mgr.getTable('PD_DerivedColumns')
            colnames_dc = t_derivedcolumns.getColumnNames()
        except UnknownVisiDBSpec:
            print 'Not an initialized LP tenant'
            continue

        colnames_as = []
        try:
            t_alexasource = conn_mgr.getTable('PD_Alexa_Source')
            colnames_as = t_alexasource.getColumnNames()
        except UnknownVisiDBSpec:
            pass

        if template_type == 'SFDC':
            entityname = 'SFDC_Lead_Contact_ID'
        elif template_type == 'MKTO':
            entityname = 'MKTO_LeadRecord_ID'
        elif template_type == 'ELQ':
            entityname = 'ELQ_Contact_ContactID'
        else:
            print 'Unsupported template type \'{0}\'; skipping'.format(template_type)
            continue

        filterModel = QueryFilterVDBImpl('LatticeAddressSetFcn(LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("NOT"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("Like"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("Right"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("ConvertToString"), LatticeFunctionExpressionAddressID(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("{0}"))))), LatticeFunctionExpressionConstant("1", DataTypeInt)), LatticeFunctionExpressionConstantScalar("0|5", DataTypeVarChar(3)))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("{0}")))))'.format(entityname))
        #q_pls_modeling.getFilters().append(filterModel)
        q_pls_modeling.setName("Q_LP3_Modeling")
        conn_mgr.setQuery(q_pls_modeling)
        #q_pls_modeling.getFilters().remove(filterModel)


        colnames_exclude = []
        for c in q_pls_modeling.getColumns():
            if c.getApprovedUsage() == 'None':                
                colnames_exclude.append(c.getName())

        for c in colnames_modeling:
            if c == 'EntityFunctionBoundary':
                continue
            if c in STD_TMPL_COLS:
                q_pls_modeling.removeColumn(c)
                continue
            elif c in STD_DS_COLS:
                q_pls_modeling.removeColumn(c)
                continue
            elif c in STD_CUSTOMER_DERIVED_COLS:
                q_pls_modeling.removeColumn(c)
                continue
            elif c in colnames_as:
                q_pls_modeling.removeColumn(c)
                continue
            elif c in colnames_dc:
                q_pls_modeling.removeColumn(c)
                continue
            elif c in colnames_exclude and c not in STD_REPORT_COLS:
                print 'ApprovedUsage=="None" for "{0}"'.format(c)
                q_pls_modeling.removeColumn(c)
                continue

        if template_type == 'SFDC':
            adjustQuerySFDC(conn_mgr, q_pls_modeling, filterModel)
        elif template_type == 'MKTO':
            adjustQueryMKTO(conn_mgr, q_pls_modeling, filterModel)
        elif template_type == 'ELQ':
            adjustQueryELQ(conn_mgr, q_pls_modeling, filterModel)
        else:
            print 'Unsupported template type \'{0}\'; skipping'.format(template_type)
            continue


        addDataProviderLP2ModelingData(conn_mgr)
        addCreateAnalyticPlayWithArchive(conn_mgr, t)
        conn_mgr.getLoadGroupMgr().commit()

        dt = datetime.datetime.now()

        writeQueryToCSV(conn_mgr, t, 'Q_LP3_ScoringLead', dt)
        #writeQueryToCSV(conn_mgr, t, 'Q_LP3_ModelingLead_AllRows', dt)
        #writeQueryToCSV(conn_mgr, t, 'Q_LP3_ModelingLead_OneLeadPerDomain', dt)

        #executeModelBuild(conn_mgr)


def adjustQuerySFDC(conn_mgr, q_pls_modeling, filterModel):
    cols = []
    cols.append(QueryColumnVDBImpl('City', ExpressionVDBImplFactory.parse('SFDC_City')))
    cols.append(QueryColumnVDBImpl('State', ExpressionVDBImplFactory.parse('SFDC_State')))
    cols.append(QueryColumnVDBImpl('Country', ExpressionVDBImplFactory.parse('SFDC_Country')))
    cols.append(QueryColumnVDBImpl('Title', ExpressionVDBImplFactory.parse('SFDC_Title')))
    cols.append(QueryColumnVDBImpl('PhoneNumber', ExpressionVDBImplFactory.parse('SFDC_Phone')))
    cols.append(QueryColumnVDBImpl('Industry', ExpressionVDBImplFactory.parse('SFDC_Industry')))
    cols.append(QueryColumnVDBImpl('IsClosed', ExpressionVDBImplFactory.parse('SFDC_Opportunity.IsClosed')))
    cols.append(QueryColumnVDBImpl('StageName', ExpressionVDBImplFactory.parse('SFDC_Opportunity.StageName')))
    entityname = 'SFDC_Lead_Contact_ID'
    mapTable = 'Map_Lead_Contact_ID_PropDataID'
    colPropDataID = 'SFDC_Lead_Contact_PropDataID'
    
    if 'LeadSource' not in q_pls_modeling.getColumnNames():
        q_pls_modeling.renameColumn('From_SFDC_LeadSource', 'LeadSource')
    else:
        q_pls_modeling.removeColumn('From_SFDC_LeadSource')

    if 'AnnualRevenue' not in q_pls_modeling.getColumnNames():
        q_pls_modeling.renameColumn('From_SFDC_AnnualRevenue', 'AnnualRevenue')
    else:
        q_pls_modeling.removeColumn('From_SFDC_AnnualRevenue')

    if 'CompanySize' not in q_pls_modeling.getColumnNames():
        q_pls_modeling.renameColumn('From_SFDC_CompanySize', 'CompanySize')
    else:
        q_pls_modeling.removeColumn('From_SFDC_CompanySize')

    adjustQueryCommon(conn_mgr, q_pls_modeling, entityname, mapTable, colPropDataID, cols, filterModel)


def adjustQueryMKTO(conn_mgr, q_pls_modeling, filterModel):
    cols = []
    cols.append(QueryColumnVDBImpl('City', ExpressionVDBImplFactory.parse('MKTO_LeadRecord.City')))
    cols.append(QueryColumnVDBImpl('State', ExpressionVDBImplFactory.parse('MKTO_LeadRecord.State')))
    cols.append(QueryColumnVDBImpl('Country', ExpressionVDBImplFactory.parse('MKTO_LeadRecord.Country')))
    cols.append(QueryColumnVDBImpl('Title', ExpressionVDBImplFactory.parse('MKTO_LeadRecord.Title')))
    cols.append(QueryColumnVDBImpl('PhoneNumber', ExpressionVDBImplFactory.parse('MKTO_LeadRecord.Phone')))
    cols.append(QueryColumnVDBImpl('Industry', ExpressionVDBImplFactory.parse('MKTO_LeadRecord.Industry')))
    cols.append(QueryColumnVDBImpl('IsClosed', ExpressionVDBImplFactory.parse('SFDC_Opportunity.IsClosed')))
    cols.append(QueryColumnVDBImpl('StageName', ExpressionVDBImplFactory.parse('SFDC_Opportunity.StageName')))
    entityname = 'MKTO_LeadRecord_ID'
    mapTable = 'Map_LeadID_PropDataID'
    colPropDataID = 'MKTO_LeadRecord_PropDataID'
    
    adjustQueryCommon(conn_mgr, q_pls_modeling, entityname, mapTable, colPropDataID, cols, filterModel)


def adjustQueryELQ(conn_mgr, q_pls_modeling, filterModel):
    cols = []
    cols.append(QueryColumnVDBImpl('City', ExpressionVDBImplFactory.parse('ELQ_Contact.C_City')))
    cols.append(QueryColumnVDBImpl('State', ExpressionVDBImplFactory.parse('ELQ_Contact.C_State_Prov')))
    cols.append(QueryColumnVDBImpl('Country', ExpressionVDBImplFactory.parse('ELQ_Contact.C_Country')))
    cols.append(QueryColumnVDBImpl('Title', ExpressionVDBImplFactory.parse('ELQ_Contact.C_Title')))
    cols.append(QueryColumnVDBImpl('PhoneNumber', ExpressionVDBImplFactory.parse('ELQ_Contact.C_BusPhone')))
    cols.append(QueryColumnVDBImpl('Industry', ExpressionVDBImplFactory.parse('ELQ_Contact.C_Industry1')))
    cols.append(QueryColumnVDBImpl('IsClosed', ExpressionVDBImplFactory.parse('SFDC_Opportunity.IsClosed')))
    cols.append(QueryColumnVDBImpl('StageName', ExpressionVDBImplFactory.parse('SFDC_Opportunity.StageName')))
    entityname = 'ELQ_Contact_ContactID'
    mapTable = 'Map_ContactID_PropDataID'
    colPropDataID = 'ELQ_Contact_PropDataID'
    
    adjustQueryCommon(conn_mgr, q_pls_modeling, entityname, mapTable, colPropDataID, cols, filterModel)


def adjustQueryCommon(conn_mgr, q_pls_modeling, entityname, mapTable, colPropDataID, cols, filterModel):

    for c in cols:
        c.setApprovedUsage("None")
        q_pls_modeling.appendColumn(c)

    q_pls_modeling.renameColumn('P1_Event', 'Event')
    q_pls_modeling.renameColumn('CreationDate', 'CreatedDate')
    q_pls_modeling.renameColumn('LeadID', 'Id')
    q_pls_modeling.renameColumn('Company', 'CompanyName')

    #q_pls_modeling.getFilters().append(filterModel)
    q_pls_modeling.setName("Q_LP3_ModelingLead_OneLeadPerDomain")
    conn_mgr.setQuery(q_pls_modeling)

    sep = ''
    lasmspec = ''
    for f in q_pls_modeling.getFilters():
        lasmspec += sep+f.definition()
        sep = ', '

    #q_pls_modeling.getFilters().remove(filterModel)

    filterSample = QueryFilterVDBImpl('LatticeAddressSetFcn(LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("Like"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("Right"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("ConvertToString"), LatticeFunctionExpressionAddressID(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("{0}"))))), LatticeFunctionExpressionConstant("1", DataTypeInt)), LatticeFunctionExpressionConstantScalar("0|5", DataTypeVarChar(3))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("{0}")))))'.format(entityname))

    #q_pls_modeling.getFilters().append(filterSample)
    q_pls_modeling.setName("Q_LP3_ScoringLead")
    conn_mgr.setQuery(q_pls_modeling)
    #q_pls_modeling.getFilters().remove(filterSample)

    filtersToRemove = []
    for f in q_pls_modeling.getFilters():
        c = re.search('.*?ContainerElementName\("SelectedForModeling_(SFDC|MKTO|ELQ)"\).*?', f.definition())
        if c:
            filtersToRemove.append(f)

    for f in filtersToRemove:
        q_pls_modeling.getFilters().remove(f)

    filterIsConsideredForModeling = QueryFilterVDBImpl('LatticeAddressSetFcn(LatticeFunctionIdentifier(ContainerElementName("AppData_Lead_IsConsideredForModeling")), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("{0}")))))'.format(entityname))
    filterAllInSample = QueryFilterVDBImpl('LatticeAddressSetIdentifier(ContainerElementName("LP3AllInSampleLeadsByPropDataID"))')

    q_pls_modeling.getFilters().append(filterIsConsideredForModeling)
    q_pls_modeling.getFilters().append(filterAllInSample)
    q_pls_modeling.setName("Q_LP3_ModelingLead_AllRows")
    #conn_mgr.setQuery(q_pls_modeling)

    filterAllInSampleSpec = 'SpecLatticeNamedElements((SpecLatticeNamedElement(SpecLatticeAliasDeclaration(\
                               LatticeAddressSetFcn(\
                                 LatticeFunctionExpression(\
                                   LatticeFunctionOperatorIdentifier("Equal")\
                                 , LatticeFunctionIdentifier(\
                                     ContainerElementNameTableQualifiedName(\
                                       LatticeSourceTableIdentifier(ContainerElementName("{0}"))\
                                     , ContainerElementName("{1}")\
                                     )\
                                   )\
                                 , LatticeFunctionExpressionTransform(\
                                     LatticeFunctionExpressionTransform(\
                                       LatticeFunctionIdentifier(\
                                         ContainerElementNameTableQualifiedName(\
                                           LatticeSourceTableIdentifier(ContainerElementName("{0}"))\
                                         , ContainerElementName("{1}")\
                                         )\
                                       )\
                                     , LatticeAddressSetMeet(({3}))\
                                     , FunctionAggregationOperator("Combine")\
                                     )\
                                   , LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("PropDataID"))))\
                                   , FunctionAggregationOperator("Combine")\
                                   )\
                                 )\
                               , LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("{2}"))))\
                               )\
                             ), ContainerElementName("LP3AllInSampleLeadsByPropDataID"))))'.format(mapTable, colPropDataID, entityname, lasmspec)
    #conn_mgr.setSpec('LP3AllInSampleLeadsByPropDataID', filterAllInSampleSpec)


def executeModelBuild(conn_mgr):
    launchid = conn_mgr.executeGroup('CreateAnalyticPlayWithArchive', 'mwilson@lattice-engines.com')
    print '  * Creating LP2 Model and Archiving Event Table: DL Launch ID \'{0}\''.format(launchid)


def addDataProviderLP2ModelingData(conn_mgr):

    dpxml = '<dataProvider name="SQL_LP2EventTables" autoMatch="False" connectionString="ServerName=le-spa;Authentication=SQL Server Authentication;User=dataloader_dep;Password=L@ttice1;Database=LP2EventTables;Schema=dbo;DateTimeOffsetOption=UtcDateTime;Timeout=100;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=2000;" dbType="2" usedFor="31" e="False"/>'
    conn_mgr.getLoadGroupMgr().createDataProvider(dpxml)


def addCreateAnalyticPlayWithArchive(conn_mgr, tenant):

    lgMgr = conn_mgr.getLoadGroupMgr()

    capxml = lgMgr.getLoadGroup('CreateAnalyticPlay')
    cap = etree.fromstring(capxml)
    cap.set('name', 'CreateAnalyticPlayWithArchive')
    cap.set('alias', 'CreateAnalyticPlayWithArchive')
    cap.set('createdBy', 'mwilson@lattice-engines.com')
    lgMgr.setLoadGroup(etree.tostring(cap))

    capwaTqxml = ' \
    <targetQueries> \
        <targetQuery w="Workspace" t="2" name="Q_LP3_Modeling" alias="Q_LP3_Modeling" isc="True" threshold="10000" fsTableName="{0}" sourceType="1" jobType="20" ignoreOptionsValue="0" exportToDestDirectly="True" exportRule="2" fileExt="bcp" rowTerminator="\\0\\r" columnTerminator="\\0" edts="False" destType="SQL" destDataProvider="SQL_LP2EventTables" cto="1"> \
          <schemas /> \
          <specs /> \
          <fsColumnMappings /> \
          <excludeColumns /> \
          <validationQueries /> \
          <constantRows /> \
          <fut dn="" d="" n="" iet="False" iets="False" t="1" /> \
        </targetQuery> \
      </targetQueries> \
    '.format(tenant)
    capwaTqsxml = ' \
      <targetQuerySequences> \
        <sequence w="Workspace" queryName="Q_LP3_Modeling" sequence="1" /> \
      </targetQuerySequences> \
    '

    lgMgr.setLoadGroupFunctionality('CreateAnalyticPlayWithArchive', capwaTqxml)
    lgMgr.setLoadGroupFunctionality('CreateAnalyticPlayWithArchive', capwaTqsxml)

    capwaLsxml = lgMgr.getLoadGroupFunctionality('CreateAnalyticPlayWithArchive', 'leadscroings')
    capwaLsxml = capwaLsxml.replace('Q_PLS_Modeling', 'Q_LP3_Modeling')
    lgMgr.setLoadGroupFunctionality('CreateAnalyticPlayWithArchive', capwaLsxml)


def writeQueryToCSV(conn_mgr, t, queryName, dt):

    print '  * Executing query \'{0}\''.format(queryName),
    queryResult = conn_mgr.executeQuery(queryName)

    while(not queryResult.isReady()):
        print '.',
        sys.stdout.flush()
        time.sleep(5)

    print '.'

    #print queryResult.queryHandle()

    csvFileName = queryName.replace('Q_',t+'_') + '_' + dt.strftime('%Y%m%d_%H%M%S') + '.csv'

    #with codecs.open(csvFileName, encoding = 'utf-8-sig', mode = 'w') as csvFile:
    with codecs.open(csvFileName, encoding = 'utf-8', mode = 'w') as csvFile:
        #csvFile.write(createUnicodeDelimited(queryResult.columnNames(), u',') + u'\r\n')
        csvFile.write(createUnicodeDelimited(queryResult.columnNames(), u',') + u'\n')

        while True:
            queryRows = queryResult.fetchMany(5000)
            if queryRows is None:
                break
            for row in queryRows:
                #csvFile.write(createUnicodeDelimited(row, u',') + u'\r\n')
                csvFile.write(createUnicodeDelimited(row, u',') + u'\n')


def createUnicodeDelimited(e, d):
    s = u''
    first_word = True
    for w in e:
        if type(w) is str:
            try:
                w = unicode(w)
            except:
                sys.stderr.write('le_unicode_csv.create_unicode_delim(): invalid character in string for this encoding, setting to NULL\n')
                w = u''
        if type(w) is bytearray:
            hex = ['0x']
            for aChar in w:
                hex.append('{0:02X}'.format(aChar))
            w = ''.join(hex).strip()
        has_quote = False
        if (w is not None) and (u'"' in unicode(w)):
            w = w.replace('"','""')
            has_quote = True
        if (w is not None) and (u'\n' in unicode(w)):
            has_quote = True
        if (w is not None) and (d in unicode(w) or has_quote):
            w = u'"' + w + u'"'
        if(not first_word):
            s += d
        if type(w) is bool:
            if w is True:
                s += u'1'
            else:
                s += u'0'
        elif w is not None:
            s += unicode(w)
        first_word = False
    return s


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

    generateLeadDatasets(tenants)
