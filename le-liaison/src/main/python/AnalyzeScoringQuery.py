#!/usr/bin/python

import sys, datetime, requests, re
from copy import deepcopy
from liaison import *

def AnalyzeScoringQuery( tenantFileName, queryReportName ):
    
    standard_tmpl_cols = ['LeadID','ModelingID','Email','CreationDate','Company','LastName','FirstName','P1_Event','Model_GUID']
    standard_ds_cols = ['Website_Age_Months','Uses_Public_Email_Provider','CompanyName_Length','Domain_Length'
                       ,'Title_Length','Title_Level','RelatedLinks_Count','FundingStage1'
                       ,'JobsTrendString1','ModelAction1','Title_IsAcademic','FirstName_SameAs_LastName'
                       ,'CompanyName_Entropy','Phone_Entropy','Domain_IsClient','Industry_Group'
                       ,'Title_IsTechRelated','SpamIndicator']
    standard_activity_cols = ['Activity_Count_Click_Email','Activity_Count_Click_Link','Activity_Count_Email_Bounced_Soft'
                             ,'Activity_Count_Fill_Out_Form','Activity_Count_Interesting_Moment_Any'
                             ,'Activity_Count_Open_Email','Activity_Count_Unsubscribe_Email','Activity_Count_Visit_Webpage'
                             ,'Activity_Count_Interesting_Moment_Email'
                             ,'Activity_Count_Interesting_Moment_Event'
                             ,'Activity_Count_Interesting_Moment_Multiple'
                             ,'Activity_Count_Interesting_Moment_Pricing'
                             ,'Activity_Count_Interesting_Moment_Search'
                             ,'Activity_Count_Interesting_Moment_Webinar'
                             ,'Activity_Count_Interesting_Moment_key_web_page']
    standard_customer_cols = ['Number_of_Contacts_for_Domain','From_SFDC_AnnualRevenue'
                             ,'From_SFDC_CompanySize','From_SFDC_LeadSource'
                             ,'Most_Recent_Lead_Source','From_Eloqua_Annual_Revenue'
                             ,'From_Eloqua_Company_Size','From_Eloqua_Job_Role'
                             ,'From_Eloqua_Lead_Source_Orig','From_Eloqua_Product_Solution_of_Interest'
                             ,'From_Eloqua_SFDC_Email_Opt_Out']

    tenants = []
    
    with open( tenantFileName ) as tenantFile:
        for line in tenantFile:
            cols = line.strip().split(',')
            tenants.append(cols[0])

    with open( queryReportName, mode = 'w' ) as queryReport:

        queryReport.write( 'TenantName,N_DerivedColumns,N_AlexaSource,N_DSCols,N_CustomerCols,N_Activity,N_ExtraSourceCols,ExtraTables,N_CustomCols,CustomColums\n' )
        
        for t in tenants:

            print '{0}...'.format( t )

            try:
                conn_mgr = ConnectionMgrFactory.Create( 'visiDB', tenant_name=t )
            except TenantNotMappedToURL:
                print 'Not on LP DataLoader'
                continue
            
            try:
                q_scoring = conn_mgr.getQuery('Q_PLS_Scoring_Incremental')
                cols_scoring = deepcopy(q_scoring.getColumnNames())
                colobj_scoring = q_scoring.getColumns()
                t_derivedcolumns = conn_mgr.getTable('PD_DerivedColumns')
                cols_dc = t_derivedcolumns.getColumnNames()
            except UnknownVisiDBSpec:
                print 'Not an initialized LP tenant'
                continue

            cols_as = []
            try:
                t_alexasource = conn_mgr.getTable('PD_Alexa_Source')
                cols_as = t_alexasource.getColumnNames()
            except UnknownVisiDBSpec:
                pass

            n_extra = 0
            extratables = set()

            for colobj in colobj_scoring:
                if type(colobj.getExpression()) is ExpressionVDBImplColRef:
                    tablename = colobj.getExpression().TableName()
                    if tablename not in ['PD_DerivedColumns','PD_Alexa_Source']:
                        n_extra += 1
                        extratables.add(colobj.getExpression().TableName())
                        cols_scoring.remove(colobj.getName())

            n_ldc = 0
            n_alexasource = 0
            n_ds = 0
            n_customer = 0
            n_activity = 0
            n_custom = 0
            cols_custom = []

            for c in cols_scoring:
                if c == 'EntityFunctionBoundary':
                    continue
                if c in standard_tmpl_cols:
                    continue
                elif c in cols_dc:
                    n_ldc += 1
                    continue
                elif c in cols_as:
                    n_alexasource += 1
                    continue
                elif c in standard_ds_cols:
                    n_ds += 1
                    continue
                elif c in standard_customer_cols:
                    n_customer += 1
                    continue
                elif c in standard_activity_cols:
                    n_activity += 1
                    continue
                else:
                    n_custom += 1
                    cols_custom.append(c)
            
            #queryReport.write( '{0},{1},{2},{3},{4},{5},{6},{7},{8},"'.format(t,n_ldc,n_alexasource,n_ds,n_customer,n_activity,n_extra,n_custom) )
            queryReport.write( '{0},{1},{2},{3},{4},{5},{6},"'.format(t,n_ldc,n_alexasource,n_ds,n_customer,n_activity,n_extra) )
            sep = ''
            for t in extratables:
                queryReport.write( '{0}{1}'.format(sep,t) )
                sep = ','
            queryReport.write('",')

            queryReport.write( '{0},"'.format(n_custom) )
            sep = ''
            for c in cols_custom:
                queryReport.write( '{0}{1}'.format(sep,c) )
                sep = ','
            queryReport.write('"\n')


def Usage( cmd, exit_code ):
    path = ''
    i = cmd.rfind('\\')
    if( i != -1 ):
        path = cmd[:i]
        cmd = cmd[i+1:]
    
    print ''
    print 'Usage: {0} <tenant_list.csv> <query_report.csv>'.format( cmd )
    print ''
    
    exit( exit_code )


if __name__ == "__main__":

    if len(sys.argv) == 1:
        Usage( sys.argv[0], 0 )
    
    if len(sys.argv) != 3:
        Usage( sys.argv[0], 1 )

    tenantFileName = sys.argv[1]
    queryReportName = sys.argv[2]
    
    AnalyzeScoringQuery( tenantFileName, queryReportName )
