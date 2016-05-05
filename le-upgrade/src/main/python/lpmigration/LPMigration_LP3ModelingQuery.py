
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import os, sys
from copy import deepcopy
from liaison import *
from appsequence import Applicability, AppSequence, StepBase

class LPMigration_LP3ModelingQuery(StepBase):

    name        = 'LPMigration_LP3ModelingQuery'
    description = 'Adds query for extracting an event table for LP3 modeling.'
    version     = '$Rev$'

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


    def __init__(self, forceApply=False):
        super(LPMigration_LP3ModelingQuery, self).__init__(forceApply)


    def getApplicability(self, appseq):
        return Applicability.canApply


    def apply( self, appseq ):
        print '\n    * Installing query (Q_LP3_ModelingLead_OneLeadPerDomain) for LP3 modeling . .',
        sys.stdout.flush()

        conn_mgr = appseq.getConnectionMgr()
        template_type = appseq.getText('template_type')

        q_pls_modeling = conn_mgr.getQuery('Q_PLS_Modeling')
        colnames_modeling = deepcopy(q_pls_modeling.getColumnNames())
        t_derivedcolumns = conn_mgr.getTable('PD_DerivedColumns')
        colnames_dc = t_derivedcolumns.getColumnNames()

        colnames_as = []
        try:
            t_alexasource = conn_mgr.getTable('PD_Alexa_Source')
            colnames_as = t_alexasource.getColumnNames()
        except UnknownVisiDBSpec:
            pass

        colnames_exclude = []
        for c in q_pls_modeling.getColumns():
            if c.getApprovedUsage() == 'None':                
                colnames_exclude.append(c.getName())

        for c in colnames_modeling:
            if c == 'EntityFunctionBoundary':
                continue
            if c in self.STD_TMPL_COLS:
                q_pls_modeling.removeColumn(c)
                continue
            elif c in self.STD_DS_COLS:
                q_pls_modeling.removeColumn(c)
                continue
            elif c in self.STD_CUSTOMER_DERIVED_COLS:
                q_pls_modeling.removeColumn(c)
                continue
            elif c in colnames_as:
                q_pls_modeling.removeColumn(c)
                continue
            elif c in colnames_dc:
                q_pls_modeling.removeColumn(c)
                continue
            elif c in colnames_exclude and c not in self.STD_REPORT_COLS:
                print '\n      !! ApprovedUsage=="None" for "{0}"'.format(c),
                sys.stdout.flush()
                q_pls_modeling.removeColumn(c)
                continue

        if template_type == 'SFDC':
            self._adjustQuerySFDC(conn_mgr, q_pls_modeling)
        elif template_type == 'MKTO':
            self._adjustQueryMKTO(conn_mgr, q_pls_modeling)
        elif template_type == 'ELQ':
            self._adjustQueryELQ(conn_mgr, q_pls_modeling)
        else:
            print 'Unsupported template type \'{0}\'; skipping'.format(template_type)
            return False

        return True


    def _adjustQuerySFDC(self, conn_mgr, q_pls_modeling):
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

        self._adjustQueryCommon(conn_mgr, q_pls_modeling, cols)


    def _adjustQueryMKTO(self, conn_mgr, q_pls_modeling):
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

        self._adjustQueryCommon(conn_mgr, q_pls_modeling, cols)


    def _adjustQueryELQ(self, conn_mgr, q_pls_modeling):
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

        self._adjustQueryCommon(conn_mgr, q_pls_modeling, cols)


    def _adjustQueryCommon(self, conn_mgr, q_pls_modeling, cols):

        for c in cols:
            c.setApprovedUsage("None")
            q_pls_modeling.appendColumn(c)

        q_pls_modeling.renameColumn('P1_Event', 'Event')
        q_pls_modeling.renameColumn('CreationDate', 'CreatedDate')
        q_pls_modeling.renameColumn('LeadID', 'Id')
        q_pls_modeling.renameColumn('Company', 'CompanyName')

        q_pls_modeling.setName('Q_LP3_ModelingLead_OneLeadPerDomain')
        conn_mgr.setQuery(q_pls_modeling)
