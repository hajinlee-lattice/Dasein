import './import.utils';
import { actions, reducer } from './import.redux';

angular
.module('lp.import', [
    'common.wizard',
    'lp.import.entry',
    'lp.import.calendar',
    'lp.import.wizard.thirdpartyids',
    'lp.import.wizard.latticefields',
    'lp.import.wizard.matchtoaccounts',
    'lp.import.wizard.jobstatus',
    'lp.import.wizard.customfields',
    'lp.import.wizard.accountids',
    'lp.import.wizard.contactids',
    'lp.import.wizard.transactionids',
    'lp.import.wizard.productids',
    'lp.import.wizard.producthierarchyids',
    'lp.import.wizard.producthierarchy',
    'lp.import.wizard.validatetemplate',
    'lp.import.utils',
    'mainApp.core.utilities.AuthorizationUtility',
    'mainApp.core.redux'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.import', {
            url: '/import',
            onEnter: ['$state', 'AuthorizationUtility', 'FeatureFlagService', 'ReduxService', function($state, AuthorizationUtility, FeatureFlagService, ReduxService) {
                var flags = FeatureFlagService.Flags();
                var featureFlagsConfig = {};
                featureFlagsConfig[flags.VDB_MIGRATION] = false;
                featureFlagsConfig[flags.ENABLE_FILE_IMPORT] = true;
                
                AuthorizationUtility.redirectIfNotAuthorized(AuthorizationUtility.excludeExternalUser, featureFlagsConfig, 'home');
                ReduxService.connect(
                    'fieldMappings',
                    actions,
                    reducer,
                    $state.get('home.import')
                );
            }],
            onExit: ['$state', function($state){
                $state.get('home.import').data.redux.unsubscribe();
            }],
            redirectTo: 'home.import.entry.accounts'
        })
        .state('home.import.calendar', {
            url: '/calendar',
            onEnter: function(ImportWizardStore){
                //ImportWizardStore.clear();
            },
            resolve: {
                FieldDocument: function($q, ImportWizardService, ImportWizardStore) {
                    return false;
                    var deferred = $q.defer();
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                Calendar: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardStore.getCalendar().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                DateRange: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer(),
                        year = new Date().getFullYear();

                    ImportWizardService.getDateRange(year).then(function(result){
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'main@': {
                    controller: 'ImportWizardCalendar',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/calendar/calendar.component.html'
                }
            }
        })
        .state('home.import.s3', {
            url:'/s3',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Data Processing & Analysis'
            },
            resolve: {
                path: () => {
                    return 'files';
                },
                ngservices: (ImportWizardStore) => {
                    let obj = {
                        ImportWizardStore: ImportWizardStore
                    }
                    return obj;
                }
            },
            views: {
                'main@': {
                    component: 'reactAngularMainComponent'
                }
            }
        })
        .state('home.import.entry', {
            url: '/entry',
            params: {
                action: null,
                type: null,
                data: null
            },
            resolve:{
                DateSupport : function($state, $stateParams){
                    let redux = $state.get('home.import').data.redux;
                    redux.fetchDateSupport();
                }
            },
            views: {
                'main@': {
                    templateUrl: 'app/import/entry/entry.component.html'
                }
            },
            redirectTo: 'home.import.entry.accounts'
        })
        .state('home.import.entry.accounts', {
            url: '/accounts',
            onEnter: function($state, ImportWizardStore){
                ImportWizardStore.fieldDocument = {};
            },
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Data Processing & Analysis'
            },
            views: {
                'entry_content@home.import.entry': 'accountsContent'
            }
        })
        .state('home.import.entry.contacts', {
            url: '/contacts',
            onEnter: function(ImportWizardStore){
                ImportWizardStore.fieldDocument = {};
            },
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Data Processing & Analysis'
            },
            views: {
                'entry_content@home.import.entry': 'contactsContent'
            }
        })
        .state('home.import.entry.productpurchases', {
            url: '/productpurchases',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Data Processing & Analysis'
            },
            views: {
                'entry_content@home.import.entry': 'productPurchasesContent'
            }
        })
        .state('home.import.entry.productbundles', {
            url: '/productbundles',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Data Processing & Analysis'
            },
            views: {
                'entry_content@home.import.entry': 'productBundlesContent'
            }
        })
        .state('home.import.entry.producthierarchy', {
            url: '/producthierarchy',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Data Processing & Analysis'
            },
            views: {
                'entry_content@home.import.entry': 'productHierarchyContent'
            }
        })
        .state('home.import.data', {
            url: '/data',
            resolve: {
                WizardHeaderTitle: function() {
                    return 'Import';
                },
                WizardContainerId: function() {
                    return 'data-import';
                }
            },
            views: {
                'main@': {
                    controller: 'ImportWizard',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/wizard.component.html'
                }
            }
        })
        .state('home.import.data.accounts', {
            url: '/accounts',
            resolve: {
                WizardValidationStore: function(ImportWizardStore) {
                    return ImportWizardStore;
                },
                WizardProgressContext: function() {
                    return 'import.data';
                },
                WizardProgressItems: function($stateParams, ImportWizardStore) {                    
                    var entityType = ImportWizardStore.getEntityType(),
                        wizard_steps = $stateParams.wizard_steps || (entityType || '').toLowerCase();

                    return ImportWizardStore.getWizardProgressItems(wizard_steps || 'account');
                },
                WizardControlsOptions: function() {
                    return { 
                        backState: 'home.import.entry.accounts', 
                        nextState: 'home.importtemplates' 
                    };
                }
            },
            views: {
                'wizard_progress': {                    
                    controller: 'ImportWizardProgress',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/progress/progress.component.html'
                },
                'wizard_controls': {
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            }
        })
        .state('home.import.data.accounts.ids', {
            url: '/accountids',
            onEnter: function($state, ImportWizardStore, FieldDocument){
                ImportWizardStore.setSavedDocumentInState('home.import.data.accounts', FieldDocument.fieldMappings);
                ImportWizardStore.setSavedDocumentInState('home.import.data.accounts.ids', FieldDocument.fieldMappings);
            },
            resolve: {
                FieldDocument: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType(), null, ImportWizardStore.getFeedType()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardService.GetSchemaToLatticeFields(null, ImportWizardStore.getEntityType(), ImportWizardStore.getFeedType()).then(function(result) {
                        deferred.resolve(result['Account']);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardAccountIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/accountids/accountids.component.html'
                }
            }
        })
        .state('home.import.data.accounts.ids.thirdpartyids', {
            url: '/thirdpartyids',
            resolve: {
                Identifiers: function() {
                    return [
                        { name: 'CRM ID', value: '' },
                        { name: 'MAP ID', value: '' }
                    ];
                },
                FieldDocument: function($q, ImportWizardStore) {
                    return ImportWizardStore.getFieldDocument();
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardThirdPartyIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/thirdpartyids/thirdpartyids.component.html'
                }
            }
        })
        .state('home.import.data.accounts.ids.thirdpartyids.latticefields', {
            url: '/latticefields',
            onEnter: function($state, ImportWizardStore, $transition$){
                var from = $transition$._targetState._definition.parent.name;
                if(from.includes('thirdpartyids')){
                    // ImportWizardStore.removeFromState('home.import.data.accounts.ids.thirdpartyids.latticefields');
                    var copy = ImportWizardStore.getSavedDocumentCopy('home.import.data.accounts.ids.thirdpartyids');
                    ImportWizardStore.setSavedDocumentInState('home.import.data.accounts.ids.thirdpartyids.latticefields', copy);
                }
            },
            resolve: {
                FieldDocument: function($q, ImportWizardStore) {
                    return ImportWizardStore.getFieldDocument();
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    return ImportWizardStore.getUnmappedFields();
                },
                Type: function(){
                    return "Account";
                },
                MatchingFields: function() {
                    return [
                        { name: 'Website' },
                        { name: 'DUNS', displayName: 'D-U-N-S' },
                        { name: 'CompanyName', displayName: 'Company Name' },
                        { name: 'PhoneNumber', displayName: 'Phone Number'},
                        { name: 'City' },
                        { name: 'Country' },
                        { name: 'State' },
                        { name: 'PostalCode', displayName: 'Postal Code'}
                    ];
                },
                AnalysisFields: function() {
                    return [
                        { name: 'Type', type: 'TEXT' },
                        { name: 'Industry', type: 'TEXT' },
                        { name: 'SpendAnalyticsSegment', displayName: 'Account Business Segment',type: 'TEXT' },
                        { name: 'AnnualRevenue', displayName: 'Estimated Yearly Revenue', type: 'TEXT' },
                        { name: 'Longitude', type: 'TEXT' },
                        { name: 'AnnualRevenueCurrency', displayName: 'Estimated Yearly Revenue Currency', type: 'TEXT' },
                        { name: 'Latitude', type: 'TEXT' }
                        //{ name: 'NumberOfEmployees', displayName: 'Number Of Employees' },
                    ];
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardLatticeFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/latticefields/latticefields.component.html'
                }
            }
        })
        .state('home.import.data.accounts.ids.thirdpartyids.latticefields.customfields', {
            url: '/customfields',
            onExit: function($transition$, ImportWizardStore){
                ImportWizardStore.setIgnore([]);
                var to = $transition$._targetState._definition.name;
                if(to === 'home.import.data.accounts.ids.thirdpartyids.latticefields'){
                    ImportWizardStore.removeFromState('home.import.data.accounts.ids.thirdpartyids.latticefields.customfields');
                }
                
            },
            resolve: {
                FieldDocument: function($q, ImportWizardStore) {
                    return ImportWizardStore.getFieldDocument();
                },
                mergedFieldDocument: function($q, ImportWizardStore) {
                    return ImportWizardStore.mergeFieldDocument({segment: true, save: false});
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardCustomFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/customfields/custom-fields.component.html'
                }
            }
        })
        .state('home.import.data.accounts.ids.thirdpartyids.latticefields.customfields.validation', {
            url: '/validatetemplate',
            resolve: {
                FileName: function (ImportWizardStore) {
                    return ImportWizardStore.getCsvFileName();
                },
                FieldDocument: function(ImportWizardStore) {
                    return ImportWizardStore.getFieldDocument();
                },
                TemplateData: function(ImportWizardStore) {
                    return ImportWizardStore.getTemplateData();
                },
                Validation: function($q, ImportWizardService, FileName, FieldDocument, TemplateData) {
                    var deferred = $q.defer();

                    ImportWizardService.validateTemplate(FileName, TemplateData, FieldDocument).then(function(result) {
                        console.log(result);
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardValidateTemplate',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/validatetemplate/validatetemplate.component.html'
                }
            }
        })
        .state('home.import.data.accounts.ids.thirdpartyids.latticefields.customfields.validation.jobstatus', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardJobStatus',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        })
        .state('home.import.data.contacts', {
            url: '/contacts',
            resolve: {
                WizardValidationStore: function(ImportWizardStore) {
                    return ImportWizardStore;
                },
                WizardProgressContext: function() {
                    return 'import.data';
                },
                WizardProgressItems: function($stateParams, ImportWizardStore) {
                    var entityType = ImportWizardStore.getEntityType(),
                        wizard_steps = $stateParams.wizard_steps || entityType.toLowerCase();

                    return ImportWizardStore.getWizardProgressItems(wizard_steps || 'contact');
                },
                WizardControlsOptions: function() {
                    return { 
                        backState: 'home.import.entry.contacts', 
                        nextState: 'home.importtemplates' 
                    };
                }
            },
            views: {
                'wizard_progress': {                    
                    controller: 'ImportWizardProgress',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/progress/progress.component.html'
                },
                'wizard_controls': {
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            }
        })
        .state('home.import.data.contacts.ids', {
            url: '/contactids',
            onEnter: function($state, ImportWizardStore, FieldDocument){
                ImportWizardStore.setSavedDocumentInState('home.import.data.contacts', FieldDocument.fieldMappings);
                ImportWizardStore.setSavedDocumentInState('home.import.data.contacts.ids', FieldDocument.fieldMappings);
                // ImportWizardStore.removeFromState('home.import.data.contacts.ids');
            },
            resolve: {
                FieldDocument: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();
                    
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType(), null, ImportWizardStore.getFeedType()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardService.GetSchemaToLatticeFields(null, ImportWizardStore.getEntityType(), ImportWizardStore.getFeedType()).then(function(result) {
                        deferred.resolve(result['Account']);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardContactIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/contactids/contactids.component.html'
                }
            }
        })
        .state('home.import.data.contacts.ids.thirdpartyids', {
            url: '/thirdpartyids',
            resolve: {
                Identifiers: function() {
                    return [
                        { name: 'CRM ID', value: '' },
                        { name: 'MAP ID', value: '' }
                    ];
                },
                FieldDocument: function($q, ImportWizardStore) {
                    return ImportWizardStore.getFieldDocument();
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardThirdPartyIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/thirdpartyids/thirdpartyids.component.html'
                }
            }
        })
        .state('home.import.data.contacts.ids.thirdpartyids.latticefields', {
            url: '/latticefields',
            onExit: function($transition$, ImportWizardStore){
                ImportWizardStore.setIgnore([]);
                var to = $transition$._targetState._definition.name;
                if(to === 'home.import.data.contacts.ids'){
                    ImportWizardStore.removeFromState('home.import.data.contacts.ids.latticefields');
                }
                
            },
            resolve: {
                FieldDocument: function($q, ImportWizardStore) {
                    let ret = ImportWizardStore.getFieldDocument();
                    //console.log('Field Document ', ret);
                    return ret;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    let ret = ImportWizardStore.getUnmappedFields();
                    //console.log('Unmapped fields ', ret);
                    return ret;
                },
                mergedFieldDocument: function($q, ImportWizardStore) {
                    let ret = ImportWizardStore.mergeFieldDocument({segment: true, save: false});
                    //console.log('Merged field ', ret);
                    return ret;
                },
                Type: function(){
                    return "Contacts";
                },
                MatchingFields: function() {
                    return [
                        //{ name: 'ContactName', displayName: 'Contact Name' },
                        { name: 'FirstName', displayName: 'First Name' },
                        { name: 'LastName', displayName: 'Last Name' },
                        { name: 'Title', displayName: '' },
                        { name: 'Email', displayName: '' }
                        // { name: 'CompanyName', displayName: 'Company Name' },
                        // { name: 'City', displayName: '' },
                        // { name: 'State', displayName: '' },
                        // { name: 'Country', displayName: '' },
                        // { name: 'PostalCode', displayName: 'Postal Code' },
                        // { name: 'DUNS', displayName: 'D-U-N-S' },
                        // { name: 'Website', displayName: '' },
                   ];
                },
                AnalysisFields: function(ImportWizardStore, ImportUtils) {
                   let createdDate = ImportUtils.getFieldFromLaticeSchema(ImportWizardStore.getEntityType(), 'CreatedDate');
                   if(!createdDate){
                       createdDate = {
                            fromExistingTemplate: false,
                            fieldType: 'DATE'
                       }
                   }
                   let lastModifiedDate = ImportUtils.getFieldFromLaticeSchema(ImportWizardStore.getEntityType(), 'LastModifiedDate');
                   if(!lastModifiedDate){
                        lastModifiedDate = {
                            fromExistingTemplate: false,
                            fieldType: 'DATE'
                        }
                } 
                   return [
                        { name: 'LeadStatus', displayName: 'Lead Status', type: 'TEXT'},
                        { name: 'LeadSource', displayName: 'Lead Source', type: 'TEXT'},
                        { name: 'LeadType', displayName: 'Lead Type', type: 'TEXT'},
                        { 
                            name: 'CreatedDate', 
                            displayName: 'Created Date', 
                            type: createdDate.fieldType, 
                            fromExistingTemplate: createdDate.fromExistingTemplate,
                            dateFormatString : createdDate.fromExistingTemplate == true ? createdDate.dateFormatString : undefined,
                            timeFormatString: createdDate.fromExistingTemplate == true ? createdDate.timeFormatString: undefined,
                            timezone: createdDate.fromExistingTemplate == true ? createdDate.timezone : undefined
                        },
                        { 
                            name: 'LastModifiedDate', 
                            displayName: 'Last Modified Date', 
                            type: lastModifiedDate.fieldType, 
                            fromExistingTemplate: lastModifiedDate.fromExistingTemplate,
                            dateFormatString : lastModifiedDate.fromExistingTemplate == true ? lastModifiedDate.dateFormatString : undefined,
                            timeFormatString: lastModifiedDate.fromExistingTemplate == true ? lastModifiedDate.timeFormatString: undefined,
                            timezone: lastModifiedDate.fromExistingTemplate == true ? lastModifiedDate.timezone : undefined
                        },
                        { name: 'DoNotMail', displayName: 'Has Opted Out of Email',  type: 'TEXT' },
                        { name: 'DoNotCall', displayName: 'Has Opted Out of Phone Calls', type: 'TEXT' }
                    ];
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardLatticeFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/latticefields/latticefields.component.html'
                }
            }
        })
        .state('home.import.data.contacts.ids.thirdpartyids.latticefields.matchtoaccounts', {
            url: '/matchtoaccounts',
            onExit: function($transition$, ImportWizardStore){
                ImportWizardStore.setIgnore([]);
                var to = $transition$._targetState._definition.name;
                if(to === 'home.import.data.contacts.ids.latticefields'){
                    ImportWizardStore.removeFromState('home.import.data.contacts.ids.latticefields.matchtoaccounts');
                }
                
            },
            resolve: {
                FieldDocument: function($q, ImportWizardStore) {
                    return ImportWizardStore.getFieldDocument();
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    return ImportWizardStore.getUnmappedFields();
                },
                mergedFieldDocument: function($q, ImportWizardStore) {
                    return ImportWizardStore.mergeFieldDocument({segment: true, save: false});
                },
                MatchingFields: function() {
                    return [
                        // { name: 'Email', displayName: '' },
                        { name: 'Website', displayName: '' },
                        { name: 'CompanyName', displayName: 'Company Name' },
                        { name: 'DUNS', displayName: '' },
                        { name: 'City', displayName: '' },
                        { name: 'State', displayName: '' },
                        { name: 'Country', displayName: '' },
                        { name: 'PostalCode', displayName: 'Postal Code' },
                        { name: 'PhoneNumber', displayName: 'Phone Number' }
                   ];
                },
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardMatchToAccounts',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/matchtoaccounts/matchtoaccounts.component.html'
                }
            }
        })
        .state('home.import.data.contacts.ids.thirdpartyids.latticefields.matchtoaccounts.customfields', {
            url: '/customfields',
            onExit: function($transition$, ImportWizardStore){
                ImportWizardStore.setIgnore([]);
                var to = $transition$._targetState._definition.name;
                if(to === 'home.import.data.contacts.ids.latticefields.matchtoaccounts'){
                    ImportWizardStore.removeFromState('home.import.data.contacts.ids.latticefields.matchtoaccounts.customfields');
                    // ImportWizardStore.saveDocumentFields('home.import.data.contacts.ids.latticefields');
                }
            },
            resolve: {
                mergedFieldDocument: function($q, ImportWizardStore) {
                    return ImportWizardStore.mergeFieldDocument({segment: true, save: false});
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardCustomFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/customfields/custom-fields.component.html'
                }
            }
        })
        .state('home.import.data.contacts.ids.thirdpartyids.latticefields.matchtoaccounts.customfields.validation', {
            url: '/validatetemplate',
            resolve: {
                FileName: function (ImportWizardStore) {
                    return ImportWizardStore.getCsvFileName();
                },
                FieldDocument: function(ImportWizardStore) {
                    return ImportWizardStore.getFieldDocument();
                },
                TemplateData: function(ImportWizardStore) {
                    return ImportWizardStore.getTemplateData();
                },
                Validation: function($q, ImportWizardService, FileName, FieldDocument, TemplateData) {
                    var deferred = $q.defer();

                    ImportWizardService.validateTemplate(FileName, TemplateData, FieldDocument).then(function(result) {
                        console.log(result);
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardValidateTemplate',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/validatetemplate/validatetemplate.component.html'
                }
            }
        })
        .state('home.import.data.contacts.ids.thirdpartyids.latticefields.matchtoaccounts.customfields.validation.jobstatus', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardJobStatus',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        })
        .state('home.import.data.productpurchases', {
            url: '/productpurchases',
            resolve: {
                WizardValidationStore: function(ImportWizardStore) {
                    return ImportWizardStore;
                },
                WizardProgressContext: function() {
                    return 'import.data';
                },
                WizardProgressItems: function($stateParams, ImportWizardStore) {
                    var entityType = ImportWizardStore.getEntityType(),
                        wizard_steps = $stateParams.wizard_steps || entityType.toLowerCase();

                    return ImportWizardStore.getWizardProgressItems(wizard_steps || 'transaction');
                },
                WizardControlsOptions: function() {
                    return { 
                        backState: 'home.import.entry.productpurchases', 
                        nextState: 'home.importtemplates' 
                    };
                }
            },
            views: {
                'wizard_progress': {                    
                    controller: 'ImportWizardProgress',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/progress/progress.component.html'
                },
                'wizard_controls': {
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            },
            redirectTo: 'home.import.data.productpurchases.ids'
        })
        .state('home.import.data.productpurchases.ids', {
            url: '/transactionids',
            onEnter: function($state, ImportWizardStore, FieldDocument){
                ImportWizardStore.setSavedDocumentInState('home.import.data.productpurchases', FieldDocument.fieldMappings);
                ImportWizardStore.setSavedDocumentInState('home.import.data.productpurchases.ids', FieldDocument.fieldMappings);
            },
            resolve: {
                FieldDocument: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType(), null, ImportWizardStore.getFeedType()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardService.GetSchemaToLatticeFields(null, ImportWizardStore.getEntityType(), ImportWizardStore.getFeedType()).then(function(result) {
                        deferred.resolve(result['Account']);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardTransactionIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/transactionids/transactionids.component.html'
                }
            }
        })
        .state('home.import.data.productpurchases.ids.latticefields', {
            url: '/latticefields',
            resolve: {
                FieldDocument: function($q, ImportWizardStore) {
                    return ImportWizardStore.getFieldDocument();
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    return ImportWizardStore.getUnmappedFields();
                },
                Type: function(){
                    return "Transactions";
                },
                MatchingFields: function() {
                    return [
                        { name: 'TransactionTime', displayName: 'Transaction Date', required: true },
                        { name: 'Amount', required: true },
                        { name: 'Quantity', required: true },
                        { name: 'Cost', }
                        //{ name: 'TransactionType', displayName: 'Transaction Type' },
                    ];
                },
                AnalysisFields: function() {
                    return [];
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardLatticeFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/latticefields/latticefields.component.html'
                }
            }
        })
        .state('home.import.data.productpurchases.ids.latticefields.jobstatus', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardJobStatus',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        })
        .state('home.import.data.productbundles', {
            url: '/productbundles',
            resolve: {
                WizardValidationStore: function(ImportWizardStore) {
                    return ImportWizardStore;
                },
                WizardProgressContext: function() {
                    return 'import.data';
                },
                WizardProgressItems: function($stateParams, ImportWizardStore) {
                    var entityType = ImportWizardStore.getEntityType(),
                        wizard_steps = $stateParams.wizard_steps || entityType.toLowerCase();

                    return ImportWizardStore.getWizardProgressItems(wizard_steps || 'product');
                },
                WizardControlsOptions: function() {
                    return { 
                        backState: 'home.import.entry.productbundles', 
                        nextState: 'home.importtemplates' 
                    };
                }
            },
            views: {
                'wizard_progress': {                    
                    controller: 'ImportWizardProgress',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/progress/progress.component.html'
                },
                'wizard_controls': {
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            },
            redirectTo: 'home.import.data.productbundles.ids'
        })
        .state('home.import.data.productbundles.ids', {
            url: '/transactionids',
            onEnter: function($state, ImportWizardStore, FieldDocument){
                ImportWizardStore.setSavedDocumentInState('home.import.data.productbundles', FieldDocument.fieldMappings);
                ImportWizardStore.setSavedDocumentInState('home.import.data.productbundles.ids', FieldDocument.fieldMappings);
            },
            resolve: {
                FieldDocument: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType(), null, ImportWizardStore.getFeedType()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardService.GetSchemaToLatticeFields(null, ImportWizardStore.getEntityType(), ImportWizardStore.getFeedType()).then(function(result) {
                        deferred.resolve(result['Account']);
                    });

                    return deferred.promise;
                },
                Calendar: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardStore.getCalendar().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardProductIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/productids/productids.component.html'
                }
            }
        })
        .state('home.import.data.productbundles.ids.latticefields', {
            url: '/latticefields',
            resolve: {
                FieldDocument: function($q, ImportWizardStore) {
                    return ImportWizardStore.getFieldDocument();
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    return ImportWizardStore.getUnmappedFields();
                },
                Type: function(){
                    return "Products";
                },
                MatchingFields: function() {
                    return [
                        { name: 'ProductBundle', displayName: 'Product Bundle Name' },
                        { name: 'Description', displayName: 'Product Description' }
                        // { name: 'ProductName', displayName: 'Product Name' },
                        // { name: 'ProductFamily', displayName: 'Product Family' },
                    ];
                },
                AnalysisFields: function() {
                    return [];
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardLatticeFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/latticefields/latticefields.component.html'
                }
            }
        })
        .state('home.import.data.productbundles.ids.latticefields.jobstatus', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardJobStatus',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        })
        .state('home.import.data.producthierarchy', {
            url: '/producthierarchy',
            params: {
                wizard_steps: 'producthierarchy'  // use this to override entity type as default for wizard step key
            },
            resolve: {
                WizardValidationStore: function(ImportWizardStore) {
                    return ImportWizardStore;
                },
                WizardProgressContext: function() {
                    return 'import.data';
                },
                WizardProgressItems: function($stateParams, ImportWizardStore) {
                    var entityType = ImportWizardStore.getEntityType(),
                        wizard_steps = $stateParams.wizard_steps || entityType.toLowerCase();

                    return ImportWizardStore.getWizardProgressItems(wizard_steps || 'producthierarchy');
                },
                WizardControlsOptions: function() {
                    return { backState: 'home.import.entry.producthierarchy', nextState: 'home.importtemplates' };
                }
            },
            views: {
                'wizard_progress': {
                    controller: 'ImportWizardProgress',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/progress/progress.component.html'
                },
                'wizard_controls': {
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            },
            redirectTo: 'home.import.data.producthierarchy.ids'
        })
        .state('home.import.data.producthierarchy.ids', {
            url: '/producthierarchyids',
            onEnter: function($state, ImportWizardStore, FieldDocument){
                ImportWizardStore.setSavedDocumentInState('home.import.data.producthierarchy', FieldDocument.fieldMappings);
                ImportWizardStore.setSavedDocumentInState('home.import.data.producthierarchy.ids', FieldDocument.fieldMappings);
            },
            resolve: {
                FieldDocument: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType(), null, ImportWizardStore.getFeedType()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardService.GetSchemaToLatticeFields(null, ImportWizardStore.getEntityType(), ImportWizardStore.getFeedType()).then(function(result) {
                        deferred.resolve(result['Account']);
                    });

                    return deferred.promise;
                },
                Calendar: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardStore.getCalendar().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardProductHierarchyIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/producthierarchyids/producthierarchyids.component.html'
                }
            }
        })
        .state('home.import.data.producthierarchy.ids.producthierarchy', {
            url: '/producthierarchy',
            resolve: {
                FieldDocument: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType(), null, ImportWizardStore.getFeedType()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardProductHierarchy',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/producthierarchy/producthierarchy.component.html'
                }
            }
        })
        .state('home.import.data.producthierarchy.ids.producthierarchy.jobstatus', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardJobStatus',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        });
});