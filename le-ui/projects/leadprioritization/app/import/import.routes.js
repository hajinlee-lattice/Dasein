angular
.module('lp.import', [
    'common.wizard',
    'lp.import.entry',
    'lp.import.calendar',
    'lp.import.wizard.thirdpartyids',
    'lp.import.wizard.latticefields',
    'lp.import.wizard.jobstatus',
    'lp.import.wizard.customfields',
    'lp.import.wizard.accountids',
    'lp.import.wizard.contactids',
    'lp.import.wizard.transactionids',
    'lp.import.wizard.productids',
    'lp.import.wizard.producthierarchyids',
    'lp.import.wizard.producthierarchy',
    'lp.import.utils',
    'mainApp.core.utilities.AuthorizationUtility'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.import', {
            url: '/import',
            onEnter: ['AuthorizationUtility', 'FeatureFlagService', function(AuthorizationUtility, FeatureFlagService) {
                var flags = FeatureFlagService.Flags();
                var featureFlagsConfig = {};
                featureFlagsConfig[flags.VDB_MIGRATION] = false;
                featureFlagsConfig[flags.ENABLE_FILE_IMPORT] = true;

                AuthorizationUtility.redirectIfNotAuthorized(AuthorizationUtility.excludeExternalUser, featureFlagsConfig, 'home');
            }],
            redirectTo: 'home.import.entry.accounts'
        })
        .state('home.import.calendar', {
            url: '/calendar',
            resolve: {
                FieldDocument: function($q, ImportWizardService, ImportWizardStore) {
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
        .state('home.import.entry', {
            url: '/entry',
            views: {
                'main@': {
                    templateUrl: 'app/import/entry/entry.component.html'
                }
            },
            redirectTo: 'home.import.entry.accounts'
        })
        .state('home.import.entry.accounts', {
            url: '/accounts',
            onEnter: function(ImportWizardStore){
                ImportWizardStore.fieldDocument = {};
            },
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'My Data'
            },
            views: {
                'entry_content@home.import.entry': {
                    templateUrl: 'app/import/entry/accounts/accounts.component.html'
                }
            }
        })
        .state('home.import.entry.contacts', {
            url: '/contacts',
            onEnter: function(ImportWizardStore){
                ImportWizardStore.fieldDocument = {};
            },
            views: {
                'entry_content@home.import.entry': {
                    templateUrl: 'app/import/entry/contacts/contacts.component.html'
                }
            }
        })
        .state('home.import.entry.product_purchases', {
            url: '/product_purchases',
            views: {
                'entry_content@home.import.entry': {
                    templateUrl: 'app/import/entry/productpurchases/productpurchases.component.html'
                }
            }
        })
        .state('home.import.entry.product_bundles', {
            url: '/product_bundles',
            views: {
                'entry_content@home.import.entry': {
                    templateUrl: 'app/import/entry/productbundles/productbundles.component.html'
                }
            }
        })
        .state('home.import.entry.product_hierarchy', {
            url: '/product_hierarchy',
            views: {
                'entry_content@home.import.entry': {
                    templateUrl: 'app/import/entry/producthierarchy/producthierarchy.component.html'
                }
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
                DisableWizardNavOnLastStep: function () {
                    return null;
                },
                WizardControlsOptions: function() {
                    return { 
                        backState: 'home.import.entry.accounts', 
                        nextState: 'home.jobs.data' 
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
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardService.GetSchemaToLatticeFields(null, ImportWizardStore.getEntityType()).then(function(result) {
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
                        { name: 'Type' },
                        { name: 'Industry' },
                        { name: 'SpendAnalyticsSegment', displayName: 'Account Business Segment' },
                        { name: 'AnnualRevenue', displayName: 'Estimated Yearly Revenue' },
                        { name: 'Longitude' },
                        { name: 'AnnualRevenueCurrency', displayName: 'Estimated Yearly Revenue Currency' },
                        { name: 'Latitude' }
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
                    templateUrl: 'app/import/content/customfields/customfields.component.html'
                }
            }
        })
        .state('home.import.data.accounts.ids.thirdpartyids.latticefields.customfields.jobstatus', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.data': {
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
                DisableWizardNavOnLastStep: function () {
                    return null;
                },
                WizardControlsOptions: function() {
                    return { backState: 'home.import.entry.contacts', nextState: 'home.jobs.data' };
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
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType()).then(function(result) {
                        // console.log(result.Result);
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardService.GetSchemaToLatticeFields(null, ImportWizardStore.getEntityType()).then(function(result) {
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
        .state('home.import.data.contacts.ids.latticefields', {
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
                    return ImportWizardStore.getFieldDocument();
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    return ImportWizardStore.getUnmappedFields();
                },
                mergedFieldDocument: function($q, ImportWizardStore) {
                    return ImportWizardStore.mergeFieldDocument({segment: true, save: false});
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
               AnalysisFields: function() {
                    return [
                        { name: 'LeadStatus', displayName: 'Lead Status' },
                        { name: 'LeadSource', displayName: 'Lead Source' },
                        { name: 'LeadType', displayName: 'Lead Type' },
                        { name: 'CreatedDate', displayName: 'Created Date' },
                        { name: 'LastModifiedDate', displayName: 'Last Modified Date' },
                        { name: 'DoNotMail', displayName: 'Has Opted Out of Email' },
                        { name: 'DoNotCall', displayName: 'Has Opted Out of Phone Calls' }
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
        .state('home.import.data.contacts.ids.latticefields.customfields', {
            url: '/customfields',
            onExit: function($transition$, ImportWizardStore){
                ImportWizardStore.setIgnore([]);
                var to = $transition$._targetState._definition.name;
                if(to === 'home.import.data.contacts.ids.latticefields'){
                    ImportWizardStore.removeFromState('home.import.data.contacts.ids.latticefields.customfields');
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
                    templateUrl: 'app/import/content/customfields/customfields.component.html'
                }
            }
        })
        .state('home.import.data.contacts.ids.latticefields.customfields.jobstatus', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.data': {
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        })
        .state('home.import.data.product_purchases', {
            url: '/product_purchases',
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
                DisableWizardNavOnLastStep: function () {
                    return null;
                },
                WizardControlsOptions: function() {
                    return { backState: 'home.import.entry.product_purchases', nextState: 'home.jobs.data' };
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
            redirectTo: 'home.import.data.product_purchases.ids'
        })
        .state('home.import.data.product_purchases.ids', {
            url: '/transactionids',
            resolve: {
                FieldDocument: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardService.GetSchemaToLatticeFields(null, ImportWizardStore.getEntityType()).then(function(result) {
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
        .state('home.import.data.product_purchases.ids.latticefields', {
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
        .state('home.import.data.product_purchases.ids.latticefields.jobstatus', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.data': {
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        })
        .state('home.import.data.product_bundles', {
            url: '/product_bundles',
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
                DisableWizardNavOnLastStep: function () {
                    return null;
                },
                WizardControlsOptions: function() {
                    return { backState: 'home.import.entry.product_bundles', nextState: 'home.jobs.data' };
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
            redirectTo: 'home.import.data.product_bundles.ids'
        })
        .state('home.import.data.product_bundles.ids', {
            url: '/transactionids',
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
        .state('home.import.data.product_bundles.ids.latticefields', {
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
        .state('home.import.data.product_bundles.ids.latticefields.jobstatus', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.data': {
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        })
        .state('home.import.data.product_hierarchy', {
            url: '/product_hierarchy',
            params: {
                wizard_steps: 'product_hierarchy'  // use this to override entity type as default for wizard step key
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

                    return ImportWizardStore.getWizardProgressItems(wizard_steps || 'product_hierarchy');
                },
                DisableWizardNavOnLastStep: function () {
                    return null;
                },
                WizardControlsOptions: function() {
                    return { backState: 'home.import.entry.product_hierarchy', nextState: 'home.jobs.data' };
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
            redirectTo: 'home.import.data.product_hierarchy.ids'
        })
        .state('home.import.data.product_hierarchy.ids', {
            url: '/producthierarchyids',
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
        .state('home.import.data.product_hierarchy.ids.product_hierarchy', {
            url: '/producthierarchy',
            resolve: {
                FieldDocument: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType()).then(function(result) {
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
        .state('home.import.data.product_hierarchy.ids.product_hierarchy.jobstatus', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.data': {
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        });
});