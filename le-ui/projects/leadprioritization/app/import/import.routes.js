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
    'lp.import.wizard.producthierarchy'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.import', {
            url: '/import',
            redirectTo: 'home.import.entry.accounts'
        })
        .state('home.import.calendar', {
            url: '/calendar',
            views: {
                'main@': {
                    controller: 'ImportWizardCalendar',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/calendar/calendar.component.html'
                }
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
                Calendar: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardStore.getCalendar().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
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
            views: {
                'main@': {
                    resolve: {
                        WizardHeaderTitle: function() {
                            return 'Import';
                        },
                        WizardContainerId: function() {
                            return 'data-import';
                        }
                    },
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
                }
            },
            views: {
                'wizard_progress': {
                    controller: 'ImportWizardProgress',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/progress/progress.component.html'
                },
                'wizard_controls': {
                    resolve: {
                        WizardControlsOptions: function() {
                            return { backState: 'home.import.entry.accounts', nextState: 'home.jobs.data' };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            }
        })
        .state('home.import.data.accounts.ids', {
            url: '/accountids',
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardAccountIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/accountids/accountids.component.html'
                }
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
                        { name: 'Lattitude' }
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
                        wizard_steps = $stateParams.wizard_steps || entityType.toLowerCase();

                    return ImportWizardStore.getWizardProgressItems(wizard_steps || 'contact');
                }
            },
            views: {
                'wizard_progress': {
                    controller: 'ImportWizardProgress',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/progress/progress.component.html'
                },
                'wizard_controls': {
                    resolve: {
                        WizardControlsOptions: function() {
                            return { backState: 'home.import.entry.contacts', nextState: 'home.jobs.data' };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            }
        })
        .state('home.import.data.contacts.ids', {
            url: '/contactids',
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardContactIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/contactids/contactids.component.html'
                }
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
            }
        })
        .state('home.import.data.contacts.ids.latticefields', {
            url: '/latticefields',
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
                }
            },
            views: {
                'wizard_progress': {
                    controller: 'ImportWizardProgress',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/progress/progress.component.html'
                },
                'wizard_controls': {
                    resolve: {
                        WizardControlsOptions: function() {
                            return { backState: 'home.import.entry.product_purchases', nextState: 'home.jobs.data' };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            },
            redirectTo: 'home.import.data.product_purchases.ids'
        })
        .state('home.import.data.product_purchases.ids', {
            url: '/transactionids',
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardTransactionIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/transactionids/transactionids.component.html'
                }
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
                }
            },
            views: {
                'wizard_progress': {
                    controller: 'ImportWizardProgress',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/progress/progress.component.html'
                },
                'wizard_controls': {
                    resolve: {
                        WizardControlsOptions: function() {
                            return { backState: 'home.import.entry.product_bundles', nextState: 'home.jobs.data' };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            },
            redirectTo: 'home.import.data.product_bundles.ids'
        })
        .state('home.import.data.product_bundles.ids', {
            url: '/transactionids',
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardProductIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/productids/productids.component.html'
                }
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
                },
                Calendar: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardStore.getCalendar().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
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
                }
            },
            views: {
                'wizard_progress': {
                    controller: 'ImportWizardProgress',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/progress/progress.component.html'
                },
                'wizard_controls': {
                    resolve: {
                        WizardControlsOptions: function() {
                            return { backState: 'home.import.entry.product_hierarchy', nextState: 'home.jobs.data' };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            },
            redirectTo: 'home.import.data.product_hierarchy.ids'
        })
        .state('home.import.data.product_hierarchy.ids', {
            url: '/producthierarchyids',
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardProductHierarchyIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/producthierarchyids/producthierarchyids.component.html'
                }
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
                },
                Calendar: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardStore.getCalendar().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            }
        })
        .state('home.import.data.product_hierarchy.ids.product_hierarchy', {
            url: '/producthierarchy',
            views: {
                'wizard_content@home.import.data': {
                    controller: 'ImportWizardProductHierarchy',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/producthierarchy/producthierarchy.component.html'
                }
            },
            resolve: {
                FieldDocument: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName(), ImportWizardStore.getEntityType()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
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