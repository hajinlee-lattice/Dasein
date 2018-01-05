angular
.module('lp.import', [
    'common.wizard',
    'lp.import.entry',
    'lp.import.wizard.thirdpartyids',
    'lp.import.wizard.latticefields',
    'lp.import.wizard.jobstatus',
    'lp.import.wizard.customfields',
    'lp.import.wizard.accountids',
    'lp.import.wizard.contactids',
    'lp.import.wizard.transactionids',
    'lp.import.wizard.productids'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.import', {
            url: '/import',
            redirectTo: 'home.import.entry.accounts'
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

        .state('home.import.wizard', {
            url: '/wizard',
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
        .state('home.import.wizard.accounts', {
            url: '/accounts',
            resolve: {
                WizardValidationStore: function(ImportWizardStore) {
                    return ImportWizardStore;
                },
                WizardProgressContext: function() {
                    return 'import';
                },
                WizardProgressItems: function($stateParams, ImportWizardStore) {
                    var wizard_steps = $stateParams.wizard_steps;
                    return ImportWizardStore.getWizardProgressItems(wizard_steps || 'all');
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
                            return { backState: 'home.import.entry.accounts', nextState: 'home.segments' };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            }
        })
        .state('home.import.wizard.accounts.ids', {
            url: '/accountids',
            views: {
                'wizard_content@home.import.wizard': {
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
        .state('home.import.wizard.accounts.ids.thirdpartyids', {
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
                'wizard_content@home.import.wizard': {
                    controller: 'ImportWizardThirdPartyIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/thirdpartyids/thirdpartyids.component.html'
                }
            }
        })
        .state('home.import.wizard.accounts.ids.thirdpartyids.latticefields', {
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
                        { name: 'DUNS' },
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
                        { name: 'Account_Type', displayName: 'Account Type' },
                        { name: 'Industry' },
                        { name: 'Est_Yearly_Revenue', displayName: 'Estimated Yearly Revenue' },
                        { name: 'Est_Yearly_Revenue_Currency_External_ID', displayName: 'Estimated Yearly Revenue Currency External ID' },
                        { name: 'Lattitude' },
                        { name: 'Longitude' },
                        { name: 'Account_Business_Segment', displayName: 'Account Business Segment' }
                    ];
                }
            },
            views: {
                'wizard_content@home.import.wizard': {
                    controller: 'ImportWizardLatticeFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/latticefields/latticefields.component.html'
                }
            }
        })
        .state('home.import.wizard.accounts.ids.thirdpartyids.latticefields.customfields', {
            url: '/customfields',
            resolve: {
                FieldDocument: function($q, ImportWizardStore) {
                    return ImportWizardStore.getFieldDocument();
                }
            },
            views: {
                'wizard_content@home.import.wizard': {
                    controller: 'ImportWizardCustomFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/customfields/customfields.component.html'
                }
            }
        })
        .state('home.import.wizard.accounts.ids.thirdpartyids.latticefields.customfields.jobstatus', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        })
        .state('home.import.wizard.contacts', {
            url: '/accounts',
            resolve: {
                WizardValidationStore: function(ImportWizardStore) {
                    return ImportWizardStore;
                },
                WizardProgressContext: function() {
                    return 'import';
                },
                WizardProgressItems: function($stateParams, ImportWizardStore) {
                    var wizard_steps = $stateParams.wizard_steps;
                    return ImportWizardStore.getWizardProgressItems(wizard_steps || 'contacts');
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
                            return { backState: 'home.import.entry.contacts', nextState: 'home.segments' };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            }
        })
        .state('home.import.wizard.contacts.ids', {
            url: '/contactids',
            views: {
                'wizard_content@home.import.wizard': {
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
        .state('home.import.wizard.contacts.ids.latticefields', {
            url: '/latticefields',
            resolve: {
                FieldDocument: function($q, ImportWizardStore) {
                    return ImportWizardStore.getFieldDocument();
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    return ImportWizardStore.getUnmappedFields();
                },
                Type: function(){
                    return "Contacts";
                },
                MatchingFields: function() {
                    return [
                        { name: 'Website' },
                        { name: 'CompanyName' },
                        { name: 'DUNS' },
                        { name: 'IP_Address', displayName: 'IP Address' },
                        { name: 'City' },
                        { name: 'Country' },
                        { name: 'State_Provence', displayName: 'State or Provence' },
                        { name: 'PostalCode', displayName: 'Postal Code' },
                        { name: 'Last_Name', displayName: 'Last Name' },
                        { name: 'First_Name', displayName: 'First Name' },
                        { name: 'Email' }
                    ];
                },
               AnalysisFields: function() {
                    return [
                        { name: 'Lead_Status', displayName: 'Lead Status' },
                        { name: 'Lead_Source', displayName: 'Lead Source' },
                        { name: 'Lead_Type', displayName: 'Lead Type' },
                        { name: 'Created_Date', displayName: 'Created Date' },
                        { name: 'Do_Not_Call', displayName: 'Do Not Call' },
                        { name: 'Do_Not_Mail', displayName: 'Do Not Mail' }
                    ];
                }
            },
            views: {
                'wizard_content@home.import.wizard': {
                    controller: 'ImportWizardLatticeFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/latticefields/latticefields.component.html'
                }
            }
        })
        .state('home.import.wizard.contacts.ids.latticefields.customfields', {
            url: '/customfields',
            views: {
                'wizard_content@home.import.wizard': {
                    controller: 'ImportWizardCustomFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/customfields/customfields.component.html'
                }
            }
        })
        .state('home.import.wizard.contacts.ids.latticefields.customfields.jobstatus', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        })
        .state('home.import.wizard.product_purchases', {
            url: '/product_purchases',
            resolve: {
                WizardValidationStore: function(ImportWizardStore) {
                    return ImportWizardStore;
                },
                WizardProgressContext: function() {
                    return 'import';
                },
                WizardProgressItems: function($stateParams, ImportWizardStore) {
                    var wizard_steps = $stateParams.wizard_steps;
                    return ImportWizardStore.getWizardProgressItems(wizard_steps || 'product_purchases');
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
                            return { backState: 'home.import.entry.product_purchases', nextState: 'home.segments' };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            },
            redirectTo: 'home.import.wizard.product_purchases.ids'
        })
        .state('home.import.wizard.product_purchases.ids', {
            url: '/transactionids',
            views: {
                'wizard_content@home.import.wizard': {
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
        .state('home.import.wizard.product_purchases.ids.latticefields', {
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
                        { name: 'TransactionTime', displayName: 'Transaction Time', required: true },
                        { name: 'Amount', required: true },
                        { name: 'Quantity', required: true },
                        { name: 'Cost' }
                    ];
                },
                AnalysisFields: function() {
                    return [];
                }
            },
            views: {
                'wizard_content@home.import.wizard': {
                    controller: 'ImportWizardLatticeFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/latticefields/latticefields.component.html'
                }
            }
        })
        .state('home.import.wizard.product_purchases.ids.latticefields.jobstatus', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        })
        .state('home.import.wizard.product_bundles', {
            url: '/product_bundles',
            resolve: {
                WizardValidationStore: function(ImportWizardStore) {
                    return ImportWizardStore;
                },
                WizardProgressContext: function() {
                    return 'import';
                },
                WizardProgressItems: function($stateParams, ImportWizardStore) {
                    var wizard_steps = $stateParams.wizard_steps;
                    return ImportWizardStore.getWizardProgressItems(wizard_steps || 'product_bundles');
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
                            return { backState: 'home.import.entry.product_bundles', nextState: 'home.segments' };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            },
            redirectTo: 'home.import.wizard.product_bundles.ids'
        })
        .state('home.import.wizard.product_bundles.ids', {
            url: '/transactionids',
            views: {
                'wizard_content@home.import.wizard': {
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
                }
            }
        })
        .state('home.import.wizard.product_bundles.ids.latticefields', {
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
                        { name: 'ProductName' },
                        { name: 'Description'}
                    ];
                },
                AnalysisFields: function() {
                    return [];
                }
            },
            views: {
                'wizard_content@home.import.wizard': {
                    controller: 'ImportWizardLatticeFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/latticefields/latticefields.component.html'
                }
            }
        })
        .state('home.import.wizard.product_bundles.ids.latticefields.jobstatus', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        });
});