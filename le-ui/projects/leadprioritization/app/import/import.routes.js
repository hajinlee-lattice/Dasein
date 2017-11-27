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
            url: '/product_purchases'
        })
        .state('home.import.entry.product_bundles', {
            url: '/product_bundles'
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
        .state('home.import.wizard.accounts.one', {
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
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardService.GetSchemaToLatticeFields().then(function(result) {
                        deferred.resolve(result['Account']);
                    });

                    return deferred.promise;
                }
            }
        })
        .state('home.import.wizard.accounts.one.two', {
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
        .state('home.import.wizard.accounts.one.two.three', {
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
                        { name: 'Website Address', options: [{ name: "Website" }, { name: "Another Value" }] },
                        { name: 'D-U-N-S', options: [{ name: "DUNS" }, { name: "Another Value" }] },
                        { name: 'Company Name', options: [{ name: "Name" }, { name: "Another Value" }] },
                        { name: 'Phone', options: [{ name: "Phone Number" }, { name: "Another Value" }] },
                        { name: 'City', options: [{ name: "CompanyCity" }, { name: "Another Value" }] },
                        { name: 'Country', options: [{ name: "Country" }, { name: "Another Value" }] },
                        { name: 'State', options: [{ name: "CompanyState" }, { name: "Another Value" }] },
                        { name: 'Zip', options: [{ name: "Zipcode" }, { name: "Another Value" }] }
                    ];
                },
                AnalysisFields: function() {
                    return [
                        { name: 'Is Customer', options: [{ name: "Customer" }, { name: "Another Value" }] },
                        { name: 'Revenue', options: [{ name: "Revenue" }, { name: "Another Value" }] },
                        { name: 'Industry', options: [{ name: "Industry" }, { name: "Another Value" }] },
                        { name: 'Employees', options: [{ name: "Employees" }, { name: "Another Value" }] }
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
        .state('home.import.wizard.accounts.one.two.three.four', {
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
        .state('home.import.wizard.accounts.one.two.three.four.five', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        })
        .state('home.import.wizard.contacts.one', {
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
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardService.GetSchemaToLatticeFields().then(function(result) {
                        deferred.resolve(result['Account']);
                    });

                    return deferred.promise;
                }
            }
        })
        .state('home.import.wizard.contacts.one.two', {
            url: '/latticefields',
            resolve: {
                FieldDocument: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardService.GetSchemaToLatticeFields().then(function(result) {
                        deferred.resolve(result['Account']);
                    });

                    return deferred.promise;
                },
                Type: function(){
                    return "Contact";
                },
                MatchingFields: function() {
                    return [
                        { name: 'Contact ID', options: [{ name: "ContactID" }, { name: "Another Value" }] },
                        { name: 'Account ID', options: [{ name: "AccountID" }, { name: "Another Value" }] },
                        { name: 'Last Name', options: [{ name: "LastName" }, { name: "Another Value" }] },
                        { name: 'First Name', options: [{ name: "FirstName" }, { name: "Another Value" }] },
                        { name: 'Title', options: [{ name: "Title" }, { name: "Another Value" }] },
                        { name: 'Email', options: [{ name: "Email" }, { name: "Another Value" }] }
                    ];
                },
                AnalysisFields: function() {
                    return [
                        { name: 'Lead Status', options: [{ name: "LeadStatus" }, { name: "Another Value" }] },
                        { name: 'Lead Source', options: [{ name: "LeadSource" }, { name: "Another Value" }] },
                        { name: 'Lead Type', options: [{ name: "Contact" }, { name: "Another Value" }] },
                        { name: 'Twitter', options: [{ name: "Twitter" }, { name: "Another Value" }] },
                        { name: 'LinkedIn URL', options: [{ name: "LinkedInURL" }, { name: "Another Value" }] },
                        { name: 'Created Date', options: [{ name: "CreatedDate" }, { name: "Another Value" }] },
                        { name: 'Last Modified Date', options: [{ name: "LastModified" }, { name: "Another Value" }] },
                        { name: 'Has Opted Out of Email', options: [{ name: "OptedOutEmail" }, { name: "Another Value" }] },
                        { name: 'Has Opted Out of Phone Calls', options: [{ name: "OptedOutPhone" }, { name: "Another Value" }] },
                        { name: 'Birthdate', options: [{ name: "Birthdate" }, { name: "Another Value" }] }
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
        .state('home.import.wizard.contacts.one.two.three', {
            url: '/customfields',
            views: {
                'wizard_content@home.import.wizard': {
                    controller: 'ImportWizardCustomFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/customfields/customfields.component.html'
                }
            }
        })
        .state('home.import.wizard.contacts.one.two.three.four', {
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
            redirectTo: 'home.import.wizard.product_purchases.one'
        })
        .state('home.import.wizard.product_purchases.one', {
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
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardService.GetSchemaToLatticeFields().then(function(result) {
                        deferred.resolve(result['Account']);
                    });

                    return deferred.promise;
                }
            }
        })
        .state('home.import.wizard.product_purchases.one.two', {
            url: '/latticefields',
            resolve: {
                FieldDocument: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardService.GetSchemaToLatticeFields().then(function(result) {
                        deferred.resolve(result['Account']);
                    });

                    return deferred.promise;
                },
                Type: function(){
                    return "Contact";
                },
                MatchingFields: function() {
                    return [
                        { name: 'Contact ID', options: [{ name: "ContactID" }, { name: "Another Value" }] },
                        { name: 'Account ID', options: [{ name: "AccountID" }, { name: "Another Value" }] },
                        { name: 'Last Name', options: [{ name: "LastName" }, { name: "Another Value" }] },
                        { name: 'First Name', options: [{ name: "FirstName" }, { name: "Another Value" }] },
                        { name: 'Title', options: [{ name: "Title" }, { name: "Another Value" }] },
                        { name: 'Email', options: [{ name: "Email" }, { name: "Another Value" }] }
                    ];
                },
                AnalysisFields: function() {
                    return [
                        { name: 'Lead Status', options: [{ name: "LeadStatus" }, { name: "Another Value" }] },
                        { name: 'Lead Source', options: [{ name: "LeadSource" }, { name: "Another Value" }] },
                        { name: 'Lead Type', options: [{ name: "Contact" }, { name: "Another Value" }] },
                        { name: 'Twitter', options: [{ name: "Twitter" }, { name: "Another Value" }] },
                        { name: 'LinkedIn URL', options: [{ name: "LinkedInURL" }, { name: "Another Value" }] },
                        { name: 'Created Date', options: [{ name: "CreatedDate" }, { name: "Another Value" }] },
                        { name: 'Last Modified Date', options: [{ name: "LastModified" }, { name: "Another Value" }] },
                        { name: 'Has Opted Out of Email', options: [{ name: "OptedOutEmail" }, { name: "Another Value" }] },
                        { name: 'Has Opted Out of Phone Calls', options: [{ name: "OptedOutPhone" }, { name: "Another Value" }] },
                        { name: 'Birthdate', options: [{ name: "Birthdate" }, { name: "Another Value" }] }
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
        .state('home.import.wizard.product_purchases.one.two.three', {
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
            redirectTo: 'home.import.wizard.product_bundles.one'
        })
        .state('home.import.wizard.product_bundles.one', {
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
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardService.GetSchemaToLatticeFields().then(function(result) {
                        deferred.resolve(result['Account']);
                    });

                    return deferred.promise;
                }
            }
        })
        .state('home.import.wizard.product_bundles.one.two', {
            url: '/latticefields',
            resolve: {
                FieldDocument: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();
                    ImportWizardService.GetFieldDocument(ImportWizardStore.getCsvFileName()).then(function(result) {
                        ImportWizardStore.setFieldDocument(result.Result);
                        deferred.resolve(result.Result);
                    });

                    return deferred.promise;
                },
                UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                    var deferred = $q.defer();

                    ImportWizardService.GetSchemaToLatticeFields().then(function(result) {
                        deferred.resolve(result['Account']);
                    });

                    return deferred.promise;
                },
                Type: function(){
                    return "Contact";
                },
                MatchingFields: function() {
                    return [
                        { name: 'Contact ID', options: [{ name: "ContactID" }, { name: "Another Value" }] },
                        { name: 'Account ID', options: [{ name: "AccountID" }, { name: "Another Value" }] },
                        { name: 'Last Name', options: [{ name: "LastName" }, { name: "Another Value" }] },
                        { name: 'First Name', options: [{ name: "FirstName" }, { name: "Another Value" }] },
                        { name: 'Title', options: [{ name: "Title" }, { name: "Another Value" }] },
                        { name: 'Email', options: [{ name: "Email" }, { name: "Another Value" }] }
                    ];
                },
                AnalysisFields: function() {
                    return [
                        { name: 'Lead Status', options: [{ name: "LeadStatus" }, { name: "Another Value" }] },
                        { name: 'Lead Source', options: [{ name: "LeadSource" }, { name: "Another Value" }] },
                        { name: 'Lead Type', options: [{ name: "Contact" }, { name: "Another Value" }] },
                        { name: 'Twitter', options: [{ name: "Twitter" }, { name: "Another Value" }] },
                        { name: 'LinkedIn URL', options: [{ name: "LinkedInURL" }, { name: "Another Value" }] },
                        { name: 'Created Date', options: [{ name: "CreatedDate" }, { name: "Another Value" }] },
                        { name: 'Last Modified Date', options: [{ name: "LastModified" }, { name: "Another Value" }] },
                        { name: 'Has Opted Out of Email', options: [{ name: "OptedOutEmail" }, { name: "Another Value" }] },
                        { name: 'Has Opted Out of Phone Calls', options: [{ name: "OptedOutPhone" }, { name: "Another Value" }] },
                        { name: 'Birthdate', options: [{ name: "Birthdate" }, { name: "Another Value" }] }
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
        .state('home.import.wizard.product_bundles.one.two.three', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        });
});