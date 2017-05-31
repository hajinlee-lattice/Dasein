angular
.module('lp.import', [
    'common.wizard',
    'lp.import.entry',
    'lp.import.wizard.thirdpartyids',
    'lp.import.wizard.latticefields',
    'lp.import.wizard.jobstatus',
    'lp.import.wizard.customfields',
    'lp.import.wizard.accountids'
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
        .state('home.import.entry.accountfields', {
            url: '/accountfields',
            views: {
                'entry_content@home.import.entry': {
                    templateUrl: 'app/import/entry/accountfields/accountfields.component.html'
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
        .state('home.import.entry.contactfields', {
            url: '/contactfields',
            views: {
                'entry_content@home.import.entry': {
                    templateUrl: 'app/import/entry/contactfields/contactfields.component.html'
                }
            }
        })
        .state('home.import.entry.eloquoa', {
            url: '/eloquoa'
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
                WizardProgressContext: function() {
                    return 'import';
                },
                WizardProgressItems: function() {
                    return [
                        { label: 'Account IDs', state: 'accounts.one' },
                        { label: '3rd Party IDs', state: 'accounts.one.two' },
                        { label: 'Lattice Fields', state: 'accounts.one.two.three' },
                        { label: 'Custom Fields', state: 'accounts.one.two.three.four' },
                        { label: 'Import Data', state: 'accounts.one.two.three.four.five' }
                    ];
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


        .state('home.import.wizard.accountfields', {
            url: '/accountfields',
            resolve: {
                WizardProgressContext: function() {
                    return 'import';
                },
                WizardProgressItems: function() {
                    return [
                        { label: 'Account IDs', state: 'accountfields.one' },
                        { label: '3rd Party IDs', state: 'accountfields.one.two' },
                        { label: 'Custom Fields', state: 'accountfields.one.two.three' },
                        { label: 'Import Data', state: 'accountfields.one.two.three.four' }
                    ];
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
                            return { backState: 'home.import.entry.accountfields', nextState: 'home.segments' };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            }
        })
        .state('home.import.wizard.accountfields.one', {
            url: '/accountids',
            views: {
                'wizard_content@home.import.wizard': {
                    controller: 'ImportWizardAccountIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/accountids/accountids.component.html'
                }
            }
        })
        .state('home.import.wizard.accountfields.one.two', {
            url: '/thirdpartyids',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/content/thirdpartyids/thirdpartyids.component.html'
                }
            }
        })
        .state('home.import.wizard.accountfields.one.two.three', {
            url: '/customfields',
            views: {
                'wizard_content@home.import.wizard': {
                    controller: 'ImportWizardCustomFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/customfields/customfields.component.html'
                }
            }
        })
        .state('home.import.wizard.accountfields.one.two.three.four', {
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
                WizardProgressContext: function() {
                    return 'import';
                },
                WizardProgressItems: function() {
                    return [
                        { label: 'Lattice Fields', state: 'contacts.one' },
                        { label: 'Custom Fields', state: 'contacts.one.two' },
                        { label: 'Import Data', state: 'contacts.one.two.three' }
                    ]
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
        .state('home.import.wizard.contacts.one', {
            url: '/latticefields',
            resolve: {
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
        .state('home.import.wizard.contacts.one.two', {
            url: '/customfields',
            views: {
                'wizard_content@home.import.wizard': {
                    controller: 'ImportWizardCustomFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/customfields/customfields.component.html'
                }
            }
        })
        .state('home.import.wizard.contacts.one.two.three', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        })


        .state('home.import.wizard.contactfields', {
            url: '/accounts',
            resolve: {
                WizardProgressContext: function() {
                    return 'import';
                },
                WizardProgressItems: function() {
                    return [
                        { label: 'Lattice Fields', state: 'contactfields.one' },
                        { label: 'Custom Fields', state: 'contactfields.one.two' },
                        { label: 'Import Data', state: 'contactfields.one.two.three' }
                    ]
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
                            return { backState: 'home.import.entry.contactfields', nextState: 'home.segments' };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            }
        })
        .state('home.import.wizard.contactfields.one', {
            url: '/latticefields',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/content/latticefields/latticefields.component.html'
                }
            }
        })
        .state('home.import.wizard.contactfields.one.two', {
            url: '/customfields',
            views: {
                'wizard_content@home.import.wizard': {
                    controller: 'ImportWizardCustomFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/customfields/customfields.component.html'
                }
            }
        })
        .state('home.import.wizard.contactfields.one.two.three', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        })


        .state('home.import.wizard.eloquoa', {
            url: '/accounts',
            resolve: {
                WizardProgressContext: function() {
                    return 'import';
                },
                WizardProgressItems: function() {
                    return [
                        { label: 'Lattice Fields', state: 'eloquoa.one' },
                        { label: 'Custom Fields', state: 'eloquoa.one.two' },
                        { label: 'Import Data', state: 'eloquoa.one.two.three' }
                    ]
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
                            return { backState: 'home.import.entry.eloquoa', nextState: 'home.segments' };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            },
            redirectTo: 'home.import.wizard.eloquoa.one'
        })
        .state('home.import.wizard.eloquoa.one', {
            url: '/latticefields',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/content/latticefields/latticefields.component.html'
                }
            }
        })
        .state('home.import.wizard.eloquoa.one.two', {
            url: '/customfields',
            views: {
                'wizard_content@home.import.wizard': {
                    controller: 'ImportWizardCustomFields',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/content/customfields/customfields.component.html'
                }
            }
        })
        .state('home.import.wizard.eloquoa.one.two.three', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/content/jobstatus/jobstatus.component.html'
                }
            }
        });

});