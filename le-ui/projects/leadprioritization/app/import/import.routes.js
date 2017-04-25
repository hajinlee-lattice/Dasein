angular
.module('lp.import', [
    'lp.import.entry',
    'lp.import.wizard'
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
            url: '/eloquoa',
            views: {
                'entry_content@home.import.entry': {
                    templateUrl: 'app/import/entry/eloquoa/eloquoa.component.html'
                }
            }
        })


        .state('home.import.wizard', {
            url: '/wizard',
            views: {
                'main@': {
                    templateUrl: 'app/import/wizard/wizard.component.html'
                }
            }
        })
        .state('home.import.wizard.accounts', {
            url: '/accounts',
            resolve: {
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
                    templateUrl: 'app/import/wizard/progress/progress.component.html'
                },
                'wizard_controls': {
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/wizard/controls/controls.component.html'
                }
            }
        })
        .state('home.import.wizard.accounts.one', {
            url: '/accountids',
            views: {
                'wizard_content@home.import.wizard': {
                    controller: 'ImportWizardAccountIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/wizard/accountids/accountids.component.html'
                }
            }
        })
        .state('home.import.wizard.accounts.one.two', {
            url: '/thirdpartyids',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/thirdpartyids/thirdpartyids.component.html'
                }
            }
        })
        .state('home.import.wizard.accounts.one.two.three', {
            url: '/latticefields',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/latticefields/latticefields.component.html'
                }
            }
        })
        .state('home.import.wizard.accounts.one.two.three.four', {
            url: '/customfields',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/customfields/customfields.component.html'
                }
            }
        })
        .state('home.import.wizard.accounts.one.two.three.four.five', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/jobstatus/jobstatus.component.html'
                }
            }
        })


        .state('home.import.wizard.accountfields', {
            url: '/accountfields',
            resolve: {
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
                    templateUrl: 'app/import/wizard/progress/progress.component.html'
                },
                'wizard_controls': {
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/wizard/controls/controls.component.html'
                }
            }
        })
        .state('home.import.wizard.accountfields.one', {
            url: '/accountids',
            views: {
                'wizard_content@home.import.wizard': {
                    controller: 'ImportWizardAccountIDs',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/wizard/accountids/accountids.component.html'
                }
            }
        })
        .state('home.import.wizard.accountfields.one.two', {
            url: '/thirdpartyids',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/thirdpartyids/thirdpartyids.component.html'
                }
            }
        })
        .state('home.import.wizard.accountfields.one.two.three', {
            url: '/customfields',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/customfields/customfields.component.html'
                }
            }
        })
        .state('home.import.wizard.accountfields.one.two.three.four', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/jobstatus/jobstatus.component.html'
                }
            }
        })


        .state('home.import.wizard.contacts', {
            url: '/accounts',
            resolve: {
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
                    templateUrl: 'app/import/wizard/progress/progress.component.html'
                },
                'wizard_controls': {
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/wizard/controls/controls.component.html'
                }
            }
        })
        .state('home.import.wizard.contacts.one', {
            url: '/latticefields',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/latticefields/latticefields.component.html'
                }
            }
        })
        .state('home.import.wizard.contacts.one.two', {
            url: '/customfields',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/customfields/customfields.component.html'
                }
            }
        })
        .state('home.import.wizard.contacts.one.two.three', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/jobstatus/jobstatus.component.html'
                }
            }
        })


        .state('home.import.wizard.contactfields', {
            url: '/accounts',
            resolve: {
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
                    templateUrl: 'app/import/wizard/progress/progress.component.html'
                },
                'wizard_controls': {
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/wizard/controls/controls.component.html'
                }
            }
        })
        .state('home.import.wizard.contactfields.one', {
            url: '/latticefields',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/latticefields/latticefields.component.html'
                }
            }
        })
        .state('home.import.wizard.contactfields.one.two', {
            url: '/customfields',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/customfields/customfields.component.html'
                }
            }
        })
        .state('home.import.wizard.contactfields.one.two.three', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/jobstatus/jobstatus.component.html'
                }
            }
        })


        .state('home.import.wizard.eloquoa', {
            url: '/accounts',
            resolve: {
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
                    templateUrl: 'app/import/wizard/progress/progress.component.html'
                },
                'wizard_controls': {
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: 'app/import/wizard/controls/controls.component.html'
                }
            },
            redirectTo: 'home.import.wizard.eloquoa.one'
        })
        .state('home.import.wizard.eloquoa.one', {
            url: '/latticefields',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/latticefields/latticefields.component.html'
                }
            }
        })
        .state('home.import.wizard.eloquoa.one.two', {
            url: '/customfields',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/customfields/customfields.component.html'
                }
            }
        })
        .state('home.import.wizard.eloquoa.one.two.three', {
            url: '/jobstatus',
            views: {
                'wizard_content@home.import.wizard': {
                    templateUrl: 'app/import/wizard/jobstatus/jobstatus.component.html'
                }
            }
        });

});