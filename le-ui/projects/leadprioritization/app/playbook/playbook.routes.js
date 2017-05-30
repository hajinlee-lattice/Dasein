angular
.module('lp.playbook', [
    'common.wizard',
    'lp.playbook.wizard.segment',
    'lp.playbook.wizard.ratings',
    'lp.playbook.wizard.insights',
    'lp.playbook.wizard.targets',
    'lp.playbook.wizard.settings'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.playbook', {
            url: '/playbook',
            views: {
                'main@': {
                    resolve: {
                        WizardHeaderTitle: function() {
                            return 'Play Book';
                        }
                    },
                    controller: 'ImportWizard',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/wizard.component.html'
                }
            },
            redirectTo: 'home.playbook.wizard'
        })
        .state('home.playbook.wizard', {
            url: '/wizard',
            resolve: {
                WizardProgressContext: function() {
                    return 'playbook';
                },
                WizardProgressItems: function() {
                    return [
                        { label: 'Settings', state: 'one' },
                        { label: 'Segment', state: 'one.two' },
                        { label: 'Ratings', state: 'one.two.three' },
                        { label: 'Targets', state: 'one.two.three.four' },
                        { label: 'Insights', state: 'one.two.three.four.five' },
                        { label: 'Preview', state: 'one.two.three.four.five.six' },
                        { label: 'Launch', state: 'one.two.three.four.five.six.seven' }
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
                            return { backState: 'home.models', nextState: 'home.models' };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            },
            redirectTo: 'home.playbook.wizard.one'
        })
        .state('home.playbook.wizard.one', {
            url: '/settings',
            views: {
                'wizard_content@home.playbook': {
                    controller: 'PlaybookWizardSettings',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/settings/settings.component.html'
                }
            }
        })
        .state('home.playbook.wizard.one.two', {
            url: '/segment',
            resolve: {
                Identifiers: function() {
                    return [
                        { name: 'CRM ID', value: '' },
                        { name: 'MAP ID', value: '' }
                    ];
                }
            },
            views: {
                'wizard_content@home.playbook': {
                    controller: 'PlaybookWizardSegment',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/segment/segment.component.html'
                }
            }
        })
        .state('home.playbook.wizard.one.two.three', {
            url: '/ratings',
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
                'wizard_content@home.playbook': {
                    controller: 'PlaybookWizardRatings',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/ratings/ratings.component.html'
                }
            }
        })
        .state('home.playbook.wizard.one.two.three.four', {
            url: '/targets',
            views: {
                'wizard_content@home.playbook': {
                    controller: 'PlaybookWizardTargets',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/targets/targets.component.html'
                }
            }
        })
        .state('home.playbook.wizard.one.two.three.four.five', {
            url: '/insights',
            views: {
                'wizard_content@home.playbook': {
                    templateUrl: 'app/playbook/content/insights/insights.component.html'
                }
            }
        })
        .state('home.playbook.wizard.one.two.three.four.five.six', {
            url: '/preview',
            views: {
                'wizard_content@home.playbook': {
                    templateUrl: 'app/playbook/content/preview/preview.component.html'
                }
            }
        })
        .state('home.playbook.wizard.one.two.three.four.five.six.seven', {
            url: '/launch',
            views: {
                'wizard_content@home.playbook': {
                    templateUrl: 'app/playbook/content/launch/launch.component.html'
                }
            }
        })
});