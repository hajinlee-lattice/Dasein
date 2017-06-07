angular
.module('lp.playbook', [
    'common.wizard',
    'lp.cg.talkingpoint',
    'lp.playbook.wizard.settings',
    'lp.playbook.wizard.segment',
    'lp.playbook.wizard.rating',
    'lp.playbook.wizard.targets',
    'lp.playbook.wizard.insights',
    'lp.playbook.wizard.preview',
    'lp.playbook.wizard.launch'
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
                        },
                        WizardContainerId: function() {
                            return 'playbook';
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
            url: '/wizard/:play_name',
            resolve: {
                WizardValidationStore: function(PlaybookWizardStore) {
                    return PlaybookWizardStore;
                },
                WizardProgressContext: function() {
                    return 'playbook';
                },
                WizardProgressItems: function() {
                    return [
                        { label: 'Settings', state: 'settings' },
                        { label: 'Segment', state: 'settings.segment' },
                        { label: 'Rating', state: 'settings.segment.rating' },
                        { label: 'Targets', state: 'settings.segment.rating.targets' },
                        { label: 'Insights', state: 'settings.segment.rating.targets.insights' },
                        { label: 'Preview', state: 'settings.segment.rating.targets.insights.preview' },
                        { label: 'Launch', state: 'settings.segment.rating.targets.insights.preview.launch' }
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
            redirectTo: 'home.playbook.wizard.settings'
        })
        .state('home.playbook.wizard.settings', {
            url: '/settings',
            views: {
                'wizard_content@home.playbook': {
                    templateUrl: 'app/playbook/content/settings/settings.component.html'
                }
            },
        })
        .state('home.playbook.wizard.settings.segment', {
            url: '/segment',
            resolve: {
                Segments: function(SegmentService) {
                    return SegmentService.GetSegments();
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
        .state('home.playbook.wizard.settings.segment.rating', {
            url: '/rating',
            resolve: {
                Ratings: function(PlaybookWizardStore) {
                    return PlaybookWizardStore.getRatings();
                }
            },
            views: {
                'wizard_content@home.playbook': {
                    controller: 'PlaybookWizardRating',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/rating/rating.component.html'
                }
            }
        })
        .state('home.playbook.wizard.settings.segment.rating.targets', {
            url: '/targets',
            resolve: {
                LoadDemoData: function(QueryStore) {
                    return QueryStore.loadData();
                },
                DefaultSelectedObject: function() {
                    return 'accounts';
                },
                SelectedSegment: function($q, PlaybookWizardStore, QueryStore) {
                    var deferred = $q.defer();
                    var segment = PlaybookWizardStore.getSavedSegment()
                    QueryStore.setupStore(segment).then(function() {
                        deferred.resolve(segment);
                    });
                    return deferred.promise;
                }
            },
            views: {
                'wizard_content@home.playbook': {
                    controller: 'PlaybookWizardTargets',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/targets/targets.component.html'
                }
            }
        })
        .state('home.playbook.wizard.settings.segment.rating.targets.insights', {
            url: '/insights',
            resolve: {
                TalkingPoints: function(CgTalkingPointStore) {
                    return CgTalkingPointStore.getTalkingPoints();
                },
                TalkingPointAttributes: function (CgTalkingPointStore) {
                    return CgTalkingPointStore.getAttributes();
                },
                loadTinyMce: function($ocLazyLoad) {
                    return $ocLazyLoad.load('lib/js/tinymce/tinymce.min.js');
                },
                loadUiTinyMce: function($ocLazyLoad) {
                    return $ocLazyLoad.load('lib/js/tinymce/uitinymce.min.js');
                }
            },
            views: {
                'wizard_content@home.playbook': {
                    controller: 'PlaybookWizardInsights',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/insights/insights.component.html'
                }
            }
        })
        .state('home.playbook.wizard.settings.segment.rating.targets.insights.preview', {
            url: '/preview',
            views: {
                'wizard_content@home.playbook': {
                    templateUrl: 'app/playbook/content/preview/preview.component.html'
                }
            }
        })
        .state('home.playbook.wizard.settings.segment.rating.targets.insights.preview.launch', {
            url: '/launch',
            views: {
                'wizard_content@home.playbook': {
                    templateUrl: 'app/playbook/content/launch/launch.component.html'
                }
            }
        })
});