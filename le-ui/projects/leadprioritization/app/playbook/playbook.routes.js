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
                        { label: 'Insights', state: 'one.two.three.four.cgfive' },
                        { label: 'Preview', state: 'one.two.three.four.cgfive.cgsix' },
                        { label: 'Launch', state: 'one.two.three.four.cgfive.cgsix.seven' }
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
            },
        })
        .state('home.playbook.wizard.one.two', {
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
        .state('home.playbook.wizard.one.two.three', {
            url: '/ratings',
            resolve: {
                Ratings: function(PlaybookWizardStore) {
                    return PlaybookWizardStore.getRatings();
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
                },
            },
            views: {
                'wizard_content@home.playbook': {
                    controller: 'PlaybookWizardTargets',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/targets/targets.component.html'
                }
            }
        })
        .state('home.playbook.wizard.one.two.three.four.cgfive', {
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
                    controller: 'cgTalkingPointCtrl',
                    controllerAs: 'vm',
                    templateUrl: 'app/cgtalkingpoint/cgtalkingpoint.component.html'
                }
            }
        })
        .state('home.playbook.wizard.one.two.three.four.cgfive.cgsix', {
            url: '/preview',
            views: {
                'wizard_content@home.playbook': {
                    controller: 'cgTalkingPointPreviewCtrl',
                    controllerAs: 'vm',
                    templateUrl: 'app/cgtalkingpoint/tppreview/tppreview.component.html'
                }
            }
        })
        .state('home.playbook.wizard.one.two.three.four.cgfive.cgsix.seven', {
            url: '/launch',
            views: {
                'wizard_content@home.playbook': {
                    templateUrl: 'app/playbook/content/launch/launch.component.html'
                }
            }
        })
});