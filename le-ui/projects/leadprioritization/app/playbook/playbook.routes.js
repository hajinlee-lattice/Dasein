angular
.module('lp.playbook', [
    'common.wizard',
    'lp.cg.talkingpoint',
    'lp.playbook.plays',
    'lp.playbook.dashboard',
    'lp.playbook.dashboard.launch_history',
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
            redirectTo: 'home.playbook.plays'
        })
        .state('home.playbook.plays', {
            url: '/plays',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Play Book'
            },
            resolve: {
                PlayList: function($q, PlaybookWizardService) {
                    var deferred = $q.defer();

                    PlaybookWizardService.getPlays().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                "main@": {
                    controller: 'PlayListController',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/playList/playList.component.html'
                }
            }
        })
        .state('home.playbook.dashboard', {
            url: '/dashboard/:play_name',
            params: {
                pageIcon: 'ico-insights',
                pageTitle: 'Play Overview'
            },
            resolve: {
                Play: function(PlaybookWizardStore, $state, $stateParams) {
                    return PlaybookWizardStore.getPlay($stateParams.play_name);
                }
            },
            views: {
                "navigation@": {
                    controller: function($scope, $stateParams, $state, $rootScope, PlaybookWizardStore) {
                        var play = PlaybookWizardStore.getCurrentPlay();
                        $scope.play_name = $stateParams.play_name || '';
                        $scope.segment = play.segment;
                        $scope.targetsDisabled = (play.segment ? false : true);
                        $scope.stateName = function() {
                            return $state.current.name;
                        }
                        $rootScope.$broadcast('header-back', { 
                            path: '^home.playbook.dashboard',
                            displayName: play.displayName,
                            sref: 'home.playbook'
                        });
                    },
                    templateUrl: 'app/playbook/content/dashboard/sidebar/sidebar.component.html'
                },
                'main@': {
                    controller: 'PlaybookDashboard',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/dashboard/dashboard.component.html'
                }
            }
        })
        .state('home.playbook.dashboard.insights', {
            url: '/insights',
            params: {
                pageIcon: 'ico-scoring',
                pageTitle: 'Insights',
                section: 'dashboard.insights'
            },
            resolve: {
                TalkingPoints: function(CgTalkingPointStore, $stateParams) {
                    var play_name = $stateParams.play_name || '';
                    return CgTalkingPointStore.getTalkingPoints(play_name);
                },
                TalkingPointAttributes: function (CgTalkingPointStore) {
                    return CgTalkingPointStore.getAttributes();
                },
                TalkingPointPreviewResources: function(CgTalkingPointStore) {
                    return CgTalkingPointStore.getTalkingPointsPreviewResources();
                },
                loadTinyMce: function($ocLazyLoad) {
                    return $ocLazyLoad.load('lib/js/tinymce/tinymce.min.js');
                },
                loadUiTinyMce: function($ocLazyLoad) {
                    return $ocLazyLoad.load('lib/js/tinymce/uitinymce.min.js');
                }
            },
            views: {
                'main@': {
                    controller: 'PlaybookWizardInsights',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/insights/insights.component.html'
                }
            }
        })
        .state('home.playbook.dashboard.insights.preview', {
            url: '/preview',
            params: {
                pageIcon: 'ico-scoring',
                pageTitle: 'Insights',
                section: 'dashboard.insights.preview'
            },
            resolve: {
                Play: function(PlaybookWizardStore, $stateParams) {
                    return PlaybookWizardStore.getPlay($stateParams.play_name);
                },
                TalkingPointPreviewResources: function(CgTalkingPointStore) {
                    return CgTalkingPointStore.getTalkingPointsPreviewResources();
                }
            },
            views: {
                'main@': {
                    controller: 'PlaybookWizardPreview',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/preview/preview.component.html'
                }
            }
        })
        .state('home.playbook.dashboard.segment', {
            url: '/segment',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Segments',
                section: 'dashboard.segment'
            },
            resolve: {
                Segments: function(SegmentService) {
                    return SegmentService.GetSegments();
                }
            },
            views: {
                'main@': {
                    controller: 'PlaybookWizardSegment',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/segment/segment.component.html'
                }
            }
        })
        .state('home.playbook.dashboard.targets', {
            url: '/targets/:segment',
            params: {
                pageIcon: 'ico-targets',
                pageTitle: 'Avaliable Targets',
                section: 'dashboard.targets'
            },
            resolve: {
                Accounts: ['$q', 'QueryStore', function($q, QueryStore) {
                    var deferred = $q.defer(),
                        segment = QueryStore.getSegment(),
                        restriction = QueryStore.getRestriction(),
                        query = {
                            free_form_text_search: '',
                            frontend_restriction: restriction,
                            page_filter: {
                                num_rows: 10,
                                row_offset: 0
                            },
                            restrict_with_sfdcid: false
                        };
                        deferred.resolve( QueryStore.GetDataByQuery('accounts', query, segment).then(function(data){ return data; }) );

                    return deferred.promise;

                }],
                AccountsCount: ['$q', 'QueryStore', function($q, QueryStore) {
                    var deferred = $q.defer(),
                        segment = QueryStore.getSegment(),
                        restriction = QueryStore.getRestriction();

                        if (segment === null) {                     
                            query = { 
                                'free_form_text_search': '',
                                'frontend_restriction': restriction,
                                'page_filter': {
                                    'num_rows': 15,
                                    'row_offset': 0
                                }
                            };
                        } else {
                            query = { 
                                'free_form_text_search': '',
                                'frontend_restriction': segment.frontend_restriction,
                                'page_filter': {
                                    'num_rows': 15,
                                    'row_offset': 0
                                }
                            };
                        };

                    deferred.resolve( QueryStore.GetCountByQuery('accounts', query, segment).then(function(data){ return data; }));
                    return deferred.promise;
                }],
                CountWithoutSalesForce: ['$q', 'QueryStore', function($q, QueryStore){

                    var deferred = $q.defer(),
                        restriction = QueryStore.getRestriction(),
                        query = {
                            'free_form_text_search': '',
                            'frontend_restriction': restriction,
                            'page_filter': {
                                'num_rows': 1000000,
                                'row_offset': 0
                            },
                            'restrict_with_sfdcid': false
                        };

                    QueryStore.GetCountByQuery('accounts', query).then(function(response){ 
                        deferred.resolve(response);
                    }); 

                    return deferred.promise;

                }],
                Config: ['$q', '$stateParams', 'PlaybookWizardStore', function($q, $stateParams, PlaybookWizardStore) {
                    var deferred = $q.defer();
                    PlaybookWizardStore.getPlay($stateParams.play_name).then(function(play){
                        var play = play;
                        var config = {
                            play: play,
                            excludeAccountsWithoutSalesforceId: play.excludeAccountsWithoutSalesforceId,
                            excludeContactsWithoutSalesforceId: play.excludeContactsWithoutSalesforceId,
                            onClick: function(property, bool) {
                                play[property] = bool;
                                PlaybookWizardStore.savePlay(play);
                            },
                            header: {
                                class: 'playbook-targets',
                                label: 'Targets'
                            }
                        }
                        deferred.resolve(config);
                    });
                    return deferred.promise;
                }],
            },
            views: {
                'main@': {
                    controller: 'QueryResultsCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/queryresults.component.html'
                }
            }
        })
        .state('home.playbook.dashboard.launch_history', {
            url: '/launch_history',
            params: {
                pageIcon: 'ico-refine',
                pageTitle: 'Launch History',
                section: 'dashboard.launch_history'
            },
            resolve: {
                LaunchHistoryData: function($q, $stateParams, PlaybookWizardStore) {
                    var deferred = $q.defer();
                    PlaybookWizardStore.getPlayLaunches($stateParams.play_name, "Launched").then(function(result){
                        console.log(result);
                        deferred.resolve(result);
                    });
                    return deferred.promise;
                }
            },
            views: {
                'main@': {
                    controller: 'PlaybookDashboardLaunchHistory',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/launch_history/launch_history.component.html'
                }
            }
        })
        .state('home.playbook.dashboard.launch_job', {
            url: '/launch/:applicationId/job',
            params: {
                pageIcon: 'ico-model',
                pageTitle: 'Launch Play'
            },
            resolve:  {
                BuildProgressConfig: function($stateParams) {
                    var play_name = $stateParams.play_name || '';
                    return {
                        text: {
                            main_title: 'Your play is launching',
                            main_title_completed: 'Your play is launched',
                            button_goto: 'Go to Play Book'
                        },
                        button_goto_sref: 'home.playbook',
                        disable_create_button: true,
                        disable_cancel_button: true,
                        disable_steps: true,
                        disable_view_report: true
                    };
                }
            },
            views: {
                "main@": {
                    controller: 'ImportJobController',
                    templateUrl: 'app/create/buildprogress/BuildProgressView.html'
                }
            }
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
                WizardProgressItems: function(PlaybookWizardStore) {
                    return [
                        { label: 'Settings', state: 'settings', nextFn: PlaybookWizardStore.nextSaveGeneric },
                        { label: 'Segment', state: 'settings.segment', nextFn: PlaybookWizardStore.nextSaveGeneric },
                        { label: 'Rating', state: 'settings.segment.rating' },
                        { label: 'Targets', state: 'settings.segment.rating.targets' },
                        { label: 'Insights', state: 'settings.segment.rating.targets.insights', nextFn: PlaybookWizardStore.nextSaveInsight },
                        { label: 'Preview', state: 'settings.segment.rating.targets.insights.preview' },
                        { label: 'Launch', state: 'settings.segment.rating.targets.insights.preview.launch', nextFn: PlaybookWizardStore.nextLaunch }
                    ];
                }
            },
            views: {
                'summary@': {
                    controller: function($scope, PlaybookWizardStore) {
                        $scope.$on('$destroy', function () {
                            PlaybookWizardStore.clear();
                        });
                    }
                },
                'main@': {
                    resolve: {
                        WizardHeaderTitle: function() {
                            return 'Create Play';
                        },
                        WizardContainerId: function() {
                            return 'playbook';
                        }
                    },
                    controller: 'ImportWizard',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/wizard.component.html'
                },
                'wizard_progress@home.playbook.wizard': {
                    controller: 'ImportWizardProgress',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/progress/progress.component.html'
                },
                'wizard_controls@home.playbook.wizard': {
                    resolve: {
                        WizardControlsOptions: function() {
                            return { backState: 'home.playbook', nextState: 'home.playbook' };
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
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Play Book',
                section: 'wizard.targets'
            },
            views: {
                'wizard_content@home.playbook.wizard': {
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
                'wizard_content@home.playbook.wizard': {
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
                'wizard_content@home.playbook.wizard': {
                    controller: 'PlaybookWizardRating',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/rating/rating.component.html'
                }
            }
        })
        .state('home.playbook.wizard.settings.segment.rating.targets', {
            url: '/targets',
            resolve: {
                SegmentServiceProxy: ['SegmentService', 'QueryStore', function(SegmentService, QueryStore) {
                    var CreateOrUpdateSegment = function() {
                        var segment = QueryStore.getSegment(),
                            ts = new Date().getTime(),
                            restriction = QueryStore.getRestriction();

                        //console.log("[resolve] SegmentServiceProxy",segment);
                        
                        if (segment === null) {
                            segment = {
                                'name': 'segment' + ts,
                                'display_name': 'segment' + ts,
                                'frontend_restriction': restriction,
                                'page_filter': {
                                    'row_offset': 0,
                                    'num_rows': 10
                                }
                            };
                        } else {

                            segment = {
                                'name': segment.name,
                                'display_name': segment.display_name,
                                'frontend_restriction': segment.frontend_restriction,
                                'page_filter': {
                                    'row_offset': 0,
                                    'num_rows': 10
                                }
                            };
                        }

                        return SegmentService.CreateOrUpdateSegment(segment);
                    };

                    return {
                        CreateOrUpdateSegment: CreateOrUpdateSegment
                    };
                }],
                CountWithoutSalesForce: ['$q', 'QueryStore', function($q, QueryStore){

                    var deferred = $q.defer(),
                        restriction = QueryStore.getRestriction(),
                        query = {
                            'free_form_text_search': '',
                            'frontend_restriction': restriction,
                            'page_filter': {
                                'num_rows': 1000000,
                                'row_offset': 0
                            },
                            'restrict_without_sfdcid': true
                        };

                    QueryStore.GetCountByQuery('accounts', query).then(function(response){ 
                        deferred.resolve(response);
                    }); 

                    return deferred.promise;

                }],
                AccountsCount: ['$q', 'QueryStore', function($q, QueryStore) {
                    var deferred = $q.defer();

                    QueryStore.GetCountByQuery('accounts').then(function(data){ 
                        deferred.resolve(data);
                    });

                    return deferred.promise;
                }],
                Config: [function() {
                    return { 
                            header: {
                                class: 'playbook-targets',
                                label: 'Targets'
                            }
                        }
                }]
            },
            views: {
                'wizard_content@home.playbook.wizard': {
                    // controller: 'PlaybookWizardTargets',
                    // controllerAs: 'vm',
                    // templateUrl: 'app/playbook/content/targets/targets.component.html'
                    controller: 'QueryResultsCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/queryresults.component.html'
                }
            }
        })
        .state('home.playbook.wizard.settings.segment.rating.targets.insights', {
            url: '/insights',
            params: {
                section: 'wizard.insights'
            },
            resolve: {
                TalkingPoints: function(CgTalkingPointStore, $stateParams) {
                    var play_name = $stateParams.play_name || '';
                    return CgTalkingPointStore.getTalkingPoints(play_name);
                },
                TalkingPointAttributes: function (CgTalkingPointStore) {
                    return CgTalkingPointStore.getAttributes();
                },
                TalkingPointPreviewResources: function(CgTalkingPointStore) {
                    return CgTalkingPointStore.getTalkingPointsPreviewResources();
                },
                loadTinyMce: function($ocLazyLoad) {
                    return $ocLazyLoad.load('lib/js/tinymce/tinymce.min.js');
                },
                loadUiTinyMce: function($ocLazyLoad) {
                    return $ocLazyLoad.load('lib/js/tinymce/uitinymce.min.js');
                }
            },
            views: {
                'wizard_content@home.playbook.wizard': {
                    controller: 'PlaybookWizardInsights',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/insights/insights.component.html'
                }
            }
        })
        .state('home.playbook.wizard.settings.segment.rating.targets.insights.preview', {
            url: '/preview',
            resolve: {
                Play: function(PlaybookWizardStore) {
                    return PlaybookWizardStore.getCurrentPlay();
                },
                TalkingPointPreviewResources: function(CgTalkingPointStore) {
                    return CgTalkingPointStore.getTalkingPointsPreviewResources();
                }
            },
            views: {
                'wizard_content@home.playbook.wizard': {
                    controller: 'PlaybookWizardPreview',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/preview/preview.component.html'
                }
            }
        })
        .state('home.playbook.wizard.settings.segment.rating.targets.insights.preview.launch', {
            url: '/launch',
            views: {
                'wizard_content@home.playbook.wizard': {
                    templateUrl: 'app/playbook/content/launch/launch.component.html'
                }
            }
        })
});