angular
.module('lp.playbook', [
    'common.wizard',
    'lp.cg.talkingpoint',
    'lp.playbook.playlisttabs',
    'lp.playbook.plays',
    'lp.playbook.dashboard',
    'lp.playbook.dashboard.launch_history',
    'lp.playbook.wizard.settings',
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
            views: {
                "summary@": {
                    controller: 'PlayListTabsController',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/playList/tabs/playlisttabs.component.html'
                }
            },
            redirectTo: 'home.playbook.plays.list'
        })
        .state('home.playbook.plays.list', {
            url: '/list',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Playbook'
            },
            resolve: {
                PlayList: function($q, PlaybookWizardStore) {
                    var deferred = $q.defer();

                    PlaybookWizardStore.getPlays().then(function(result) {
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
        .state('home.playbook.plays.launch_history', {
            url: '/launch_history',
            resolve: {
                LaunchHistoryData: function($q, $stateParams, PlaybookWizardStore) {
                    var deferred = $q.defer();

                    var params = {
                        playName: $stateParams.play_name
                    };
                    PlaybookWizardStore.getPlayLaunches(params).then(function(result){
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
                        $scope.targetsDisabled = (play.ratingEngine ? false : true);
                        $scope.stateName = function() {
                            return $state.current.name;
                        }
                        var launchedStatus = PlaybookWizardStore.getLaunchedStatus(play);
                        if($state.current.name === 'home.playbook.dashboard.launch_job') {
                            $scope.menuDisabled = true;
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
                    return PlaybookWizardStore.getPlay($stateParams.play_name, true);
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
                Accounts: ['$q', '$stateParams', 'PlaybookWizardService', 'PlaybookWizardStore', function($q, $stateParams, PlaybookWizardService, PlaybookWizardStore) {
                    var deferred = $q.defer();

                    PlaybookWizardStore.getPlay($stateParams.play_name).then(function(data){

                        var engineId = data.ratingEngine.id,
                            query = { 
                                free_form_text_search: '',
                                restrictNotNullSalesforceId: false,
                                entityType: 'Account',
                                bucketFieldName: 'ScoreBucket',
                                maximum: 10,
                                offset: 0,
                                sortBy: 'LDC_Name',
                                descending: false
                            };

                        PlaybookWizardService.getTargetData(engineId, query).then(function(data){ 
                            PlaybookWizardStore.setTargetData(data.data);
                            deferred.resolve(PlaybookWizardStore.getTargetData());
                        });

                    });

                    return deferred.promise;

                }],
                AccountsCoverage: ['$q', '$stateParams', 'PlaybookWizardStore', function($q, $stateParams, PlaybookWizardStore) {

                    var deferred = $q.defer();

                    PlaybookWizardStore.getPlay($stateParams.play_name).then(function(data){
                        var engineId = data.ratingEngine.id,
                            engineIdObject = [{id: engineId}];

                        PlaybookWizardStore.getRatingsCounts(engineIdObject, true).then(function(data){
                            var accountCount = (data.ratingEngineIdCoverageMap && data.ratingEngineIdCoverageMap[engineId] && data.ratingEngineIdCoverageMap[engineId].accountCount ? data.ratingEngineIdCoverageMap[engineId].accountCount : 0);
                            deferred.resolve(accountCount);
                        });
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
                            header: {
                                class: 'playbook-targets',
                                label: 'Targets'
                            }
                        };
                        deferred.resolve(config);
                    });

                    return deferred.promise;
                }],
            },
            redirectTo: 'home.playbook.dashboard.targets.accounts',
            views: {
                "summary@": {
                    controller: 'TargetTabsController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/targettabs.component.html'
                }
            }
        })
        .state('home.playbook.dashboard.targets.accounts', {
            url: '/accounts',
            params: {
                pageIcon: 'ico-targets',
                pageTitle: 'Avaliable Targets',
                section: 'dashboard.targets',
                currentTargetTab: 'accounts'
            },
            resolve: {
                Contacts: [function(){
                    return null;
                }],
                // ContactsCount: [function(){
                //     return null;
                // }],
                Config: [function(){
                    return null;
                }],
            },
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                'main@': {
                    controller: 'QueryResultsCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/queryresults.component.html'
                }
            }
        })
        .state('home.playbook.dashboard.targets.contacts', {
            url: '/contacts',
            params: {
                pageIcon: 'ico-targets',
                pageTitle: 'Avaliable Targets',
                section: 'dashboard.targets',
                currentTargetTab: 'contacts'
            },
            resolve: {
                Contacts: ['$q', '$stateParams', 'PlaybookWizardStore', function($q, $stateParams, PlaybookWizardStore) {

                    var deferred = $q.defer();

                    PlaybookWizardStore.getPlay($stateParams.play_name).then(function(data){

                        var engineId = data.ratingEngine.id,
                            query = { 
                                free_form_text_search: '',
                                restrictNotNullSalesforceId: false,
                                entityType: 'Contact',
                                bucketFieldName: 'ScoreBucket',
                                maximum: 15,
                                offset: 0,
                                sortBy: 'ContactName',
                                descending: false
                            };

                        PlaybookWizardService.getTargetData(engineId, query).then(function(data){ 
                            PlaybookWizardStore.setTargetData(data.data);
                            deferred.resolve(PlaybookWizardStore.getTargetData());
                        });

                    });

                    return deferred.promise;

                }],
                Accounts: [function(){
                    return null;
                }],
                AccountsCoverage: [function(){
                    return null;
                }],
                Config: [function(){
                    return null;
                }],
            },
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
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
                    var deferred = $q.defer(),
                        params = {
                            playName: $stateParams.play_name
                        };
                    PlaybookWizardStore.getPlayLaunches(params).then(function(result){
                        deferred.resolve(result);
                    });
                    return deferred.promise;
                }
            },
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
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
                            button_goto: 'Go to Playbook'
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
                "summary@": {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                "main@": {
                    controller: 'ImportJobController',
                    templateUrl: 'app/create/buildprogress/BuildProgressView.html'
                }
            }
        })
        .state('home.playbook.wizard', {
            url: '/wizard/:play_name',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Playbook'
            },
            resolve: {
                WizardValidationStore: function(PlaybookWizardStore) {
                    return PlaybookWizardStore;
                },
                WizardProgressContext: function() {
                    return 'playbook';
                },
                WizardProgressItems: function(PlaybookWizardStore) {
                    return [
                        { label: 'Rating', state: 'rating', nextFn: PlaybookWizardStore.nextSaveGeneric },
                        { label: 'Targets', state: 'rating.targets' },
                        { label: 'Insights', state: 'rating.targets.insights', nextFn: PlaybookWizardStore.nextSaveInsight },
                        { label: 'Preview', state: 'rating.targets.insights.preview' },
                        { label: 'Launch', state: 'rating.targets.insights.preview.launch', nextFn: PlaybookWizardStore.nextLaunch }
                    ];
                }
            },
            views: {
                'summary@': {
                    controller: function($scope, PlaybookWizardStore) {
                        $scope.$on('$destroy', function () {
                            PlaybookWizardStore.clear();
                        });
                    },
                    templateUrl: 'app/navigation/summary/BlankLine.html'
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
            redirectTo: 'home.playbook.wizard.rating'
        })
        .state('home.playbook.wizard.rating', {
            url: '/rating/:rating_id',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Playbook'
            },
            resolve: {
                Ratings: function(PlaybookWizardStore) {
                    return PlaybookWizardStore.getRatings(true);
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
        .state('home.playbook.wizard.rating.targets', {
            url: '/targets',
            params: {
                section: 'wizard.targets',
                currentTargetTab: 'accounts',
                pageIcon: 'ico-playbook',
                pageTitle: 'Playbook'
            },
            resolve: {
                Config: [function() {
                    return { 
                            header: {
                                class: 'playbook-targets',
                                label: 'Targets'
                            }
                        }
                }],
                Accounts: ['$q', '$stateParams', 'PlaybookWizardService', 'PlaybookWizardStore', function($q, $stateParams, PlaybookWizardService, PlaybookWizardStore) {

                    var deferred = $q.defer(),
                        savedRating = PlaybookWizardStore.getSavedRating(),
                        engineId = savedRating.id,
                        query = { 
                                free_form_text_search: '',
                                restrictNotNullSalesforceId: false,
                                entityType: 'Account',
                                bucketFieldName: 'ScoreBucket',
                                maximum: 10,
                                offset: 0,
                                sortBy: 'LDC_Name',
                                descending: false
                            };

                    PlaybookWizardStore.getPlay($stateParams.play_name).then(function(data){

                        var engineId = data.ratingEngine.id;

                        PlaybookWizardService.getTargetData(engineId, query).then(function(data){ 
                            PlaybookWizardStore.setTargetData(data.data);
                            deferred.resolve(PlaybookWizardStore.getTargetData());

                        });

                    });

                    return deferred.promise;

                }],
                AccountsCoverage: ['$q', '$stateParams', 'PlaybookWizardStore', function($q, $stateParams, PlaybookWizardStore) {

                    var deferred = $q.defer();

                    PlaybookWizardStore.getPlay($stateParams.play_name).then(function(data){

                        var engineId = data.ratingEngine.id,
                            engineIdObject = [{id: engineId}];

                        PlaybookWizardStore.getRatingsCounts(engineIdObject).then(function(data){
                            var accountsCoverage = (data.ratingEngineIdCoverageMap && data.ratingEngineIdCoverageMap[engineId] ? data.ratingEngineIdCoverageMap[engineId] : null);
                            deferred.resolve(accountsCoverage);
                        });
                    
                    });

                    return deferred.promise;

                }],
                Contacts: [function(){
                    return null;
                }]
            },
            views: {
                "wizard_content@home.playbook.wizard": {
                    controller: 'QueryResultsCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/queryresults.component.html'
                }
            }
        })
        .state('home.playbook.wizard.rating.targets.insights', {
            url: '/insights',
            params: {
                section: 'wizard.insights',
                pageIcon: 'ico-playbook',
                pageTitle: 'Playbook'
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
        .state('home.playbook.wizard.rating.targets.insights.preview', {
            url: '/preview',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Playbook'
            },
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
        .state('home.playbook.wizard.rating.targets.insights.preview.launch', {
            url: '/launch',
            views: {
                'wizard_content@home.playbook.wizard': {
                    templateUrl: 'app/playbook/content/launch/launch.component.html'
                }
            }
        })
});