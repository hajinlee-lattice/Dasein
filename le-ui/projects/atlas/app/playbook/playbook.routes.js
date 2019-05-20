import { actions, reducer } from './playbook.redux';
import { store, injectAsyncReducer } from 'store';
import 'atlas/react/react-angular-main.component';

angular
    .module('lp.playbook', [
        'common.wizard',
        'lp.cg.talkingpoint',
        'lp.playbook.playlisttabs',
        'lp.playbook.plays',
        //'lp.playbook.overview',
        'lp.playbook.dashboard',
        'lp.playbook.dashboard.launchhistory',
        'lp.playbook.dashboard.sidebar',
        'lp.playbook.wizard.settings',
        'lp.playbook.wizard.rating',
        'lp.playbook.wizard.segmentcreate',
        'lp.playbook.wizard.targets',
        'lp.playbook.wizard.name',
        'lp.playbook.wizard.crmselection',
        'lp.playbook.wizard.insights',
        'lp.playbook.wizard.preview',
        'lp.playbook.wizard.launch',
        'lp.playbook.wizard.newlaunch',
        'mainApp.core.redux'
    ])
    .config(function ($stateProvider) {
        $stateProvider
            .state('home.playbook', {
                url: '/playbook',
                onExit: function ($state, FilterService) {
                    FilterService.clear();
                    // $state.get('home.playbook').data.redux.unsubscribe();
                },
                resolve: {
                    // playstore: ($q, $state, ReduxService) => {
                    //     var deferred = $q.defer();

                    //     deferred.resolve(ReduxService.connect(
                    //         'playbook',
                    //         actions,
                    //         reducer,
                    //         $state.get('home.playbook.overview')
                    //     ));

                    //     return deferred.promise;
                    // },
                    playstore: ($q, $state, ReduxService) => {
                        var deferred = $q.defer();

                        injectAsyncReducer(store, 'playbook', reducer);
                        let playstore = store.getState()['playbook'];
                        deferred.resolve(playstore);

                        return deferred.promise;
                    },
                    Model: function () {
                        return null;
                    },
                    IsPmml: function () {
                        return null;
                    },
                    HasRatingsAvailable: function () {
                        return null;
                    }
                },
                redirectTo: 'home.playbook.plays'
            })
            .state('home.playbook.plays', {
                url: '/plays',
                views: {
                    "summary@": {
                        controller: 'PlayListTabsController',
                        controllerAs: 'vm',
                        templateUrl: 'app/playbook/content/playlist/tabs/playlisttabs.component.html'
                    }
                },
                redirectTo: 'home.playbook.plays.list'
            })
            .state('home.playbook.plays.list', {
                url: '/list',
                params: {
                    pageIcon: 'ico-playbook',
                    pageTitle: 'Campaign Playbook'
                },
                resolve: {
                    PlayList: function ($q, PlaybookWizardStore) {
                        var deferred = $q.defer();

                        PlaybookWizardStore.getPlays().then(function (result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }
                },
                views: {
                    "main@": {
                        controller: 'PlayListController',
                        controllerAs: 'vm',
                        templateUrl: 'app/playbook/content/playlist/playlist.component.html'
                    }
                }
            })
            .state('home.playbook.plays.launchhistory', {
                url: '/launchhistory',
                resolve: {
                    LaunchHistoryData: function ($q, $stateParams, PlaybookWizardStore) {
                        var deferred = $q.defer(),
                            params = {
                                playName: '',
                                launchStates: 'Launching,Launched,Failed,Syncing,PartialSync,SyncFailed,Synced',
                                sortby: 'created',
                                descending: true,
                                offset: 0,
                                max: 10
                            };

                        PlaybookWizardStore.getPlayLaunches(params).then(function (result) {
                            deferred.resolve(result);
                        });
                        return deferred.promise;
                    },
                    LaunchHistoryCount: function ($q, $stateParams, PlaybookWizardStore) {
                        var deferred = $q.defer(),
                            params = {
                                playName: '',
                                launchStates: 'Launching,Launched,Failed,Syncing,PartialSync,SyncFailed,Synced',
                                startTimestamp: 0
                            };

                        PlaybookWizardStore.getPlayLaunchCount(params).then(function (result) {
                            deferred.resolve(result);
                        });
                        return deferred.promise;
                    },
                    FilterData: function ($q, $timeout, $stateParams, PlaybookWizardStore, LaunchHistoryData) {

                        var deferred = $q.defer(),
                            filterItems = [],
                            launches = LaunchHistoryData,
                            uniqueLookupIdMapping = launches.uniqueLookupIdMapping,
                            allCountQuery = {
                                launchStates: 'Launching,Launched,Failed,Syncing,PartialSync,SyncFailed,Synced',
                                offset: 0,
                                startTimestamp: 0,
                                orgId: '',
                                externalSystemType: ''
                            }

                        PlaybookWizardStore.getPlayLaunchCount(allCountQuery).then(function (result) {
                            filterItems.push({
                                label: "All",
                                action: { destinationOrgId: '' },
                                total: result.toString()
                            });
                        });

                        $timeout(function () {

                            angular.forEach(uniqueLookupIdMapping, function (value, key) {
                                angular.forEach(value, function (val, index) {
                                    if (val.orgName) {
                                        var countParams = {
                                            playName: $stateParams.play_name,
                                            launchStates: 'Launching,Launched,Failed,Syncing,PartialSync,SyncFailed,Synced',
                                            offset: 0,
                                            startTimestamp: 0,
                                            orgId: val.orgId,
                                            externalSysType: val.externalSystemType
                                        }
                                        PlaybookWizardStore.getPlayLaunchCount(countParams).then(function (result) {
                                            filterItems.push({
                                                label: val.orgName,
                                                data: {
                                                    orgName: val.orgName,
                                                    externalSystemType: val.externalSystemType,
                                                    destinationOrgId: val.orgId
                                                },
                                                action: {
                                                    destinationOrgId: val.orgId
                                                },
                                                total: result.toString()
                                            });
                                        });
                                    }
                                });
                            });
                        }, 250);

                        deferred.resolve(filterItems);

                        return deferred.promise;

                    }
                },
                views: {
                    'main@': {
                        controller: 'PlaybookDashboardLaunchHistory',
                        controllerAs: 'vm',
                        templateUrl: 'app/playbook/content/launchhistory/launchhistory.component.html'
                    }
                }
            })
            .state('home.playbook.overview', {
                url: '/overview/:play_name',
                params: {
                    pageIcon: 'ico-insights',
                    pageTitle: 'Campaign Overview',
                    play_name: ''
                },
                onEnter: function (BackStore, Play) {
                    BackStore.setBackLabel(Play.displayName);
                    BackStore.setBackState('home.playbook');
                    BackStore.setHidden(false);
                },
                onExit: function ($state) {
                    //$state.get('home.playbook.overview').data.redux.unsubscribe();
                },
                resolve: {
                    play: ($q, $stateParams, playstore) => {
                        var deferred = $q.defer();

                        actions.fetchPlay($stateParams.play_name, deferred);

                        return deferred.promise;
                    },
                    Play: (play) => { 
                        return play; 
                    },
                    path: () => {
                        return 'playbookOverview';
                    },
                    ngservices: (PlaybookWizardStore) => {
                        let obj = {
                            PlaybookWizardStore: PlaybookWizardStore
                        }
                        return obj;
                    }
                },

                views: {
                    "navigation@home": {
                        controller: 'SidebarPlaybookController',
                        controllerAs: 'vm',
                        templateUrl: 'app/playbook/content/dashboard/sidebar/sidebar.component.html'
                    },
                    'main@': {
                        component: 'reactAngularMainComponent'
                    },
                    'header.back@': 'backNav'
                }
            })
            .state('home.playbook.dashboard', {
                url: '/dashboard/:play_name',
                params: {
                    pageIcon: 'ico-insights',
                    pageTitle: 'Campaign Overview',
                    play_name: ''
                },
                onEnter: ['Play', 'BackStore', function (Play, BackStore) {
                    BackStore.setBackLabel(Play.displayName);
                    BackStore.setBackState('home.playbook');
                    BackStore.setHidden(false);
                }],
                resolve: {
                    Play: function ($q, $stateParams, PlaybookWizardStore) {
                        var deferred = $q.defer(),
                            playName = $stateParams.play_name;

                        PlaybookWizardStore.getPlay(playName, true).then(function (result) {
                            deferred.resolve(result);
                        });
                        return deferred.promise;
                    },
                    CampaignTypes: function ($q, PlaybookWizardStore) {
                        var deferred = $q.defer();

                        PlaybookWizardStore.getTypes().then(function (result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    },
                    CampaignGroups: function ($q, PlaybookWizardStore) {
                        var deferred = $q.defer();

                        PlaybookWizardStore.getGroups().then(function (result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }
                },
                views: {
                    "navigation@home": {
                        controller: 'SidebarPlaybookController',
                        controllerAs: 'vm',
                        templateUrl: 'app/playbook/content/dashboard/sidebar/sidebar.component.html'
                    },
                    'main@': {
                        controller: 'PlaybookDashboard',
                        controllerAs: 'vm',
                        templateUrl: 'app/playbook/content/dashboard/dashboard.component.html'
                    },
                    'header.back@': 'backNav'
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
                    Entities: function () {
                        return ['account', 'recommendation', 'variable'];
                    },
                    TalkingPoints: function (CgTalkingPointStore, $stateParams) {
                        var play_name = $stateParams.play_name || '';
                        return CgTalkingPointStore.getTalkingPoints(play_name);
                    },
                    TalkingPointAttributes: function (CgTalkingPointStore, Entities) {
                        return CgTalkingPointStore.getAttributes(Entities);
                    },
                    TalkingPointPreviewResources: function (CgTalkingPointStore) {
                        return CgTalkingPointStore.getTalkingPointsPreviewResources();
                    },
                    loadTinyMce: function ($ocLazyLoad) {
                        return $ocLazyLoad.load('lib/js/tinymce/tinymce.min.js');
                    },
                    loadUiTinyMce: function ($ocLazyLoad) {
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
                    Play: function (PlaybookWizardStore, $stateParams) {
                        return PlaybookWizardStore.getPlay($stateParams.play_name, true);
                    },
                    TalkingPointPreviewResources: function (CgTalkingPointStore) {
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
                    Segments: function (SegmentService) {
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
                    pageTitle: 'Available Targets',
                    section: 'dashboard.targets',
                    segment: '',
                    play_name: ''
                },
                resolve: {
                    Accounts: ['$q', '$stateParams', 'PlaybookWizardService', 'PlaybookWizardStore', function ($q, $stateParams, PlaybookWizardService, PlaybookWizardStore) {
                        var deferred = $q.defer();
                        deferred.resolve([]);
                        return deferred.promise;

                        // PlaybookWizardStore.getPlay($stateParams.play_name, true).then(function(data){

                        //     var engineId = data.ratingEngine.id,
                        //         query = { 
                        //             free_form_text_search: '',
                        //             restrictNotNullSalesforceId: false,
                        //             entityType: 'Account',
                        //             bucketFieldName: 'ScoreBucket',
                        //             maximum: 10,
                        //             offset: 0,
                        //             sortBy: 'CompanyName',
                        //             descending: false
                        //         };

                        //     PlaybookWizardService.getTargetData(engineId, query).then(function(data){ 
                        //         PlaybookWizardStore.setTargetData(data.data);
                        //         deferred.resolve(PlaybookWizardStore.getTargetData());
                        //     });

                        // });

                        // return deferred.promise;

                    }],
                    NoSFIdsCount: [function () {
                        return null;
                    }],
                    AccountsCoverage: ['$q', '$stateParams', 'PlaybookWizardStore', function ($q, $stateParams, PlaybookWizardStore) {
                        var deferred = $q.defer();

                        PlaybookWizardStore.getPlay($stateParams.play_name, { noSalesForceId: true }).then(function (data) {
                            if (data && data.ratingEngine && data.ratingEngine.id) {
                                var engineId = data.ratingEngine.id,
                                    engineIdObject = [{ id: engineId }];

                                PlaybookWizardStore.getRatingsCounts(engineIdObject, true).then(function (data) {
                                    var accountCount = (data.ratingEngineIdCoverageMap && data.ratingEngineIdCoverageMap[engineId] && data.ratingEngineIdCoverageMap[engineId].accountCount ? data.ratingEngineIdCoverageMap[engineId].accountCount : 0);
                                    deferred.resolve(accountCount);
                                });
                            } else {
                                deferred.resolve({});
                            }
                        });

                        return deferred.promise;

                    }],
                    Config: ['$q', '$stateParams', 'PlaybookWizardStore', function ($q, $stateParams, PlaybookWizardStore) {

                        var deferred = $q.defer();

                        PlaybookWizardStore.getPlay($stateParams.play_name, true).then(function (play) {
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
                views: {
                    "summary@": {
                        controller: 'TargetTabsController',
                        controllerAs: 'vm',
                        templateUrl: '/components/datacloud/query/results/targettabs.component.html'
                    }
                },
                redirectTo: 'home.playbook.dashboard.targets.accounts'
            })
            .state('home.playbook.dashboard.targets.accounts', {
                url: '/accounts',
                params: {
                    pageIcon: 'ico-targets',
                    pageTitle: 'Available Targets',
                    section: 'dashboard.targets',
                    currentTargetTab: 'accounts'
                },
                resolve: {
                    Contacts: [function () {
                        return null;
                    }],
                    // ContactsCount: [function(){
                    //     return null;
                    // }],
                    Config: [function () {
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
                    pageTitle: 'Available Targets',
                    section: 'dashboard.targets',
                    currentTargetTab: 'contacts'
                },
                resolve: {
                    Contacts: ['$q', '$stateParams', 'PlaybookWizardStore', function ($q, $stateParams, PlaybookWizardStore) {

                        var deferred = $q.defer();

                        PlaybookWizardStore.getPlay($stateParams.play_name).then(function (data) {

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

                            PlaybookWizardService.getTargetData(engineId, query).then(function (data) {
                                PlaybookWizardStore.setTargetData(data.data);
                                deferred.resolve(PlaybookWizardStore.getTargetData());
                            });

                        });

                        return deferred.promise;

                    }],
                    Accounts: [function () {
                        return null;
                    }],
                    AccountsCoverage: [function () {
                        return null;
                    }],
                    Config: [function () {
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
            .state('home.playbook.dashboard.launchhistory', {
                url: '/launchhistory',
                params: {
                    pageIcon: 'ico-refine',
                    pageTitle: 'Launch History',
                    section: 'dashboard.launchhistory'
                },
                resolve: {
                    LaunchHistoryData: function ($q, $stateParams, PlaybookWizardStore) {
                        var deferred = $q.defer(),
                            params = {
                                playName: $stateParams.play_name,
                                launchStates: 'Launching,Launched,Failed,Syncing,PartialSync,SyncFailed,Synced',
                                sortBy: 'created',
                                descending: true,
                                offset: 0,
                                max: 10
                            };
                        PlaybookWizardStore.getPlayLaunches(params).then(function (result) {
                            deferred.resolve(result);
                        });
                        return deferred.promise;
                    },
                    LaunchHistoryCount: function ($q, $stateParams, PlaybookWizardStore) {
                        var deferred = $q.defer(),
                            params = {
                                playName: $stateParams.play_name,
                                launchStates: 'Launching,Launched,Failed,Syncing,PartialSync,SyncFailed,Synced',
                                startTimestamp: 0
                            };

                        PlaybookWizardStore.getPlayLaunchCount(params).then(function (result) {
                            deferred.resolve(result);
                        });
                        return deferred.promise;
                    },
                    FilterData: [function () {
                        return null;
                    }],
                },
                views: {
                    "summary@": {
                        templateUrl: 'app/navigation/summary/BlankLine.html'
                    },
                    'main@': {
                        controller: 'PlaybookDashboardLaunchHistory',
                        controllerAs: 'vm',
                        templateUrl: 'app/playbook/content/launchhistory/launchhistory.component.html'
                    }
                }
            })
            .state('home.playbook.dashboard.launch_job', {
                url: '/launch/:applicationId/job',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Launch Campaign'
                },
                resolve: {
                    BuildProgressConfig: function ($stateParams) {
                        var play_name = $stateParams.play_name || '';
                        return {
                            text: {
                                main_title: 'Your campaign is launching',
                                main_title_completed: 'Your campaign is launched',
                                button_goto: 'Go to Campaigns'
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
            .state('home.playbook.create', {
                url: '/create/:play_name',
                params: {
                    pageIcon: 'ico-playbook',
                    pageTitle: 'Campaign Playbook',
                    play_name: ''
                },
                onExit: function ($transition$, PlaybookWizardStore) {
                    var to = $transition$._targetState._definition.name;

                    if (!to.includes('home.playbook.create')) {
                        PlaybookWizardStore.clear();
                    }
                },
                resolve: {
                    WizardValidationStore: function (PlaybookWizardStore) {
                        return PlaybookWizardStore;
                    },
                    WizardProgressContext: function () {
                        return 'playbook.create';
                    },
                    WizardProgressItems: function ($state, PlaybookWizardStore) {
                        return [
                            {
                                label: 'Segment',
                                state: 'segment',
                                nextFn: function (nextState) {
                                    $state.go(nextState); // we don't want to auto save anymore, but go to the next step (it'll be saved in the store)
                                },
                                //PlaybookWizardStore.nextSaveGeneric, 
                                progressDisabled: true
                            }, {
                                label: 'Model',
                                state: 'segment.rating',
                                nextFn: function (nextState) {
                                    $state.go(nextState);
                                },
                                progressDisabled: true
                            }, {
                                label: 'Name',
                                state: 'segment.rating.name',
                                secondaryLinkValidation: true,
                                secondaryLinkLabel: 'Save & Create Insights',
                                secondaryClass: 'aptrinsic-campaign-wizard-save-and-add-insights',
                                secondaryFn: function () {
                                    PlaybookWizardStore.nextSaveAndGoto('home.playbook.dashboard.insights', {
                                        include_play_name: true
                                    });
                                },
                                lastRoute: true,
                                nextFn: function () {
                                    PlaybookWizardStore.nextSaveAndGoto('home.playbook');
                                },
                                nextLabel: 'Save & Go to Campaign List',
                                nextClass: 'aptrinsic-campaign-wizard-save-and-campaign-list',
                                progressDisabled: true
                            }
                        ];
                    },
                    WizardControlsOptions: function () {
                        return {
                            backState: '',
                            nextState: 'home.playbook',
                            preventUnload: 'home.playbook'
                        };
                    },
                    WizardHeaderTitle: function () {
                        return 'Create Campaign';
                    },
                    WizardContainerId: function () {
                        return 'playbook';
                    }
                },
                views: {
                    'summary@': {
                        templateUrl: 'app/navigation/summary/BlankLine.html'
                    },
                    'main@': {
                        controller: 'ImportWizard',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/wizard.component.html'
                    },
                    'wizard_progress@home.playbook.create': {
                        resolve: {
                        },
                        controller: 'ImportWizardProgress',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/progress/progress.component.html'
                    },
                    'wizard_controls@home.playbook.create': {
                        resolve: {
                        },
                        controller: 'ImportWizardControls',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/controls/controls.component.html'
                    }
                },
                redirectTo: 'home.playbook.create.segment'
            })
            .state('home.playbook.create.segment', {
                url: '/segment/:segment_name',
                params: {
                    pageIcon: 'ico-playbook',
                    pageTitle: 'Campaign Playbook',
                    rating_id: '',
                    segment_name: ''
                },
                resolve: {
                    // Ratings: function(PlaybookWizardStore) {
                    //     return PlaybookWizardStore.getRatings({active: true});
                    // },
                    Segments: function (SegmentService) {
                        return SegmentService.GetSegments();
                    }
                },
                views: {
                    'wizard_content@home.playbook.create': {
                        controller: 'PlaybookWizardSegmentCreate',
                        controllerAs: 'vm',
                        templateUrl: 'app/playbook/content/segment_create/segment_create.component.html'
                    }
                }
            })
            .state('home.playbook.create.segment.rating', {
                url: '/rating/',
                params: {
                    pageIcon: 'ico-playbook',
                    pageTitle: 'Campaign Playbook',
                    rating_id: ''
                },
                resolve: {
                    Ratings: function (PlaybookWizardStore) {
                        return PlaybookWizardStore.getRatings({ active: true });
                    }
                },
                views: {
                    'wizard_content@home.playbook.create': {
                        controller: 'PlaybookWizardRating',
                        controllerAs: 'vm',
                        templateUrl: 'app/playbook/content/rating/rating.component.html'
                    }
                }
            })
            .state('home.playbook.create.segment.rating.name', {
                url: '/name',
                params: {
                    section: 'wizard.insights',
                    pageIcon: 'ico-playbook',
                    pageTitle: 'Campaign Playbook',
                },
                resolve: {
                    types: function ($q, PlaybookWizardStore) {
                        var deferred = $q.defer();

                        PlaybookWizardStore.getTypes().then(function (result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }
                },
                views: {
                    'wizard_content@home.playbook.create': 'name'
                }
            })
            .state('home.playbook.launch', {
                url: '/launch/:play_name',
                params: {
                    pageIcon: 'ico-playbook',
                    pageTitle: 'Campaign Playbook',
                    play_name: '',
                    status: 'Launch'
                },
                onEnter: function ($stateParams, PlaybookWizardStore) {
                    var play = PlaybookWizardStore.getCurrentPlay() || {};

                    if (play.name) {
                        var settings = {
                            name: play.name,
                            createdBy: play.createdBy
                        };

                        if (play.ratingEngine) {
                            if (play.ratingEngine.id == null) {
                                settings.ratingEngine = null;
                            }
                            else {
                                settings.ratingEngine = {
                                    id: play.ratingEngine.id
                                }
                            }
                        }

                        PlaybookWizardStore.setSettings(settings);
                    }
                },
                onExit: function ($transition$, PlaybookWizardStore) {
                    var to = $transition$._targetState._definition.name;

                    if (!to.includes('home.playbook.create')) {
                        PlaybookWizardStore.clear();
                    }
                },
                resolve: {
                    WizardValidationStore: function (PlaybookWizardStore) {
                        return PlaybookWizardStore;
                    },
                    WizardProgressContext: function () {
                        return 'playbook.launch';
                    },
                    WizardProgressItems: function ($state, PlaybookWizardStore) {
                        return [
                            {
                                label: 'System',  // CRM Selection
                                state: 'crmselection',
                                nextFn: function (nextState) {
                                    $state.go(nextState); // we don't want to auto save anymore, but go to the next step (it'll be saved in the store)
                                },
                                progressDisabled: true,
                            }, {
                                label: 'Suppressions', // Targets
                                state: 'crmselection.targets',
                                nextFn: function (nextState) {
                                    $state.go(nextState);
                                },
                                progressDisabled: true
                            }, {
                                label: 'Launch',
                                state: 'crmselection.targets.launch',
                                nextLabel: 'Save & Launch later',
                                nextFn: function (nextState) {
                                    PlaybookWizardStore.nextSaveLaunch(nextState, { saveOnly: true });
                                },
                                progressDisabled: true
                            }
                        ];
                    },
                    WizardControlsOptions: function () {
                        return {
                            backState: '', // '' takes you to lastFrom // 'home.playbook', 
                            nextState: 'home.playbook',
                            preventUnload: 'home.playbook'
                        };
                    },
                    WizardHeaderTitle: function ($stateParams) {
                        return $stateParams.status;
                    },
                    WizardContainerId: function () {
                        return 'playbook';
                    }
                },
                views: {
                    'summary@': {
                        templateUrl: 'app/navigation/summary/BlankLine.html'
                    },
                    'main@': {
                        controller: 'ImportWizard',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/wizard.component.html'
                    },
                    'wizard_progress@home.playbook.launch': {
                        resolve: {
                        },
                        controller: 'ImportWizardProgress',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/progress/progress.component.html'
                    },
                    'wizard_controls@home.playbook.launch': {
                        resolve: {
                        },
                        controller: 'ImportWizardControls',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/controls/controls.component.html'
                    }
                },
                redirectTo: 'home.playbook.launch.crmselection'
            })
            .state('home.playbook.launch.crmselection', {
                url: '/system',
                params: {
                    section: 'wizard.insights',
                    pageIcon: 'ico-playbook',
                    pageTitle: 'Campaign Playbook'
                },
                resolve: {
                    play: function ($q, $stateParams, PlaybookWizardStore) {
                        var deferred = $q.defer();

                        PlaybookWizardStore.getPlay($stateParams.play_name, true).then(function (play) {
                            PlaybookWizardStore.setRating(play.ratingEngine);
                            deferred.resolve(play);
                        });

                        return deferred.promise;
                    },
                    featureflags: function ($q, FeatureFlagService) {
                        var deferred = $q.defer();

                        FeatureFlagService.GetAllFlags().then(function (result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    },
                    orgs: function ($q, SfdcStore, featureflags) {
                        var deferred = $q.defer();
                        SfdcStore.getOrgs().then(function (result) {
                            let orgs = [];
                            let keys = Object.keys(result);
                            if (keys) {
                                keys.forEach((key) => {
                                    if(key !== 'FILE_SYSTEM' || (key == 'FILE_SYSTEM' && featureflags.AlphaFeature)){
                                        let ret = result[key] || [];
                                        orgs = orgs.concat(ret)
                                    }
                                    
                                });
                            }
                            deferred.resolve(orgs);
                        });

                        return deferred.promise;
                    }
                },
                views: {
                    'wizard_content@home.playbook.launch': 'crmSelection'
                }
            })
            .state('home.playbook.launch.crmselection.targets', {
                url: '/suppressions',
                params: {
                    section: 'wizard.targets',
                    currentTargetTab: 'accounts',
                    pageIcon: 'ico-playbook',
                    pageTitle: 'Campaign Playbook'
                },
                resolve: {
                    Config: [function () {
                        return {
                            header: {
                                class: 'playbook-targets',
                                label: 'Targets'
                            }
                        }
                    }],
                    Accounts: ['$q', '$stateParams', 'PlaybookWizardService', 'PlaybookWizardStore', function ($q, $stateParams, PlaybookWizardService, PlaybookWizardStore) {

                        var deferred = $q.defer();

                        PlaybookWizardStore.getPlay($stateParams.play_name, true).then(function (play) {
                            if (!play.ratingEngine) {
                                deferred.resolve(null);
                                return false;
                            }

                            var engineId = play.ratingEngine.id,
                                query = {
                                    free_form_text_search: '',
                                    restrictNotNullSalesforceId: false,
                                    entityType: 'Account',
                                    bucketFieldName: 'ScoreBucket',
                                    maximum: 10,
                                    offset: 0,
                                    sortBy: 'CompanyName',
                                    descending: false
                                };

                            PlaybookWizardService.getTargetData(engineId, query).then(function (data) {
                                PlaybookWizardStore.setTargetData(data.data);
                                deferred.resolve(PlaybookWizardStore.getTargetData());

                            });

                        });

                        return deferred.promise;

                    }],
                    NoSFIdsCount: [function () {
                        return null;
                    }],
                    AccountsCoverage: ['$q', '$stateParams', 'PlaybookWizardStore', 'PlaybookWizardService', 'QueryStore', function ($q, $stateParams, PlaybookWizardStore, PlaybookWizardService, QueryStore) {

                        var deferred = $q.defer();

                        PlaybookWizardStore.getPlay($stateParams.play_name, true).then(function (data) {
                            if (data.ratingEngine && data.ratingEngine.id) {
                                var engineId = data.ratingEngine.id,
                                    engineIdObject = [{ id: engineId }],
                                    getExcludeItems = PlaybookWizardStore.getExcludeItems(),
                                    getSegmentsOpts = {
                                        loadContactsCount: true,
                                        loadContactsCountByBucket: true
                                    };

                                if (getExcludeItems) {
                                    getSegmentsOpts.lookupId = PlaybookWizardStore.getDestinationAccountId();
                                    getSegmentsOpts.restrictNullLookupId = true;
                                }

                                var segmentName = PlaybookWizardStore.getCurrentPlay().targetSegment.name;
                                PlaybookWizardService.getRatingSegmentCounts(segmentName, [engineId], getSegmentsOpts).then(function (result) {
                                    var accountsCoverage = {};
                                    // result.ratingModelsCoverageMap[Object.keys(result.ratingModelsCoverageMap)[0]];
                                    accountsCoverage.errorMap = result.errorMap;
                                    accountsCoverage.ratingModelsCoverageMap = result.ratingModelsCoverageMap[Object.keys(result.ratingModelsCoverageMap)[0]];
                                    deferred.resolve(accountsCoverage);
                                });
                            } else {
                                // this should look like the return of getRatingSegmentCounts (this.host + '/ratingengines/coverage/segment/' +  segmentName)
                                var segment = PlaybookWizardStore.getCurrentPlay().targetSegment;
                                if (PlaybookWizardStore.getExcludeItems()) {
                                    var accountId = PlaybookWizardStore.crmselection_form.crm_selection.accountId,
                                        template = {
                                            account_restriction: {
                                                restriction: {
                                                    logicalRestriction: {
                                                        operator: "AND",
                                                        restrictions: []
                                                    }
                                                }
                                            },
                                            page_filter: {
                                                num_rows: 10,
                                                row_offset: 0
                                            }
                                        };
                                    template.account_restriction.restriction.logicalRestriction.restrictions.push(segment.account_restriction.restriction);
                                    template.account_restriction.restriction.logicalRestriction.restrictions.push({
                                        bucketRestriction: {
                                            attr: 'Account.' + accountId,
                                            bkt: {
                                                Cmp: 'IS_NOT_NULL',
                                                Id: 1,
                                                ignored: false,
                                                Vals: []
                                            }
                                        }
                                    });
                                    QueryStore.getEntitiesCounts(template).then(function (result) {
                                        deferred.resolve(
                                            PlaybookWizardStore.getCoverageMap({
                                                accounts: result.Account || 0,
                                                contacts: result.Contact || 0,
                                            }));
                                    });
                                }
                                else {
                                    deferred.resolve(
                                        PlaybookWizardStore.getCoverageMap({
                                            accounts: segment.accounts || 0,
                                            contacts: segment.contacts || 0
                                        }));
                                }
                            }
                        });

                        return deferred.promise;

                    }],
                    Contacts: [function () {
                        return null;
                    }],
                    orgs: ['$q', 'SfdcStore', 'featureflags', function ($q, SfdcStore, featureflags) {
                        var deferred = $q.defer();

                        SfdcStore.getOrgs().then(function (result) {
                            let orgs = [];
                            let keys = Object.keys(result);
                            if (keys) {
                                keys.forEach((key) => {
                                    let ret = result[key] || [];
                                    orgs = orgs.concat(ret)
                                });
                            }
                            deferred.resolve(orgs);
                        });

                        return deferred.promise;
                    }]
                },
                views: {
                    "wizard_content@home.playbook.launch": {
                        controller: 'QueryResultsCtrl',
                        controllerAs: 'vm',
                        templateUrl: '/components/datacloud/query/results/queryresults.component.html'
                    }
                }
            })
            .state('home.playbook.launch.crmselection.targets.launch', {
                url: '/launch',
                params: {
                    section: 'wizard.insights',
                    pageIcon: 'ico-playbook',
                    pageTitle: 'Campaign Playbook'
                },
                resolve: {
                    featureflags: function ($q, FeatureFlagService) {
                        var deferred = $q.defer();

                        FeatureFlagService.GetAllFlags().then(function (result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    },
                    username: function ($q, featureflags, PlaybookWizardStore, PlaybookWizardService) {
                        var deferred = $q.defer();

                        if (featureflags.EnableExternalIntegration && PlaybookWizardStore.isExternallyAuthenticatedOrg()) {
                            PlaybookWizardService.getTenantDropboxId().then(function (result) {
                                deferred.resolve(result);
                            });
                        } else {
                            deferred.resolve({});
                        }

                        return deferred.promise;
                    },
                    trayuser: function ($q, featureflags, username, PlaybookWizardStore, PlaybookWizardService) {
                        var deferred = $q.defer();

                        if (featureflags.EnableExternalIntegration && PlaybookWizardStore.isExternallyAuthenticatedOrg()) {
                            PlaybookWizardService.getTrayUser(username).then(function (result) {
                                deferred.resolve(result);
                            });
                        } else {
                            deferred.resolve({});
                        }
                        return deferred.promise;
                    }

                },
                views: {
                    'wizard_content@home.playbook.launch': 'newlaunch'
                }
            })
        // .state('home.playbook.create.rating.crmselection.targets.insights', {
        //     url: '/insights',
        //     params: {
        //         section: 'wizard.insights',
        //         pageIcon: 'ico-playbook',
        //         pageTitle: 'Playbook'
        //     },
        //     resolve: {
        //         Entities : function(){
        //             return ['account','recommendation','variable'];
        //         },
        //         TalkingPoints: function(CgTalkingPointStore, $stateParams) {
        //             var play_name = $stateParams.play_name || '';
        //             return CgTalkingPointStore.getTalkingPoints(play_name);
        //         },
        //         TalkingPointAttributes: function (CgTalkingPointStore, Entities) {
        //             return CgTalkingPointStore.getAttributes(Entities);
        //         },
        //         TalkingPointPreviewResources: function(CgTalkingPointStore) {
        //             return CgTalkingPointStore.getTalkingPointsPreviewResources();
        //         },
        //         loadTinyMce: function($ocLazyLoad) {
        //             return $ocLazyLoad.load('lib/js/tinymce/tinymce.min.js');
        //         },
        //         loadUiTinyMce: function($ocLazyLoad) {
        //             return $ocLazyLoad.load('lib/js/tinymce/uitinymce.min.js');
        //         }
        //     },
        //     views: {
        //         'wizard_content@home.playbook.create': {
        //             controller: 'PlaybookWizardInsights',
        //             controllerAs: 'vm',
        //             templateUrl: 'app/playbook/content/insights/insights.component.html'
        //         }
        //     }
        // })
        // .state('home.playbook.create.rating.crmselection.targets.insights.preview', {
        //     url: '/preview',
        //     params: {
        //         pageIcon: 'ico-playbook',
        //         pageTitle: 'Playbook'
        //     },
        //     resolve: {
        //         Play: function(PlaybookWizardStore) {
        //             return PlaybookWizardStore.getCurrentPlay();
        //         },
        //         TalkingPointPreviewResources: function(CgTalkingPointStore) {
        //             return CgTalkingPointStore.getTalkingPointsPreviewResources();
        //         }
        //     },
        //     views: {
        //         'wizard_content@home.playbook.create': {
        //             controller: 'PlaybookWizardPreview',
        //             controllerAs: 'vm',
        //             templateUrl: 'app/playbook/content/preview/preview.component.html'
        //         }
        //     }
        // })
        // .state('home.playbook.create.rating.crmselection.targets.insights.preview.launch', {
        //     url: '/launch',
        //     views: {
        //         'wizard_content@home.playbook.create': {
        //             templateUrl: 'app/playbook/content/launch/launch.component.html'
        //         }
        //     }
        // })
    });
