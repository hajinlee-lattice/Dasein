angular
.module('lp.playbook', [
    'common.wizard',
    'lp.cg.talkingpoint',
    'lp.playbook.playlisttabs',
    'lp.playbook.plays',
    'lp.playbook.dashboard',
    'lp.playbook.dashboard.launchhistory',
    'lp.playbook.dashboard.sidebar',
    'lp.playbook.wizard.settings',
    'lp.playbook.wizard.rating',
    'lp.playbook.wizard.targets',
    'lp.playbook.wizard.crmselection',
    'lp.playbook.wizard.insights',
    'lp.playbook.wizard.preview',
    'lp.playbook.wizard.launch'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.playbook', {
            url: '/playbook',
            onExit: function(FilterService) {
                FilterService.clear();
            },
            resolve: {
                Model: function(){
                    return null;
                },
                IsPmml: function(){
                    return null;
                },
                HasRatingsAvailable: function(){
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
                    templateUrl: 'app/playbook/content/playlist/playlist.component.html'
                }
            }
        })
        .state('home.playbook.plays.launchhistory', {
            url: '/launchhistory',
            resolve: {
                LaunchHistoryData: function($q, $stateParams, PlaybookWizardStore) {
                    var deferred = $q.defer(),
                        params = {
                            playName: '',
                            sortby: 'created',
                            descending: true,
                            offset: 0,
                            max: 10
                        };

                    PlaybookWizardStore.getPlayLaunches(params).then(function(result){
                        deferred.resolve(result);
                    });
                    return deferred.promise;
                },
                LaunchHistoryCount: function($q, $stateParams, PlaybookWizardStore) {
                    var deferred = $q.defer(),
                        params = {
                            playName: '',
                            startTimestamp: 0
                        };

                    PlaybookWizardStore.getPlayLaunchCount(params).then(function(result){
                        deferred.resolve(result);
                    });
                    return deferred.promise;
                },
                FilterData: function($q, $timeout, $stateParams, PlaybookWizardStore, LaunchHistoryData) {

                    var deferred = $q.defer(),
                        filterItems = [],
                        launches = LaunchHistoryData,
                        uniqueLookupIdMapping = launches.uniqueLookupIdMapping,
                        allCountQuery = {
                            offset: 0,
                            startTimestamp: 0,
                            orgId: '',
                            externalSystemType: ''
                        }

                    PlaybookWizardStore.getPlayLaunchCount(allCountQuery).then(function(result) {
                        filterItems.push({ 
                            label: "All", 
                            action: { destinationOrgId: '' },
                            total: result.toString()
                        });
                    });

                    $timeout(function(){
                    
                        angular.forEach(uniqueLookupIdMapping, function(value, key) {
                            angular.forEach(value, function(val, index) {

                                var countParams = {
                                    playName: $stateParams.play_name,
                                    offset: 0,
                                    startTimestamp: 0,
                                    orgId: val.orgId,
                                    externalSysType: val.externalSystemType
                                }
                                PlaybookWizardStore.getPlayLaunchCount(countParams).then(function(result) {
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
        .state('home.playbook.dashboard', {
            url: '/dashboard/:play_name',
            params: {
                pageIcon: 'ico-insights',
                pageTitle: 'Play Overview',
                play_name: ''
            },
            onEnter: ['Play', 'BackStore', function(Play, BackStore) {
                BackStore.setBackLabel(Play.displayName);
                BackStore.setBackState('home.playbook');
                BackStore.setHidden(false);

                // var strings = ResourceStringsUtility.getString;
                // var play_name = $stateParams.play_name || '';
                // var segment = Play.segment;
                // var targetsDisabled = Play.ratingEngine ? false : true;

                // console.log('enter playbook.dashboard');
                // SidebarStore.set([{
                //         label: strings('NAVIGATION_SIDEBAR_LP_PLAYBOOK_PLAY_OVERVIEW'),
                //         sref: "home.attributes.activate",
                //         icon: "ico-analysis ico-light-gray"
                //     },{
                //         label: strings('NAVIGATION_SIDEBAR_LP_PLAYBOOK_TARGETS'),
                //         sref: "home.attributes.enable",
                //         icon: "ico-analysis ico-light-gray"
                //     },{
                //         label: strings('NAVIGATION_SIDEBAR_LP_PLAYBOOK_INSIGHTS'),
                //         sref: "home.attributes.enable",
                //         icon: "ico-analysis ico-light-gray"
                //     },{
                //         label: strings('NAVIGATION_SIDEBAR_LP_PLAYBOOK_LAUNCH_HISTORY'),
                //         sref: "home.attributes.enable",
                //         icon: "ico-analysis ico-light-gray"
                //     }
                // ]);
            }],
            resolve: {
                Play: function($q, $stateParams, PlaybookWizardStore) {
                    var deferred = $q.defer(),
                        playName = $stateParams.play_name;

                    PlaybookWizardStore.getPlay(playName, true).then(function(result){
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
                Entities : function(){
                    return ['account','recommendation','variable'];
                },
                TalkingPoints: function(CgTalkingPointStore, $stateParams) {
                    var play_name = $stateParams.play_name || '';
                    return CgTalkingPointStore.getTalkingPoints(play_name);
                },
                TalkingPointAttributes: function (CgTalkingPointStore, Entities) {
                    return CgTalkingPointStore.getAttributes(Entities);
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
                pageTitle: 'Available Targets',
                section: 'dashboard.targets',
                segment: '',
                play_name: ''
            },
            resolve: {
                Accounts: ['$q', '$stateParams', 'PlaybookWizardService', 'PlaybookWizardStore', function($q, $stateParams, PlaybookWizardService, PlaybookWizardStore) {
                    var deferred = $q.defer();

                    PlaybookWizardStore.getPlay($stateParams.play_name, true).then(function(data){

                        var engineId = data.ratingEngine.id,
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

                        PlaybookWizardService.getTargetData(engineId, query).then(function(data){ 
                            PlaybookWizardStore.setTargetData(data.data);
                            deferred.resolve(PlaybookWizardStore.getTargetData());
                        });

                    });

                    return deferred.promise;

                }],
                NoSFIdsCount: [function(){
                    return null;
                }],
                AccountsCoverage: ['$q', '$stateParams', 'PlaybookWizardStore', function($q, $stateParams, PlaybookWizardStore) {

                    var deferred = $q.defer();

                    PlaybookWizardStore.getPlay($stateParams.play_name, true).then(function(data){
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

                    PlaybookWizardStore.getPlay($stateParams.play_name, true).then(function(play){
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
                pageTitle: 'Available Targets',
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
        .state('home.playbook.dashboard.launchhistory', {
            url: '/launchhistory',
            params: {
                pageIcon: 'ico-refine',
                pageTitle: 'Launch History',
                section: 'dashboard.launchhistory'
            },
            resolve: {
                LaunchHistoryData: function($q, $stateParams, PlaybookWizardStore) {
                    var deferred = $q.defer(),
                        params = {
                            playName: $stateParams.play_name,
                            sortBy: 'created',
                            descending: true,
                            offset: 0,
                            max: 10
                        };
                    PlaybookWizardStore.getPlayLaunches(params).then(function(result){
                        deferred.resolve(result);
                    });
                    return deferred.promise;
                }, 
                LaunchHistoryCount: function($q, $stateParams, PlaybookWizardStore) {
                    var deferred = $q.defer(),
                        params = {
                            playName: $stateParams.play_name,
                            startTimestamp: 0
                        };

                    PlaybookWizardStore.getPlayLaunchCount(params).then(function(result){
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
        .state('home.playbook.create', {
            url: '/create/:play_name',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Playbook',
                play_name: ''
            },
            onExit: function($transition$, PlaybookWizardStore){
                var to = $transition$._targetState._definition.name;

                if (!to.includes('home.playbook.create')){
                    PlaybookWizardStore.clear();                
                }
            },
            resolve: {
                WizardValidationStore: function(PlaybookWizardStore) {
                    return PlaybookWizardStore;
                },
                WizardProgressContext: function() {
                    return 'playbook.create';
                },
                WizardProgressItems: function(PlaybookWizardStore) {
                    return [
                        { label: 'Rating', state: 'rating', nextFn: PlaybookWizardStore.nextSaveGeneric, progressDisabled: true },
                        { label: 'CRM System', state: 'rating.crmselection', nextFn: PlaybookWizardStore.nextSaveGeneric, progressDisabled: true },
                        { label: 'Targets', state: 'rating.crmselection.targets', progressDisabled: true },
                        { label: 'Insights', state: 'rating.crmselection.targets.insights', progressDisabled: true},
                        { label: 'Preview', state: 'rating.crmselection.targets.insights.preview', progressDisabled: true },
                        { label: 'Launch', state: 'rating.crmselection.targets.insights.preview.launch', nextFn: PlaybookWizardStore.nextLaunch, progressDisabled: true }
                    ];
                },
                WizardControlsOptions: function() {
                    return { 
                        backState: 'home.playbook', 
                        nextState: 'home.playbook' 
                    };
                },
                WizardHeaderTitle: function() {
                    return 'Create Play';
                },
                WizardContainerId: function() {
                    return 'playbook';
                },
                DisableWizardNavOnLastStep: function () {
                    return null;
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
            redirectTo: 'home.playbook.create.rating'
        })
        .state('home.playbook.create.rating', {
            url: '/rating/:rating_id',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Playbook',
                rating_id: ''
            },
            resolve: {
                Ratings: function(PlaybookWizardStore) {
                    return PlaybookWizardStore.getRatings({active: true});
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
        .state('home.playbook.create.rating.crmselection', {
            url: '/crm-selection',
            params: {
                section: 'wizard.insights',
                pageIcon: 'ico-playbook',
                pageTitle: 'Playbook'
            },
            resolve: {
                orgs: function($q, SfdcStore) {
                    var deferred = $q.defer();

                    SfdcStore.getOrgs().then(function (result) {
                        
                        deferred.resolve(result.CRM);
                        
                        // var orgs = result.CRM;

                        // angular.forEach(orgs, function(value, key) {
                        //     if (value.isRegistered === false) {
                        //         orgs.splice(key, 1);
                        //     }
                        // });

                        // orgs.filter(function(vendor){ return vendor.Name === "Magenic" });

                        // deferred.resolve(orgs);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'wizard_content@home.playbook.create': 'crmSelection'
            }
        })
        .state('home.playbook.create.rating.crmselection.targets', {
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

                    var deferred = $q.defer();

                    PlaybookWizardStore.getPlay($stateParams.play_name, true).then(function(play){

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

                        PlaybookWizardService.getTargetData(engineId, query).then(function(data){ 
                            PlaybookWizardStore.setTargetData(data.data);
                            deferred.resolve(PlaybookWizardStore.getTargetData());

                        });

                    });

                    return deferred.promise;

                }],
                //
                //
                //
                // Keep this commented out code for future use with the "Exclude accounts without SalesForce ID checkbox"
                //
                //
                //
                // NoSFIdsCount: ['$q', '$stateParams', 'PlaybookWizardService', 'PlaybookWizardStore', function($q, $stateParams, PlaybookWizardService, PlaybookWizardStore) {
                    
                //     var deferred = $q.defer();

                //     PlaybookWizardStore.getPlay($stateParams.play_name, true).then(function(data){

                //         var engineId = data.ratingEngine.id,
                //             query = {
                //                 free_form_text_search: '',
                //                 restrictNotNullSalesforceId: true,
                //                 entityType: 'Account',
                //                 bucketFieldName: 'ScoreBucket',
                //                 maximum: 1000000,
                //                 offset: 0,
                //                 sortBy: 'CompanyName',
                //                 descending: false
                //             };

                //         PlaybookWizardService.getTargetData(engineId, query).then(function(data){ 
                //             deferred.resolve(data.data.length);
                //         });

                //     });

                //     return deferred.promise;

                // }],
                NoSFIdsCount: [function(){
                    return null;
                }],
                AccountsCoverage: ['$q', '$stateParams', 'PlaybookWizardStore', function($q, $stateParams, PlaybookWizardStore) {

                    var deferred = $q.defer();

                    PlaybookWizardStore.getPlay($stateParams.play_name, true).then(function(data){

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
                "wizard_content@home.playbook.create": {
                    controller: 'QueryResultsCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/queryresults.component.html'
                }
            }
        })
        .state('home.playbook.create.rating.crmselection.targets.insights', {
            url: '/insights',
            params: {
                section: 'wizard.insights',
                pageIcon: 'ico-playbook',
                pageTitle: 'Playbook'
            },
            resolve: {
                Entities : function(){
                    return ['account','recommendation','variable'];
                },
                TalkingPoints: function(CgTalkingPointStore, $stateParams) {
                    var play_name = $stateParams.play_name || '';
                    return CgTalkingPointStore.getTalkingPoints(play_name);
                },
                TalkingPointAttributes: function (CgTalkingPointStore, Entities) {
                    return CgTalkingPointStore.getAttributes(Entities);
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
                'wizard_content@home.playbook.create': {
                    controller: 'PlaybookWizardInsights',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/insights/insights.component.html'
                }
            }
        })
        .state('home.playbook.create.rating.crmselection.targets.insights.preview', {
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
                'wizard_content@home.playbook.create': {
                    controller: 'PlaybookWizardPreview',
                    controllerAs: 'vm',
                    templateUrl: 'app/playbook/content/preview/preview.component.html'
                }
            }
        })
        .state('home.playbook.create.rating.crmselection.targets.insights.preview.launch', {
            url: '/launch',
            views: {
                'wizard_content@home.playbook.create': {
                    templateUrl: 'app/playbook/content/launch/launch.component.html'
                }
            }
        })
});