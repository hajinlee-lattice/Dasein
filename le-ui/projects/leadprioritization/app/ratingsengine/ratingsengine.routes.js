angular
.module('lp.ratingsengine', [
    'common.wizard',
    'common.datacloud',
    'lp.models.segments',
    'lp.ratingsengine.ratingsenginetabs',
    'lp.ratingsengine.ratingslist',
    'lp.ratingsengine.creationhistory',
    'lp.ratingsengine.ratingsenginetype',
    'lp.ratingsengine.dashboard',
    'lp.ratingsengine.wizard.segment',
    'lp.ratingsengine.wizard.attributes',
    'lp.ratingsengine.wizard.summary'
])
.config(function($stateProvider, DataCloudResolvesProvider) {
    $stateProvider
        .state('home.ratingsengine', {
            url: '/ratings_engine',
            resolve: {
                RatingList: function($q, RatingsEngineStore) {
                    var deferred = $q.defer();

                    RatingsEngineStore.getRatings().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }   
            },
            views: {
                "summary@": {
                    controller: 'RatingsEngineTabsController',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/ratingslist/ratingsenginetabs.component.html'
                }
            },
            redirectTo: 'home.ratingsengine.ratingslist'
        })
        .state('home.ratingsengine.ratingslist', {
            url: '/ratings',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Rating Engine'
            },
            resolve: {
                RatingList: function($q, RatingsEngineStore) {
                    var deferred = $q.defer();

                    RatingsEngineStore.getRatings().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }   
            },
            views: {
                "main@": {
                    controller: 'RatingsEngineListController',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/ratingslist/ratingslist.component.html'
                }
            }
        })
        .state('home.ratingsengine.creationhistory', {
            url: '/creationhistory',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Creation History'
            },
            views: {
                "main@": {
                    controller: 'RatingsEngineCreationHistory',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/ratingslist/creationhistory.component.html'
                }
            }
        })
        .state('home.ratingsengine.ratingsenginetype', {
            url: '/ratingsenginetype',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Rating Engine Type'
            },
            views: {
                'summary@': {
                    template: ''
                },
                "main@": {
                    controller: 'RatingsEngineType',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/ratingsenginetype/ratingsenginetype.component.html'
                }
            }
        })
        .state('home.ratingsengine.dashboard', {
            url: '/dashboard/:rating_id',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Rating Engine'
            },
            resolve: {
                Rating: function($q, $stateParams, RatingsEngineStore) {
                    var deferred = $q.defer();

                    RatingsEngineStore.getRatingDashboard($stateParams.rating_id).then(function(data) {
                        deferred.resolve(data);
                    });

                    return deferred.promise;
                }   
            },
            views: {
                "summary@": {
                    template: ''
                },
                "navigation@": {
                    controller: function($scope, $stateParams, $state, $rootScope, Rating) {
                        $scope.stateName = function() {
                            return $state.current.name;
                        }
                        $rootScope.$broadcast('header-back', { 
                            path: '^home.rating.dashboard',
                            displayName: Rating.summary.displayName,
                            sref: 'home.ratingsengine'
                        });
                    },
                    templateUrl: 'app/ratingsengine/content/dashboard/sidebar/sidebar.component.html'
                },
                'main@': {
                    controller: 'RatingsEngineDashboard',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/dashboard/dashboard.component.html'
                }
            }
        })
        .state('home.ratingsengine.wizard', {
            url: '/wizard/:rating_id',
            resolve: {
                WizardValidationStore: function(RatingsEngineStore) {
                    return RatingsEngineStore;
                },
                WizardProgressContext: function() {
                    return 'ratingsengine';
                },
                WizardProgressItems: function(RatingsEngineStore) {
                    return [
                        { label: 'Segment', state: 'segment', nextFn: RatingsEngineStore.nextSaveRatingEngine },
                        { label: 'Attributes', state: 'segment.attributes' },
                        { label: 'Rules', state: 'segment.attributes.rules', nextFn: RatingsEngineStore.nextSaveRules },
                        { label: 'Summary', state: 'segment.attributes.rules.summary', nextFn: RatingsEngineStore.nextSaveSummary }
                    ];
                }
            },
            views: {
                'summary@': {
                    controller: function($scope, RatingsEngineStore) {
                        $scope.$on('$destroy', function () {
                            RatingsEngineStore.clear();
                        });
                    }
                },
                'main@': {
                    resolve: {
                        WizardHeaderTitle: function() {
                            return 'Create Rating Engine';
                        },
                        WizardContainerId: function() {
                            return 'ratingsengine';
                        }
                    },
                    controller: 'ImportWizard',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/wizard.component.html'
                },
                'wizard_progress@home.ratingsengine.wizard': {
                    controller: 'ImportWizardProgress',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/progress/progress.component.html'
                },
                'wizard_controls@home.ratingsengine.wizard': {
                    resolve: {
                        WizardControlsOptions: function() {
                            return { backState: 'home.ratingsengine', nextState: 'home.ratingsengine' };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            },
            redirectTo: 'home.ratingsengine.wizard.segment'
        })
        .state('home.ratingsengine.wizard.segment', {
            url: '/segment',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Create Rating Engine',
                section: 'wizard.ratingsengine_segment'
            },
            resolve: {
                Segments: function(SegmentService) {
                    return SegmentService.GetSegments();
                },
                CurrentRatingEngine: function($q, $stateParams, RatingsEngineStore) {
                    var deferred = $q.defer();

                    RatingsEngineStore.getRating($stateParams.rating_id).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'wizard_content@home.ratingsengine.wizard': {
                    controller: 'RatingsEngineSegment',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/segment/segment.component.html'
                }
            },
        })
        .state('home.ratingsengine.wizard.segment.attributes', {
            url: '/attributes',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Create Ratings Engine',
                section: 'wizard.ratingsengine_attributes',
                gotoNonemptyCategory: true
            },
            //resolve: angular.extend({}, DataCloudResolvesProvider.$get().main, {
            /**
             * for now we're ducplciating these here from datacloud.routes because when minified the resolves fail
             */
            resolve: {
                EnrichmentCount: ['$q', 'DataCloudStore', 'ApiHost', function($q, DataCloudStore, ApiHost) {
                    var deferred = $q.defer();

                    DataCloudStore.setHost(ApiHost);

                    DataCloudStore.getCount().then(function(result) {
                        DataCloudStore.setMetadata('enrichmentsTotal', result.data);
                        deferred.resolve(result.data);
                    });

                    return deferred.promise;
                }],
                Enrichments: ['$q', 'DataCloudStore', 'ApiHost', 'EnrichmentCount', function($q, DataCloudStore, ApiHost, EnrichmentCount) {
                    var deferred = $q.defer();

                    DataCloudStore.setHost(ApiHost);

                    DataCloudStore.getAllEnrichmentsConcurrently(EnrichmentCount).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }],
                EnrichmentTopAttributes: ['$q', 'DataCloudStore', 'ApiHost', function($q, DataCloudStore, ApiHost) {
                    var deferred = $q.defer();

                    DataCloudStore.setHost(ApiHost);

                    DataCloudStore.getAllTopAttributes().then(function(result) {
                        deferred.resolve(result['Categories'] || result || {});
                    });

                    return deferred.promise;
                }], 
                EnrichmentPremiumSelectMaximum: ['$q', 'DataCloudStore', 'ApiHost', function($q, DataCloudStore, ApiHost) {
                    var deferred = $q.defer();

                    DataCloudStore.setHost(ApiHost);

                    DataCloudStore.getPremiumSelectMaximum().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }],
                // below resolves are needed. Do not removed
                // override at child state when needed
                LookupResponse: [function() {
                    return { attributes: null };
                }],
                QueryRestriction: [function() {
                    return null;
                }],
                CurrentConfiguration: [function() {
                    return null;
                }],
                // end duplicates
                RatingsEngineModels: function($q, $stateParams, DataCloudStore, RatingsEngineStore) {
                    var deferred = $q.defer();
                    DataCloudStore.getRatingsEngineAttributes($stateParams.rating_id).then(function(data) {
                        var model = (data && data[0] ? data[0] : {});

                        if (!model.rule.ratingRule.bucketToRuleMap) {
                            model.rule.ratingRule.bucketToRuleMap = RatingsEngineStore.generateRatingsBuckets();
                        }

                        RatingsEngineStore.checkRatingsBuckets(model.rule.ratingRule.bucketToRuleMap);

                        deferred.resolve(model);
                    });
                    return deferred.promise;
                }
            },
            views: {
                'wizard_content@home.ratingsengine.wizard': {
                    controller: 'DataCloudController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/explorer.component.html'
                }
                // 'wizard_content@home.ratingsengine.wizard': {
                //     controller: 'RatingsEngineAttributes',
                //     controllerAs: 'vm',
                //     templateUrl: 'app/ratingsengine/content/attributes/attributes.component.html'
                // }
            },
        })
        .state('home.ratingsengine.wizard.segment.attributes.rules', {
            url: '/rules',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Create Rating Engine'
            },
            resolve: {
                Cube: function($q, DataCloudStore){
                    var deferred = $q.defer();

                    DataCloudStore.getCube().then(function(result) {
                        deferred.resolve(result.data.Stats);
                    });
                    
                    return deferred.promise;
                }
            },
            views: {
                'wizard_content@home.ratingsengine.wizard': {
                    controller: 'AdvancedQueryCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/advanced/advanced.component.html'
                }
            }
        })
        .state('home.ratingsengine.wizard.segment.attributes.rules.summary', {
            url: '/summary',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Create Rating Engine',
            },
            resolve: {
                Rating: function($q, $stateParams, RatingsEngineStore){
                    var deferred = $q.defer();

                    RatingsEngineStore.getRating($stateParams.rating_id).then(function(result){
                        deferred.resolve(result)
                    });

                    return deferred.promise;
                }
            },
            views: {
                'wizard_content@home.ratingsengine.wizard': {
                    controller: 'RatingsEngineSummary',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/summary/summary.component.html'
                }
            },
        });
});