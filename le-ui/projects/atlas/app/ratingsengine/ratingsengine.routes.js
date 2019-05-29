angular
    .module('lp.ratingsengine', [
        'common.wizard',
        'common.datacloud',
        'lp.segments',
        'lp.ratingsengine.ratingsenginetabs',
        'lp.ratingsengine.ratingslist',
        'lp.ratingsengine.creationhistory',
        'lp.ratingsengine.ratingsenginetype',
        'lp.ratingsengine.dashboard',
        'lp.ratingsengine.activatescoring',
        'le.ratingsengine.viewall',
        'lp.notes',
        'lp.ratingsengine.wizard.segment',
        'lp.ratingsengine.wizard.attributes',
        'lp.ratingsengine.wizard.products',
        'lp.ratingsengine.wizard.prioritization',
        'lp.ratingsengine.wizard.training',
        'lp.ratingsengine.wizard.summary',
        'lp.ratingsengine.wizard.creation'
    ])
    .config(function ($stateProvider, DataCloudResolvesProvider) {
        $stateProvider
            .state('home.ratingsengine', {
                url: '/ratings_engine',
                onExit: function(QueryStore) {
                    QueryStore.clear();
                },
                redirectTo: 'home.ratingsengine.list'
            })
            .state('home.ratingsengine.list', {
                url: '/list',
                onEnter: function ($state, FilterService, StateHistory) {
                    var referringRoute = StateHistory.lastFrom().name;
                    if (referringRoute != 'home.ratingsengine.dashboard' && referringRoute != 'home.model.ratings') {
                        FilterService.clear();
                    }
                },
                resolve: {
                    RatingList: function ($q, RatingsEngineStore) {
                        var deferred = $q.defer();

                        RatingsEngineStore.getRatings().then(function (result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }
                },
                views: {
                    "summary@": {
                        controller: 'RatingsEngineTabsController',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/ratingslist/tabs/ratingsenginetabs.component.html'
                    }
                },
                redirectTo: 'home.ratingsengine.list.ratings'
            })
            .state('home.ratingsengine.list.ratings', {
                url: '/ratings',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Models'
                },
                views: {
                    "main@": {
                        controller: 'RatingsEngineListController',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/ratingslist/ratingslist.component.html'
                    }
                }
            })
            .state('home.ratingsengine.list.history', {
                url: '/creationhistory',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Models'
                },
                views: {
                    "main@": {
                        controller: 'RatingsEngineCreationHistory',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/creationhistory/creationhistory.component.html'
                    }
                }
            })
            .state('home.ratingsengine.ratingsenginetype', {
                url: '/ratingsenginetype',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Models',
                    rating_id: ''
                },
                resolve: {
                    DataCollectionStatus: function ($q, QueryStore) {
                        var deferred = $q.defer();
                        QueryStore.getCollectionStatus().then(function(result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }
                },
                views: {
                    "summary@": {
                        templateUrl: 'app/navigation/summary/BlankLine.html'
                    },
                    "main@": {
                        controller: 'RatingsEngineType',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/ratingsenginetype/ratingsenginetype.component.html'
                    }
                }
            })
            .state('home.ratingsengine.dashboard', {
                url: '/:modelId/:rating_id/dashboard',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Model',
                    modelId: '',
                    modelingJobStatus: null,
                    remodelSuccessBanner: false
                },
                onEnter: ['RatingEngine', 'BackStore', function(RatingEngine, BackStore) {
                    BackStore.setBackLabel(RatingEngine.displayName);
                    BackStore.setBackState('home.ratingsengine');
                    BackStore.setHidden(false);
                }],
                resolve: {
                    Dashboard: function ($q, $stateParams, RatingsEngineStore, ModelStore) {
                        var deferred = $q.defer();
                        var rating_id = $stateParams.rating_id || RatingsEngineStore.getRatingId();
                        RatingsEngineStore.getRatingDashboard(rating_id).then(function (data) {
                            ModelStore.setDashboardData(data);
                            deferred.resolve(data);
                        });

                        return deferred.promise;
                    },
                    JobStatus : function($q, $stateParams, Dashboard, RatingsEngineStore){
                        var jobStatus = $stateParams.modelingJobStatus;
                        var deferred = $q.defer();
                        if (jobStatus == null){
                            var iterationId = Dashboard.summary.publishedIterationId ? Dashboard.summary.publishedIterationId : Dashboard.summary.latestIterationId;
                            var modelingStatus = RatingsEngineStore.getIterationFromDashboard(Dashboard, iterationId).modelingJobStatus || '';
                            deferred.resolve({modelingJobStatus: modelingStatus});
                        }else{
                            deferred.resolve({modelingJobStatus: jobStatus});
                        }
                        return deferred.promise;
                    },
                    RatingEngine: function($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer(),
                            id = $stateParams.rating_id;

                        RatingsEngineStore.getRating(id).then(function(engine) {
                            deferred.resolve(engine);
                        });

                        return deferred.promise;
                    },
                    Model: function($q, $stateParams, ModelStore, RatingEngine, RatingsEngineStore, Dashboard) {
                        var iterationId = Dashboard.summary.publishedIterationId ? Dashboard.summary.publishedIterationId : Dashboard.summary.latestIterationId;
                        var deferred = $q.defer(),
                            id = '';

                        if ($stateParams.modelId) {
                            id = $stateParams.modelId;
                        } else if (RatingsEngineStore.getIterationFromDashboard(Dashboard, iterationId)) {
                            id = RatingsEngineStore.getIterationFromDashboard(Dashboard, iterationId).modelSummaryId;
                        } else {
                            id = '';
                        }

                        if ((RatingEngine.type === 'RULE_BASED') || (id === '')) {
                            deferred.resolve(null);
                        } else if (ModelStore.data != undefined)  {
                            deferred.resolve(ModelStore.data);
                        } else {
                            ModelStore.getModel(id).then(function(result) {
                                deferred.resolve(result);
                            });
                        }

                        return deferred.promise;
                    },
                    IsRatingEngine: function(Model) {
                        return true;
                    },
                    IsPmml: function(Model) {
                        return false;
                    },
                    Products: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        var params = {
                            max: 1000,
                            offset: 0
                        };
                        RatingsEngineStore.getProducts(params).then(function (result) {
                            deferred.resolve(result);
                        });
                        return deferred.promise;
                    },
                    TargetProducts: function(RatingEngine, Products) {
                        var ratingEngine = RatingEngine,
                            type = ratingEngine.type.toLowerCase();

                        if (type != 'rule_based') {
                            var isPublishedOrScored = (ratingEngine.published_iteration || ratingEngine.scoring_iteration) ? true : false,
                                products = Products,
                                model = {},
                                targetProducts = [];

                            if (isPublishedOrScored){
                                var model = ratingEngine.published_iteration ? ratingEngine.published_iteration.AI : ratingEngine.scoring_iteration.AI;
                            } else {
                                var model = ratingEngine.latest_iteration.AI;
                            }

                            var targetProducts = model.advancedModelingConfig[type].targetProducts;

                            if (targetProducts && targetProducts.length != 0) {
                                if(targetProducts.length == 1){
                                    return products.find(function(obj) { return obj.ProductId === targetProducts[0].toString() });
                                } else {
                                    var targetProductNames = [];
                                    angular.forEach(targetProducts, function(product){
                                        targetProductNames.push(products.find(function(obj) { return obj.ProductId === product.toString() }));
                                    });
                                    return targetProductNames;
                                }
                            }
                        } else {
                            return null;
                        }
                    },
                    TrainingProducts: function($q, RatingEngine, Products) {
                        var deferred = $q.defer(),
                            ratingEngine = RatingEngine,
                            type = ratingEngine.type.toLowerCase();

                        if (type != 'rule_based') {

                            var isPublishedOrScored = (ratingEngine.published_iteration || ratingEngine.scoring_iteration) ? true : false,
                                products = Products,
                                model = {},
                                trainingProducts = [];

                            if (isPublishedOrScored){
                                var model = ratingEngine.published_iteration ? ratingEngine.published_iteration.AI : ratingEngine.scoring_iteration.AI;
                            } else {
                                var model = ratingEngine.latest_iteration.AI;
                            }

                            var trainingProducts = model.advancedModelingConfig[type].trainingProducts;

                            if (trainingProducts && trainingProducts.length != 0) {
                                if(trainingProducts.length == 1){
                                    var trainingProductNames = products.find(function(obj) { return obj.ProductId === trainingProducts[0].toString() });
                                    deferred.resolve(trainingProductNames);
                                } else {
                                    var trainingProductNames = [];
                                    angular.forEach(trainingProducts, function(product){
                                        trainingProductNames.push(products.find(function(obj) { return obj.ProductId === product.toString() }));
                                    });
                                    deferred.resolve(trainingProductNames);
                                }
                            } else {
                                deferred.resolve(null);    
                            }
                        } else {
                            deferred.resolve(null);
                        }

                        return deferred.promise;
                    },
                    DataCollectionStatus: function ($q, QueryStore) {
                        var deferred = $q.defer();
                        QueryStore.getCollectionStatus().then(function(result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }
                },
                views: {
                    "summary@": {
                        controller: 'ModelDetailController',
                        template: '<div id="ModelDetailsArea"></div>'
                    },
                    "navigation@home": {
                        controller: function ($scope, $stateParams, $state, $rootScope, RatingsEngineStore, Dashboard, RatingEngine, Model, JobStatus) {

                            $scope.rating_id = $stateParams.rating_id || '';

                            if (Dashboard.summary.publishedIterationId) {
                                var iterationId = Dashboard.summary.publishedIterationId;
                            } else if (Dashboard.summary.scoringIterationId) {
                                var iterationId = Dashboard.summary.scoringIterationId;
                            } else {
                                var iterationId = Dashboard.summary.latestIterationId;
                            }
                            // $scope.modelId = $stateParams.modelId || RatingsEngineStore.getIterationFromDashboard(Dashboard, iterationId).modelSummaryId || '';
                            if ($stateParams.modelId) {
                                $scope.modelId = $stateParams.modelId;
                            } else if (RatingsEngineStore.getIterationFromDashboard(Dashboard, iterationId)) {
                                $scope.modelId = RatingsEngineStore.getIterationFromDashboard(Dashboard, iterationId).modelSummaryId;
                            } else {
                                $scope.modelId = '';
                            }

                            $scope.isRuleBased = (RatingEngine.type === 'RULE_BASED') ? true : false;
                            $scope.isCustomEvent = (RatingEngine.type === 'CUSTOM_EVENT') ? true : false;
                            if($scope.isRuleBased || $scope.isCustomEvent) {
                                if($scope.isRuleBased) {
                                    $scope.typeContext = 'rule';
                                } else {
                                    $scope.typeContext = 'AI';
                                }
                                $scope.modelingStrategy = RatingEngine.type;
                            } else {
                                var type = RatingEngine.type.toLowerCase();
                                $scope.typeContext = 'AI';
                                $scope.modelingStrategy = RatingEngine.latest_iteration.AI.advancedModelingConfig[type].modelingStrategy;
                            }

                            $scope.activeIteration = RatingEngine.latest_iteration[$scope.typeContext].iteration;

                            // Check if the sidebar nav should be enabled/disabled in /ratingsengine/ pages
                            if (RatingEngine.published_iteration != null && RatingEngine.published_iteration != undefined) {
                                $scope.modelIsReady = true;

                            } else if (RatingEngine.scoring_iteration != null && RatingEngine.scoring_iteration != undefined) {
                                $scope.modelIsReady = RatingEngine.scoring_iteration[$scope.typeContext].modelSummaryId != null && RatingEngine.scoring_iteration[$scope.typeContext].modelSummaryId != undefined ? true : false;

                            } else if (RatingEngine.latest_iteration != null && RatingEngine.latest_iteration != undefined) {
                                // If only one iteration has been created, check it's status.
                                if (Dashboard.iterations.length == 1) {
                                    $scope.modelIsReady = Dashboard.iterations.length == 1 && (
                                        RatingEngine.latest_iteration[$scope.typeContext].modelSummaryId != null && 
                                        RatingEngine.latest_iteration[$scope.typeContext].modelSummaryId != undefined && 
                                        (
                                            RatingEngine.latest_iteration[$scope.typeContext].modelingJobStatus != 'Failed' && 
                                            RatingEngine.latest_iteration[$scope.typeContext].modelingJobStatus != 'Pending' && 
                                            RatingEngine.latest_iteration[$scope.typeContext].modelingJobStatus != 'Running'
                                        )
                                    ) ? true : false;
                                } else if (Dashboard.iterations.length > 1) {
                                    // If multiple iterations have been created, at least one is completed, but have not been scored or published
                                    var iterations = Dashboard.iterations,
                                        hasCompletedIteration = iterations.filter(iteration => (iteration.modelingJobStatus === "Completed"));
                                    $scope.modelIsReady = hasCompletedIteration ? true : false;
                                }
                            }

                            $scope.stateName = function () {
                                return $state.current.name;
                            };
                            
                            $scope.isJobCompleted = function(){
                                switch(JobStatus.modelingJobStatus){
                                case 'Completed':{
                                    return true;
                                }
                                default: return false;
                                }
                            };

                        },
                        templateUrl: 'app/ratingsengine/content/dashboard/sidebar/sidebar.component.html'
                    },
                    'main@': {
                        controller: 'RatingsEngineDashboard',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/dashboard/dashboard.component.html'
                    },
                    'header.back@': 'backNav'
                }
            })
            .state('home.ratingsengine.dashboard.viewall', {
                url: '/viewall/:type',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Models',
                    section: 'wizard.ratingsengine_segment'
                },
                views: {
                    'main@': 'viewAllComponent'
                }
            })
            .state('home.ratingsengine.dashboard.training', {
                url: '/remodel-settings/:aiModel',
                params: {
                    pageIcon: 'ico-attributes',
                    pageTitle: 'View Iteration',
                    viewingIteration: true,
                    aiModel: ''
                },
                onEnter: ['$stateParams', 'BackStore', function($stateParams, BackStore) {
                    var backState = 'home.ratingsengine.dashboard',
                        backParams = {
                            rating_id: $stateParams.rating_id,
                            modelId: $stateParams.modelId,
                            viewingIteration: false
                        },
                        displayName = 'View Model';

                    BackStore.setBackLabel(displayName);
                    BackStore.setBackState(backState);
                    BackStore.setBackParams(backParams);

                    BackStore.setHidden(false);
                }],
                resolve: {
                    ratingEngine: ['RatingsEngineStore', function (RatingsEngineStore) {
                        return RatingsEngineStore.getRatingEngine();
                    }],
                    segments: ['SegmentService', function (SegmentService) {
                        return SegmentService.GetSegments();
                    }],
                    products: ['$q', 'RatingsEngineStore', function ($q, RatingsEngineStore) {
                        var deferred = $q.defer();

                        var params = {
                            max: 1000,
                            offset: 0
                        };
                        RatingsEngineStore.getProducts(params).then(function (result) {
                            deferred.resolve(result);
                        });
                        return deferred.promise;
                    }],
                    iteration: ['$q', '$stateParams', 'RatingsEngineStore', 'ratingEngine', function($q, $stateParams, RatingsEngineStore, ratingEngine){
                        var deferred = $q.defer(),
                            engineId = ratingEngine.id,
                            iteration = RatingsEngineStore.getRemodelIteration(),
                            modelId = iteration.id;

                        RatingsEngineStore.getRatingModel(engineId, modelId).then(function(result){
                            RatingsEngineStore.setRatingEngine(ratingEngine);
                            deferred.resolve(result);
                        });
                        return deferred.promise;

                    }],
                    attributes: ['$q', '$stateParams', 'DataCloudStore', function($q, $stateParams, DataCloudStore) {
                        var deferred = $q.defer(),
                            ratingId = $stateParams.rating_id,
                            modelId = $stateParams.modelId;

                        DataCloudStore.ratingIterationFilter = 'all';

                        DataCloudStore.getAllEnrichmentsConcurrently("/pls/ratingengines/" + ratingId + "/ratingmodels/" + modelId + "/metadata", true).then((result) => {
                            deferred.resolve(result);
                        });

                        return deferred.promise;

                    }],
                    datacollectionstatus: ['$q', 'QueryStore', function ($q, QueryStore) {
                        var deferred = $q.defer();
                        QueryStore.getCollectionStatus().then(function(result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }],
                    HasRatingsAvailable: function(
                        $q,
                        $stateParams,
                        ModelRatingsService
                    ) {
                        var deferred = $q.defer(),
                            id = $stateParams.modelId;

                        ModelRatingsService.HistoricalABCDBuckets(id).then(function(
                            result
                        ) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }
                },
                views: {
                    'navigation@home': {
                        controller: 'SidebarModelController',
                        controllerAs: 'vm',
                        templateUrl:
                            'app/navigation/sidebar/model/model.component.html'
                    },          
                    'summary@': {
                        templateUrl: 'app/navigation/summary/BlankLine.html'
                    },
                    'main@': 'ratingsEngineAITraining'
                }
            })
            .state('home.ratingsengine.dashboard.segment', {
                url: '/segment',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Models',
                    section: 'wizard.ratingsengine_segment'
                },
                resolve: {
                    Segments: function (SegmentService) {
                        return SegmentService.GetSegments();
                    },
                    CurrentRatingEngine: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        if (!$stateParams.rating_id) {
                            deferred.resolve(RatingsEngineStore.currentRating);
                        } else {
                            RatingsEngineStore.getRating($stateParams.rating_id).then(function (result) {
                                deferred.resolve(result);
                            });
                        }

                        return deferred.promise;
                    }
                },
                views: {
                    'main@': {
                        controller: 'RatingsEngineSegment',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/segment/segment.component.html'
                    }
                }
            })
            .state('home.ratingsengine.dashboard.segment.attributes', {
                url: '/attributes',
                params: {
                    section: 'wizard.ratingsengine_segment',
                    gotoNonemptyCategory: true
                },
                resolve: {
                    // EnrichmentCount: ['$q', 'DataCloudStore', 'ApiHost', function ($q, DataCloudStore, ApiHost) {
                    //     var deferred = $q.defer();

                    //     DataCloudStore.setHost(ApiHost);

                    //     DataCloudStore.getCount().then(function (result) {
                    //         DataCloudStore.setMetadata('enrichmentsTotal', result.data);
                    //         deferred.resolve(result.data);
                    //     });

                    //     return deferred.promise;
                    // }],
                    // Enrichments: ['$q', 'DataCloudStore', 'ApiHost', 'EnrichmentCount', function ($q, DataCloudStore, ApiHost, EnrichmentCount) {
                    Enrichments: ['$q', 'DataCloudStore', 'ApiHost', function ($q, DataCloudStore, ApiHost) {
                        var deferred = $q.defer();

                        DataCloudStore.setHost(ApiHost);

                        DataCloudStore.getAllEnrichmentsConcurrently().then(function (result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }],
                    EnrichmentTopAttributes: ['$q', 'DataCloudStore', 'ApiHost', function ($q, DataCloudStore, ApiHost) {
                        var deferred = $q.defer();

                        DataCloudStore.setHost(ApiHost);

                        DataCloudStore.getAllTopAttributes().then(function (result) {
                            deferred.resolve(result['Categories'] || result || {});
                        });

                        return deferred.promise;
                    }],
                    EnrichmentPremiumSelectMaximum: ['$q', 'DataCloudStore', 'ApiHost', function ($q, DataCloudStore, ApiHost) {
                        var deferred = $q.defer();

                        DataCloudStore.setHost(ApiHost);

                        DataCloudStore.getPremiumSelectMaximum().then(function (result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }],
                    EnrichmentSelectMaximum: ['$q', 'DataCloudStore', function($q, DataCloudStore) {
                        var deferred = $q.defer();

                        DataCloudStore.getSelectMaximum().then(function(result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }],
                    // below resolves are needed. Do not removed
                    // override at child state when needed
                    LookupResponse: [function () {
                        return { attributes: null };
                    }],
                    QueryRestriction: [function () {
                        return null;
                    }],
                    WorkingBuckets: [function () {
                        return null;
                    }],
                    // end duplicates
                    RatingsEngineModels: function ($q, $stateParams, DataCloudStore, RatingsEngineStore) {
                        var deferred = $q.defer();

                        DataCloudStore.getRatingsEngineAttributes($stateParams.rating_id).then(function (data) {
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
                    'main@': {
                        controller: 'DataCloudController',
                        controllerAs: 'vm',
                        templateUrl: '/components/datacloud/explorer/explorer.component.html'
                    }
                }
            })
            .state('home.ratingsengine.dashboard.segment.attributes.add', {
                url: '/add',
                params: {
                    pageIcon: 'ico-performance',
                    pageTitle: 'Rules',
                    section: 'wizard.ratingsengine_segment',
                    gotoNonemptyCategory: true
                },
                views: {
                    'main@': {
                        controller: 'DataCloudController',
                        controllerAs: 'vm',
                        templateUrl: '/components/datacloud/explorer/explorer.component.html'
                    }
                }
            })
            .state('home.ratingsengine.dashboard.segment.attributes.rules', {
                url: '/rules',
                params: {
                    pageIcon: 'ico-performance',
                    pageTitle: 'Rules',
                    section: 'wizard.ratingsengine_segment',
                    gotoNonemptyCategory: true
                },
                resolve: {
                    JobStatus : function($q, $stateParams){
                        var jobStatus = $stateParams.modelingJobStatus;
                        var deferred = $q.defer();
                        if(jobStatus == null){
                            deferred.resolve({modelingJobStatus: 'Completed'});
                        }else{
                            deferred.resolve({modelingJobStatus: jobStatus});
                        }
                        return deferred.promise;
                    },
                    CurrentRatingEngine: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        if (!$stateParams.rating_id) {
                            deferred.resolve(RatingsEngineStore.currentRating);
                        } else {
                            RatingsEngineStore.getRating($stateParams.rating_id).then(function (result) {
                                deferred.resolve(result);
                            });
                        }

                        return deferred.promise;
                    },
                    Cube: function ($q, DataCloudStore) {
                        var deferred = $q.defer();

                        DataCloudStore.getCube().then(function (result) {
                            deferred.resolve(result.data);
                        });

                        return deferred.promise;
                    },
                    RatingEngineModel: function (DataCloudStore, RatingsEngineModels) {
                        var selectedAttributes = DataCloudStore.getCurrentRatingsEngineAttributes();

                        if (selectedAttributes) {
                            RatingsEngineModels.rule.selectedAttributes = selectedAttributes;
                        }

                        return RatingsEngineModels;
                    }
                },
                onEnter: ['$stateParams', 'RatingsEngineService', 'QueryStore', function($stateParams, RatingsEngineService, QueryStore) {
                    QueryStore.clear();
                    var id = $stateParams.rating_id;
                    RatingsEngineService.GetRatingEnginesDependenciesModelView(id);
                }],
                views: {
                    "navigation@home": {
                        controller: function ($scope, $stateParams, $state, $rootScope, Dashboard, RatingEngine, JobStatus) {
                            $scope.rating_id = $stateParams.rating_id || '';
                            $scope.modelId = $stateParams.modelId || '';
                            $scope.isRuleBased = (RatingEngine.type === 'RULE_BASED');
                            $scope.stateName = function () {
                                return $state.current.name;
                            };
                            $scope.isJobCompleted = function(){
                                switch(JobStatus.modelingJobStatus){
                                case 'Completed':{
                                    return true;
                                }
                                default: return false;
                                }
                            };
                        },
                        templateUrl: 'app/ratingsengine/content/dashboard/sidebar/sidebar.component.html'
                    },
                    'main@': {
                        controller: 'AdvancedQueryCtrl',
                        controllerAs: 'vm',
                        templateUrl: '/components/datacloud/query/advanced/advanced.component.html'
                    }
                }
            })
            .state('home.ratingsengine.dashboard.segment.attributes.rules.picker', {
                url: '/picker/:entity/:fieldname',
                params: {
                    mode: 'dashboardrules'
                },
                resolve: {
                    Segment: ['$q', '$stateParams', 'RatingsEngineStore',function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        if (!$stateParams.rating_id) {
                            deferred.resolve(RatingsEngineStore.currentRating.segment);
                        } else {
                            RatingsEngineStore.getRating($stateParams.rating_id).then(function (result) {
                                deferred.resolve(result.segment);
                            });
                        }

                        return deferred.promise;
                    }],
                    PickerBuckets: ['$q', '$stateParams', 'QueryTreeService', 'DataCloudStore', function($q, $stateParams, QueryTreeService, DataCloudStore){
                        var deferred = $q.defer();
                        var entity = $stateParams.entity;
                        var fieldname = $stateParams.fieldname;

                        QueryTreeService.getPickerCubeData(entity, fieldname).then(function(result) {
                            deferred.resolve(result.data);
                        });
                        
                        return deferred.promise;
                    }]
                },
                views: {
                    'main@': {
                        controller: 'ValuePickerController',
                        controllerAs: 'vm',
                        templateUrl: '/components/datacloud/picker/picker.component.html'
                    }
                }
            })
            .state('home.ratingsengine.dashboard.notes', {
                url: '/notes',
                resolve: {
                    JobStatus : function($q, $stateParams, Dashboard, RatingsEngineStore) {
                        var jobStatus = $stateParams.modelingJobStatus;
                        var deferred = $q.defer();
                        if(jobStatus == null){
                            var iterationId = Dashboard.summary.publishedIterationId ? Dashboard.summary.publishedIterationId : Dashboard.summary.latestIterationId;
                            var modelingStatus = RatingsEngineStore.getIterationFromDashboard(Dashboard, iterationId).modelingJobStatus || '';
                            deferred.resolve({modelingJobStatus: modelingStatus});
                        }else{
                            deferred.resolve({modelingJobStatus: jobStatus});
                        }
                        return deferred.promise;
                    },
                    Rating: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        RatingsEngineStore.getRating($stateParams.rating_id).then(function (result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    },
                    Notes: function($q, $stateParams, NotesService) {
                        var deferred = $q.defer(),
                            id = $stateParams.rating_id;

                        NotesService.GetNotes(id).then(function(result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    },
                    Model: [function() {
                        return null;
                    }]
                },
                params: {
                    pageIcon: 'ico-notes',
                    pageTitle: 'Notes',
                    section: 'dashboard.notes',
                    rating_id: '',
                    modelId: '',
                    modelingJobStatus: null
                },
                views: {
                    "navigation@home": {
                        controller: function ($scope, $stateParams, $state, $rootScope, Dashboard, RatingsEngineStore, RatingEngine, JobStatus) {
                            $scope.rating_id = $stateParams.rating_id || '';
                            
                            if (Dashboard.summary.publishedIterationId) {
                                var iterationId = Dashboard.summary.publishedIterationId;
                            } else if (Dashboard.summary.scoringIterationId) {
                                var iterationId = Dashboard.summary.scoringIterationId;
                            } else {
                                var iterationId = Dashboard.summary.latestIterationId;
                            }
                            $scope.modelId = $stateParams.modelId || RatingsEngineStore.getIterationFromDashboard(Dashboard, iterationId).modelSummaryId || '';

                            $scope.isRuleBased = (RatingEngine.type === 'RULE_BASED') ? true : false;
                            $scope.isCustomEvent = (RatingEngine.type === 'CUSTOM_EVENT') ? true : false;
                            if($scope.isRuleBased || $scope.isCustomEvent) {
                                if($scope.isRuleBased) {
                                    $scope.typeContext = 'rule';
                                } else {
                                    $scope.typeContext = 'AI';
                                }
                                $scope.modelingStrategy = RatingEngine.type;
                            } else {
                                var type = RatingEngine.type.toLowerCase();
                                $scope.typeContext = 'AI';

                                $scope.modelingStrategy = RatingEngine.latest_iteration.AI.advancedModelingConfig[type].modelingStrategy;
                            }
                            $scope.activeIteration = RatingEngine.latest_iteration[$scope.typeContext].iteration;
                            
                            // Check if the sidebar nav should be enabled/disabled in /ratingsengine/ pages
                            if (RatingEngine.published_iteration != null && RatingEngine.published_iteration != undefined) {
                                $scope.modelIsReady = true;
                            } else if (RatingEngine.scoring_iteration != null && RatingEngine.scoring_iteration != undefined) {
                                $scope.modelIsReady = RatingEngine.scoring_iteration[$scope.typeContext].modelSummaryId != null && RatingEngine.scoring_iteration[$scope.typeContext].modelSummaryId != undefined ? true : false;
                            } else if (RatingEngine.latest_iteration != null && RatingEngine.latest_iteration != undefined) {
                                // If only one iteration has been created, check it's status.
                                if (Dashboard.iterations.length == 1) {
                                    $scope.modelIsReady = Dashboard.iterations.length == 1 && (
                                        RatingEngine.latest_iteration[$scope.typeContext].modelSummaryId != null && 
                                        RatingEngine.latest_iteration[$scope.typeContext].modelSummaryId != undefined && 
                                        (
                                            RatingEngine.latest_iteration[$scope.typeContext].modelingJobStatus != 'Failed' && 
                                            RatingEngine.latest_iteration[$scope.typeContext].modelingJobStatus != 'Pending' && 
                                            RatingEngine.latest_iteration[$scope.typeContext].modelingJobStatus != 'Running'
                                        )
                                    ) ? true : false;
                                } else if (Dashboard.iterations.length > 1) {
                                    // If multiple iterations have been created, at least one is completed, but have not been scored or published
                                    var iterations = Dashboard.iterations,
                                        hasCompletedIteration = iterations.filter(iteration => (iteration.modelingJobStatus === "Completed"));
                                    $scope.modelIsReady = hasCompletedIteration ? true : false;
                                }
                            }

                            $scope.stateName = function () {
                                return $state.current.name;
                            };

                            $scope.isJobCompleted = function(){
                                switch(JobStatus.modelingJobStatus){
                                case 'Completed':{
                                    return true;
                                }
                                default: return false;
                                }
                            };
                        },
                        templateUrl: 'app/ratingsengine/content/dashboard/sidebar/sidebar.component.html'
                    },
                    "main@": {
                        controller: 'NotesController',
                        controllerAs: 'vm',
                        templateUrl: 'app/notes/NotesView.html'
                    }
                }
            })
            .state('home.ratingsengine.rulesprospects', {
                url: '/rules/:rating_id/:wizard_steps',
                params: {
                    rating_id: '',
                    wizard_steps: 'rulesprospects'
                },
                onExit: function ($state, RatingsEngineStore) {
                    if (!$state.params.wizard_steps) {
                        RatingsEngineStore.clear();
                    }
                },
                resolve: {
                    WizardValidationStore: function (RatingsEngineStore) {
                        return RatingsEngineStore;
                    },
                    WizardProgressContext: function () {
                        return 'ratingsengine.rulesprospects';
                    },
                    WizardProgressItems: function ($stateParams, RatingsEngineStore) {
                        var rating_id = $stateParams.rating_id || '',
                            wizard_steps = $stateParams.wizard_steps || 'rulesprospects';

                        return RatingsEngineStore.getWizardProgressItems(wizard_steps, rating_id);
                    },
                    WizardContainerId: function () {
                        return 'ratingsengine';
                    },
                    WizardHeaderTitle: function () {
                        return 'Create Model';
                    },
                    WizardCustomHeaderSteps: function() {
                        return ['segment.attributes', 'segment.attributes.add', 'segment.attributes.rules'];
                    },
                    WizardControlsOptions: function (RatingsEngineStore) {
                        return {
                            backState: 'home.ratingsengine.ratingsenginetype',
                            nextState: 'home.ratingsengine.dashboard'
                        };
                    }
                },
                onEnter: function(QueryStore){
                    QueryStore.clear();
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
                    'wizard_header@home.ratingsengine.rulesprospects': {
                        controller:'WizardHeader',
                        controllerAs:'vm',
                        templateUrl: '/components/wizard/header/header.component.html'
                    },
                    'wizard_progress@home.ratingsengine.rulesprospects': {
                        controller: 'ImportWizardProgress',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/progress/progress.component.html'

                    },
                    'wizard_controls@home.ratingsengine.rulesprospects': {
                        controller: 'ImportWizardControls',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/controls/controls.component.html'
                    }
                },
                redirectTo: 'home.ratingsengine.rulesprospects.segment'
            })
            .state('home.ratingsengine.rulesprospects.segment', {
                url: '/segment',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Models - Select Segment'
                },
                resolve: {
                    Segments: function (SegmentService) {
                        return SegmentService.GetSegments();
                    },
                    CurrentRatingEngine: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        if (!$stateParams.rating_id) {
                            deferred.resolve(RatingsEngineStore.currentRating);
                        } else {
                            RatingsEngineStore.getRating($stateParams.rating_id).then(function (result) {
                                deferred.resolve(result);
                            });
                        }

                        return deferred.promise;
                    }
                },
                views: {
                    'wizard_content@home.ratingsengine.rulesprospects': {
                        controller: 'RatingsEngineSegment',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/segment/segment.component.html'
                    }
                }
            })
            .state('home.ratingsengine.rulesprospects.segment.attributes', {
                url: '/attributes',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Models - Add Attributes',
                    section: 'wizard.ratingsengine_segment',
                    gotoNonemptyCategory: true
                },
                //resolve: angular.extend({}, DataCloudResolvesProvider.$get().main, {
                /**
                 * for now we're ducplciating these here from datacloud.routes because when minified the resolves fail
                 */
                resolve: {
                    // EnrichmentCount: ['$q', 'DataCloudStore', 'ApiHost', function ($q, DataCloudStore, ApiHost) {
                    //     var deferred = $q.defer();

                    //     DataCloudStore.setHost(ApiHost);

                    //     DataCloudStore.getCount().then(function (result) {
                    //         DataCloudStore.setMetadata('enrichmentsTotal', result.data);
                    //         deferred.resolve(result.data);
                    //     });

                    //     return deferred.promise;
                    // }],
                    // Enrichments: ['$q', 'DataCloudStore', 'ApiHost', 'EnrichmentCount', function ($q, DataCloudStore, ApiHost, EnrichmentCount) {
                    Enrichments: ['$q', 'DataCloudStore', 'ApiHost', function ($q, DataCloudStore, ApiHost) {
                        var deferred = $q.defer();

                        DataCloudStore.setHost(ApiHost);

                        DataCloudStore.getAllEnrichmentsConcurrently().then(function (result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }],
                    EnrichmentTopAttributes: ['$q', 'DataCloudStore', 'ApiHost', function ($q, DataCloudStore, ApiHost) {
                        var deferred = $q.defer();

                        DataCloudStore.setHost(ApiHost);

                        DataCloudStore.getAllTopAttributes().then(function (result) {
                            deferred.resolve(result['Categories'] || result || {});
                        });

                        return deferred.promise;
                    }],
                    EnrichmentPremiumSelectMaximum: ['$q', 'DataCloudStore', 'ApiHost', function ($q, DataCloudStore, ApiHost) {
                        var deferred = $q.defer();

                        DataCloudStore.setHost(ApiHost);

                        DataCloudStore.getPremiumSelectMaximum().then(function (result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }],
                    EnrichmentSelectMaximum: ['$q', 'DataCloudStore', function($q, DataCloudStore) {
                        var deferred = $q.defer();

                        DataCloudStore.getSelectMaximum().then(function(result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }],
                    // below resolves are needed. Do not removed
                    // override at child state when needed
                    LookupResponse: [function () {
                        return { attributes: null };
                    }],
                    QueryRestriction: [function () {
                        return null;
                    }],
                    WorkingBuckets: [function () {
                        return null;
                    }],
                    // end duplicates
                    RatingsEngineModels: function ($q, $stateParams, DataCloudStore, RatingsEngineStore) {
                        var deferred = $q.defer();

                        DataCloudStore.getRatingsEngineAttributes($stateParams.rating_id).then(function (data) {
                            var model = (data && data[0] ? data[0] : {});

                            if (!model.rule.ratingRule.bucketToRuleMap) {
                                model.rule.ratingRule.bucketToRuleMap = RatingsEngineStore.generateRatingsBuckets();
                            }

                            // console.log(model);

                            RatingsEngineStore.checkRatingsBuckets(model.rule.ratingRule.bucketToRuleMap);

                            deferred.resolve(model);
                        });

                        return deferred.promise;
                    }
                },
                views: {
                    'wizard_content@home.ratingsengine.rulesprospects': {
                        controller: 'DataCloudController',
                        controllerAs: 'vm',
                        templateUrl: '/components/datacloud/explorer/explorer.component.html'
                    }
                }
            })
            .state('home.ratingsengine.rulesprospects.segment.attributes.add', {
                url: '/add',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Models - Add Attributes',
                    section: 'wizard.ratingsengine_segment',
                    gotoNonemptyCategory: true
                },
                views: {
                    'wizard_content@home.ratingsengine.rulesprospects': {
                        controller: 'DataCloudController',
                        controllerAs: 'vm',
                        templateUrl: '/components/datacloud/explorer/explorer.component.html'
                    }
                }
            })
            .state('home.ratingsengine.rulesprospects.segment.attributes.rules', {
                url: '/rules',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Models - Create Rules'
                },
                onEnter: ['$stateParams', 'RatingsEngineService', function($stateParams, RatingsEngineService) {
                    var id = $stateParams.rating_id;
                    RatingsEngineService.GetRatingEnginesDependenciesModelView(id);
                }],
                resolve: {
                    Cube: function ($q, DataCloudStore) {
                        var deferred = $q.defer();

                        DataCloudStore.getCube().then(function (result) {
                            deferred.resolve(result.data);
                        });

                        return deferred.promise;
                    },
                    RatingEngineModel: function (DataCloudStore, RatingsEngineModels) {
                        var selectedAttributes = DataCloudStore.getCurrentRatingsEngineAttributes();

                        // console.log(RatingsEngineModels);

                        if (selectedAttributes) {
                            RatingsEngineModels.rule.selectedAttributes = selectedAttributes;
                        }

                        return RatingsEngineModels;
                    }
                },
                views: {
                    'wizard_content@home.ratingsengine.rulesprospects': {
                        controller: 'AdvancedQueryCtrl',
                        controllerAs: 'vm',
                        templateUrl: '/components/datacloud/query/advanced/advanced.component.html'
                    }
                }
            })
            .state('home.ratingsengine.rulesprospects.segment.attributes.rules.picker', {
                url: '/picker/:entity/:fieldname',
                resolve: {
                    PickerBuckets: ['$q', '$stateParams', 'QueryTreeService', 'DataCloudStore', function($q, $stateParams, QueryTreeService, DataCloudStore){
                        var deferred = $q.defer();
                        var entity = $stateParams.entity;
                        var fieldname = $stateParams.fieldname;

                        QueryTreeService.getPickerCubeData(entity, fieldname).then(function(result) {
                            deferred.resolve(result.data);
                        });
                        
                        return deferred.promise;
                    }],
                    Segment: ['$q', '$stateParams', 'RatingsEngineStore',function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        if (!$stateParams.rating_id) {
                            deferred.resolve(RatingsEngineStore.currentRating.segment);
                        } else {
                            RatingsEngineStore.getRating($stateParams.rating_id).then(function (result) {
                                deferred.resolve(result.segment);
                            });
                        }

                        return deferred.promise;
                    }]
                },
                views: {
                    "wizard_content@home.ratingsengine.rulesprospects": {
                        controller: 'ValuePickerController',
                        controllerAs: 'vm',
                        templateUrl: '/components/datacloud/picker/picker.component.html'
                    }
                }
            })
            .state('home.ratingsengine.rulesprospects.segment.attributes.rules.summary', {
                url: '/summary',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Models - Review Summary',
                },
                resolve: {
                    Rating: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        RatingsEngineStore.getRating($stateParams.rating_id).then(function (result) {
                            deferred.resolve(result)
                        });

                        return deferred.promise;
                    }
                },
                views: {
                    'wizard_content@home.ratingsengine.rulesprospects': {
                        controller: 'RatingsEngineSummary',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/summary/summary.component.html'
                    }
                }
            })
            .state('home.ratingsengine.productpurchase', {
                url: '/product/:rating_id/:wizard_steps',
                params: {
                    wizard_steps: 'productpurchase',
                    engineType: '',
                    displayName: '',
                    rating_id: '',
                    fromList: false
                },
                resolve: {
                    WizardValidationStore: function (RatingsEngineStore) {
                        return RatingsEngineStore;
                    },
                    WizardProgressContext: function () {
                        return 'ratingsengine.productpurchase';
                    },
                    WizardProgressItems: function ($stateParams, RatingsEngineStore) {
                        var rating_id = $stateParams.rating_id || '',
                            wizard_steps = 'productpurchase';

                        return RatingsEngineStore.getWizardProgressItems(wizard_steps, rating_id);
                    },
                    CurrentRatingEngine: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer(),
                            ratingId = $stateParams.rating_id;

                        if (ratingId !== '') {
                            RatingsEngineStore.getRating(ratingId).then(function(engine){
                                deferred.resolve(engine);
                            });
                        } else {
                            deferred.resolve();
                        }

                        return deferred.promise;
                    },
                    DataCollectionStatus: function ($q, QueryStore) {
                        var deferred = $q.defer();
                        QueryStore.getCollectionStatus().then(function(result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    },
                    WizardHeaderTitle: function ($stateParams, CurrentRatingEngine, RatingsEngineStore, DataCollectionStatus) {
                        var engineType = $stateParams.engineType,
                            currentRating = CurrentRatingEngine,
                            fromList = $stateParams.fromList,
                            title = '';


                        if (currentRating !== undefined && fromList){
                            title = currentRating.displayName;
                        } else if (engineType === 'CROSS_SELL_FIRST_PURCHASE') {
                            title = 'Create Model: Customers that will purchase a product for the first time';
                        } else if (engineType === 'CROSS_SELL_REPEAT_PURCHASE') {
                            var periodType = DataCollectionStatus.ApsRollingPeriod ? DataCollectionStatus.ApsRollingPeriod.toLowerCase() : 'quarter';
                            title = 'Create Model: Customers that will purchase again next ' + periodType;
                        }

                        return title;
                    },
                    WizardContainerId: function () {
                        return 'ratingsengine';
                    },
                    WizardControlsOptions: function (RatingsEngineStore) {
                        return {
                            backState: 'home.ratingsengine.ratingsenginetype',
                            secondaryLink: true,
                            nextState: 'home.ratingsengine.dashboard'
                        };
                    }
                },
                views: {
                    'summary@': {
                        controller: function ($scope, RatingsEngineStore) {
                            $scope.$on('$destroy', function () {
                                RatingsEngineStore.clear();
                            });
                        },
                        templateUrl: 'app/navigation/summary/BlankLine.html'
                    },
                    'main@': {
                        controller: 'ImportWizard',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/wizard.component.html'
                    },
                    'wizard_progress@home.ratingsengine.productpurchase': {
                        controller: 'ImportWizardProgress',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/progress/progress.component.html'

                    },
                    'wizard_controls@home.ratingsengine.productpurchase': {
                        controller: 'ImportWizardControls',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/controls/controls.component.html'
                    }
                },
                redirectTo: 'home.ratingsengine.productpurchase.segment'
            })
            .state('home.ratingsengine.productpurchase.segment', {
                url: '/segment',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Models',
                    section: 'wizard.ratingsengine_segment'
                },
                resolve: {
                    Segments: function (SegmentService) {
                        return SegmentService.GetSegments();
                    },
                    CurrentRatingEngine: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();
                        // console.log('========> ', $stateParams.rating_id);
                        if (!$stateParams.rating_id) {
                            deferred.resolve(RatingsEngineStore.currentRating);
                        } else {
                            RatingsEngineStore.getRating($stateParams.rating_id).then(function (result) {
                                deferred.resolve(result);
                            });
                        }

                        return deferred.promise;
                    }
                },
                views: {
                    'wizard_content@home.ratingsengine.productpurchase': {
                        controller: 'RatingsEngineSegment',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/segment/segment.component.html'
                    }
                }
            })
            .state('home.ratingsengine.productpurchase.segment.products', {
                url: '/products',
                resolve: {
                    Products: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        var params = {
                            max: 1000,
                            offset: 0
                        };
                        RatingsEngineStore.getProducts(params).then(function (result) {
                            deferred.resolve(result);
                        });
                        return deferred.promise;
                    },
                    PeriodType: function($q, QueryStore) {
                        var deferred = $q.defer();
                        QueryStore.getCollectionStatus().then(function(result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    },
                    RatingEngine: function(CurrentRatingEngine, RatingsEngineStore) {
                        if (RatingsEngineStore.currentRating && RatingsEngineStore.currentRating.segment) {
                            return RatingsEngineStore.currentRating;
                        } else {
                            return CurrentRatingEngine;
                        }
                    }
                },
                views: {
                    'wizard_content@home.ratingsengine.productpurchase': {
                        controller: 'RatingsEngineProducts',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/products/products.component.html'
                    }
                }
            })
            .state('home.ratingsengine.productpurchase.segment.products.prioritization', {
                url: '/prioritization',
                resolve: {
                    PredictionType: function ($q, CurrentRatingEngine) {
                        var deferred = $q.defer(),
                            engine = CurrentRatingEngine,
                            predictionType = engine.latest_iteration.AI.predictionType;

                        deferred.resolve(predictionType);

                        return deferred.promise;
                    }
                },
                views: {
                    'wizard_content@home.ratingsengine.productpurchase': {
                        controller: 'RatingsEngineAIPrioritization',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/prioritization/prioritization.component.html'
                    }
                }
            })
            .state('home.ratingsengine.productpurchase.segment.products.prioritization.training', {
                url: '/training',
                resolve: {
                    ratingEngine: ['$q', '$stateParams', 'RatingsEngineStore', function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        RatingsEngineStore.getRating($stateParams.rating_id).then(function (result) {
                            deferred.resolve(result)
                        });

                        return deferred.promise;
                    }],
                    segments: ['SegmentService', function (SegmentService) {
                        return SegmentService.GetSegments();
                    }],
                    products: ['$q', '$stateParams', 'RatingsEngineStore', function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        var params = {
                            max: 1000,
                            offset: 0
                        };
                        RatingsEngineStore.getProducts(params).then(function (result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }],
                    iteration: [function(){
                        return null;
                    }],
                    datacollectionstatus: ['$q', 'QueryStore', function ($q, QueryStore) {
                        var deferred = $q.defer();
                        QueryStore.getCollectionStatus().then(function(result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }]
                },
                views: {
                    'wizard_content@home.ratingsengine.productpurchase': 'ratingsEngineAITraining'
                }
            })
            .state('home.ratingsengine.productpurchase.segment.products.prioritization.training.creation', {
                url: '/creation',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Models',
                },
                resolve: {
                    ratingEngine: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        RatingsEngineStore.getRating($stateParams.rating_id).then(function (result) {
                            deferred.resolve(result)
                        });

                        return deferred.promise;
                    },
                    products: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        var params = {
                            max: 1000,
                            offset: 0
                        };
                        RatingsEngineStore.getProducts(params).then(function (result) {
                            deferred.resolve(result);
                        });
                        return deferred.promise;
                    },
                    datacollectionstatus: function ($q, QueryStore) {
                        var deferred = $q.defer();
                        QueryStore.getCollectionStatus().then(function(result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }
                },
                views: {
                    'wizard_progress@home.ratingsengine.productpurchase': {
                        resolve: {
                        },
                        controller: 'ImportWizardProgress',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/progress/progress.component.html'

                    },
                    'wizard_content@home.ratingsengine.productpurchase': 'ratingsEngineCreation'
                }
            })
            .state('home.ratingsengine.customevent', {
                url: '/custom/:rating_id/:wizard_steps',
                params: {
                    rating_id: '',
                    wizard_steps: 'customevent',
                    fromList: false
                },
                resolve: {
                    WizardValidationStore: function (RatingsEngineStore) {
                        return RatingsEngineStore;
                    },
                    WizardProgressContext: function () {
                        return 'ratingsengine.customevent';
                    },
                    WizardProgressItems: function ($stateParams, RatingsEngineStore) {
                        var rating_id = $stateParams.rating_id || '',
                            wizard_steps = $stateParams.wizard_steps || 'customevent';

                        // console.log(wizard_steps);

                        return RatingsEngineStore.getWizardProgressItems(wizard_steps, rating_id);
                    },
                    CurrentRatingEngine: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer(),
                            ratingId = $stateParams.rating_id;

                        if (ratingId !== '') {
                            RatingsEngineStore.getRating(ratingId).then(function(engine){
                                deferred.resolve(engine);
                            });
                        } else {
                            deferred.resolve();
                        }

                        return deferred.promise;
                    },
                    WizardHeaderTitle: function ($stateParams, CurrentRatingEngine) {
                        var engineType = $stateParams.engineType,
                            currentRating = CurrentRatingEngine,
                            fromList = $stateParams.fromList,
                            title = '';

                        if (currentRating !== undefined && fromList){
                            title = currentRating.displayName;
                        } else {
                            title = 'Create Custom Event Model';
                        }

                        return title;
                    },
                    WizardContainerId: function () {
                        return 'ratingsengine';
                    },
                    WizardControlsOptions: function (RatingsEngineStore) {
                        return {
                            backState: 'home.ratingsengine.ratingsenginetype',
                            secondaryLink: true,
                            nextState: 'home.ratingsengine.dashboard'
                        };
                    }
                },
                views: {
                    'summary@': {
                        controller: function ($state, $scope, RatingsEngineStore) {
                            $scope.$on('$destroy', function () {
                                if (!$state.params.wizard_steps) {
                                    RatingsEngineStore.clear();
                                }
                            });
                        },
                        templateUrl: 'app/navigation/summary/BlankLine.html'
                    },
                    'main@': {
                        controller: 'ImportWizard',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/wizard.component.html'
                    },
                    'wizard_progress@home.ratingsengine.customevent': {
                        controller: 'ImportWizardProgress',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/progress/progress.component.html'

                    },
                    'wizard_controls@home.ratingsengine.customevent': {
                        controller: 'ImportWizardControls',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/controls/controls.component.html'
                    }
                },
                redirectTo: 'home.ratingsengine.customevent.segment'
            })
            .state('home.ratingsengine.customevent.segment', {
                url: '/segment',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Models',
                    section: 'wizard.ratingsengine_segment'
                },
                resolve: {
                    Segments: function (SegmentService) {
                        return SegmentService.GetSegments();
                    },
                    CurrentRatingEngine: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        if (!$stateParams.rating_id) {
                            deferred.resolve(RatingsEngineStore.currentRating);
                        } else {
                            RatingsEngineStore.getRating($stateParams.rating_id).then(function (result) {
                                deferred.resolve(result);
                            });
                        }

                        return deferred.promise;
                    }
                },
                views: {
                    'wizard_content@home.ratingsengine.customevent': {
                        controller: 'RatingsEngineSegment',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/segment/segment.component.html'
                    }
                }
            })
            .state('home.ratingsengine.customevent.segment.training', {
                url: '/training',
                views: {
                    'wizard_content@home.ratingsengine.customevent': {
                        controller: 'RatingsEngineCustomEventTraining',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/training/customeventtraining.component.html'
                    }
                }
            })
            .state('home.ratingsengine.customevent.segment.training.mapping', {
                url: '/mapping',
                resolve: {
                    FieldDocument: function($q, RatingsEngineStore, ImportWizardService, ImportWizardStore) {
                        var deferred = $q.defer();

                        var customEventModelingType = RatingsEngineStore.getCustomEventModelingType();
                        ImportWizardService.GetFieldDocument(RatingsEngineStore.getCSVFileName(), '', customEventModelingType == 'CDL' ? 'Account' : 'SalesforceAccount').then(function(result) {
                            RatingsEngineStore.setFieldDocument(result.Result);
                            deferred.resolve(result.Result);
                        });

                        return deferred.promise;
                    },
                    UnmappedFields: function($q, ImportWizardService, ImportWizardStore) {
                        var deferred = $q.defer();

                        ImportWizardService.GetSchemaToLatticeFields(null).then(function(result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }
                },
                views: {
                    'wizard_content@home.ratingsengine.customevent': {
                        controllerAs: 'vm',
                        controller: 'CustomFieldsController',
                        templateUrl: 'app/create/customfields/CustomFieldsView.html'
                    }
                }
            })
            .state('home.ratingsengine.customevent.segment.training.mapping.creation', {
                url: '/creation',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Models',
                },
                resolve: {
                    ratingEngine: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        RatingsEngineStore.getRating($stateParams.rating_id).then(function (result) {
                            deferred.resolve(result)
                        });

                        return deferred.promise;
                    },
                    products: function () {
                        return [];
                    }
                },
                views: {
                    'wizard_progress@home.ratingsengine.customevent': {
                        controller: 'ImportWizardProgress',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/progress/progress.component.html'

                    },
                    'wizard_content@home.ratingsengine.customevent': 'ratingsEngineCreation'
                }
            });
    });