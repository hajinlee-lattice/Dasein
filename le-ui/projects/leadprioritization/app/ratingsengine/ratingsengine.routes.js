angular
    .module('lp.ratingsengine', [
        'common.wizard',
        'common.datacloud',
        'lp.segments.segments',
        'lp.ratingsengine.ratingsenginetabs',
        'lp.ratingsengine.ratingslist',
        'lp.ratingsengine.creationhistory',
        'lp.ratingsengine.ratingsenginetype',
        'lp.ratingsengine.dashboard',
        'lp.ratingsengine.activatescoring',
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
                    pageTitle: 'Models'
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
                url: '/dashboard/:modelId/:rating_id',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Model'
                },
                resolve: {
                    Dashboard: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();
                        var rating_id = $stateParams.rating_id || RatingsEngineStore.getRatingId();

                        RatingsEngineStore.getRatingDashboard(rating_id).then(function (data) {
                            deferred.resolve(data);
                        });

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
                    Model: function($q, $stateParams, ModelStore, RatingEngine) {
                        var deferred = $q.defer(),
                            id = $stateParams.modelId;

                        // console.log(id);
                        // console.log(ModelStore.data);
                        // console.log(RatingEngine);

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
                    }
                },
                views: {
                    "summary@": {
                        controller: 'ModelDetailController',
                        template: '<div id="ModelDetailsArea"></div>'
                    },
                    "navigation@home": {
                        controller: function ($scope, $stateParams, $state, $rootScope, Dashboard, RatingEngine) {
                            $scope.rating_id = $stateParams.rating_id || '';
                            $scope.modelId = $stateParams.modelId || '';
                            $scope.isRuleBased = (RatingEngine.type === 'RULE_BASED');


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

                                $scope.modelingStrategy = RatingEngine.activeModel.AI.advancedModelingConfig[type].modelingStrategy;
                            }
                            $scope.activeIteration = RatingEngine.activeModel[$scope.typeContext].iteration;
                            $scope.modelIsReady = (RatingEngine.activeModel[$scope.typeContext].modelSummary !== null || RatingEngine.activeModel[$scope.typeContext].modelSummary !== undefined);

                            // console.log(RatingEngine);

                            $scope.stateName = function () {
                                return $state.current.name;
                            }
                            $rootScope.$broadcast('header-back', {
                                path: '^home.rating.dashboard',
                                displayName: Dashboard.summary.displayName,
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
            .state('home.ratingsengine.dashboard.rules', {
                url: '/rules',
                resolve: {
                    EnrichmentCount: ['$q', 'DataCloudStore', 'ApiHost', function ($q, DataCloudStore, ApiHost) {
                        var deferred = $q.defer();

                        DataCloudStore.setHost(ApiHost);

                        DataCloudStore.getCount().then(function (result) {
                            DataCloudStore.setMetadata('enrichmentsTotal', result.data);
                            deferred.resolve(result.data);
                        });

                        return deferred.promise;
                    }],
                    Enrichments: ['$q', 'DataCloudStore', 'ApiHost', 'EnrichmentCount', function ($q, DataCloudStore, ApiHost, EnrichmentCount) {
                        var deferred = $q.defer();

                        DataCloudStore.setHost(ApiHost);

                        DataCloudStore.getAllEnrichmentsConcurrently(EnrichmentCount).then(function (result) {
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
                    // below resolves are needed. Do not removed
                    // override at child state when needed
                    LookupResponse: [function () {
                        return { attributes: null };
                    }],
                    QueryRestriction: [function () {
                        return null;
                    }],
                    CurrentConfiguration: [function () {
                        return null;
                    }],
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

                        // console.log(RatingsEngineModels);

                        if (selectedAttributes) {
                            RatingsEngineModels.rule.selectedAttributes = selectedAttributes;
                        }

                        return RatingsEngineModels;
                    }
                },
                views: {
                    "navigation@home": {
                        controller: function ($scope, $stateParams, $state, $rootScope, Dashboard, RatingEngine) {
                            $scope.rating_id = $stateParams.rating_id || '';
                            $scope.modelId = $stateParams.modelId || '';
                            $scope.isRuleBased = (RatingEngine.type === 'RULE_BASED');
                            $scope.stateName = function () {
                                return $state.current.name;
                            }
                            $rootScope.$broadcast('header-back', {
                                path: '^home.rating.dashboard',
                                displayName: Dashboard.summary.displayName,
                                sref: 'home.ratingsengine'
                            });
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
            .state('home.ratingsengine.dashboard.notes', {
                url: '/notes',
                resolve: {
                    Rating: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        RatingsEngineStore.getRating($stateParams.rating_id).then(function (result) {
                            deferred.resolve(result)
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
                    section: 'dashboard.notes'
                },
                views: {
                    "navigation@home": {
                        controller: function ($scope, $stateParams, $state, $rootScope, Dashboard, RatingEngine) {
                            $scope.rating_id = $stateParams.rating_id || '';
                            $scope.modelId = $stateParams.modelId || '';
                            $scope.isRuleBased = (RatingEngine.type === 'RULE_BASED');
                            $scope.stateName = function () {
                                return $state.current.name;
                            }
                            $rootScope.$broadcast('header-back', {
                                path: '^home.rating.dashboard',
                                displayName: Dashboard.summary.displayName,
                                sref: 'home.ratingsengine'
                            });
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
            .state('home.ratingsengine.dashboard.activatescoring', {
                url: '/activatescoring',
                resolve: {
                    Tmp: function($q, $stateParams) {
                        var deferred = $q.defer();

                        deferred.resolve([]);

                        return deferred.promise;
                    }
                },
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Activate Scoring',
                    section: 'dashboard.activatescoring'
                },
                views: {
                    "navigation@home": {
                        controller: function ($scope, $stateParams, $state, $rootScope, Dashboard, RatingEngine) {
                            $scope.rating_id = $stateParams.rating_id || '';
                            $scope.modelId = $stateParams.modelId || '';
                            $scope.isRuleBased = (RatingEngine.type === 'RULE_BASED');
                            $scope.stateName = function () {
                                return $state.current.name;
                            }
                            $rootScope.$broadcast('header-back', {
                                path: '^home.rating.dashboard',
                                displayName: Dashboard.summary.displayName,
                                sref: 'home.ratingsengine'
                            });
                        },
                        templateUrl: 'app/ratingsengine/content/dashboard/sidebar/sidebar.component.html'
                    },
                    "main@": {
                        controller: 'RatingsEngineActivateScoring',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/activatescoring/activatescoring.component.html'
                    }
                }
            })
            .state('home.ratingsengine.rulesprospects', {
                url: '/rules/:rating_id/:wizard_steps',
                params: {
                    rating_id: '',
                    wizard_steps: 'rulesprospects'
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

                        // console.log(wizard_steps);

                        return RatingsEngineStore.getWizardProgressItems(wizard_steps, rating_id);
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
                        resolve: {
                            WizardHeaderTitle: function () {
                                return false;
                            },
                            WizardContainerId: function () {
                                return 'ratingsengine';
                            }
                        },
                        controller: 'ImportWizard',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/wizard.component.html'
                    },
                    'wizard_header@home.ratingsengine.rulesprospects': {
                        resolve: {
                            WizardHeaderTitle: function () {
                                return 'Create Model';
                            },
                            WizardCustomHeaderSteps: function() {
                                return ['segment.attributes', 'segment.attributes.add', 'segment.attributes.rules'];
                            }
                        },
                        controller:'WizardHeader',
                        controllerAs:'vm',
                        templateUrl: '/components/wizard/header/header.component.html'
                    },
                    'wizard_progress@home.ratingsengine.rulesprospects': {
                        resolve: {
                            DisableWizardNavOnLastStep: function () {
                                return null;
                            }
                        },
                        controller: 'ImportWizardProgress',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/progress/progress.component.html'

                    },
                    'wizard_controls@home.ratingsengine.rulesprospects': {
                        resolve: {
                            WizardControlsOptions: function (RatingsEngineStore) {
                                return {
                                    backState: 'home.ratingsengine',
                                    nextState: 'home.ratingsengine.dashboard'
                                };
                            }
                        },
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
                    pageTitle: 'Models',
                    section: 'wizard.ratingsengine_attributes',
                    gotoNonemptyCategory: true
                },
                //resolve: angular.extend({}, DataCloudResolvesProvider.$get().main, {
                /**
                 * for now we're ducplciating these here from datacloud.routes because when minified the resolves fail
                 */
                resolve: {
                    EnrichmentCount: ['$q', 'DataCloudStore', 'ApiHost', function ($q, DataCloudStore, ApiHost) {
                        var deferred = $q.defer();

                        DataCloudStore.setHost(ApiHost);

                        DataCloudStore.getCount().then(function (result) {
                            DataCloudStore.setMetadata('enrichmentsTotal', result.data);
                            deferred.resolve(result.data);
                        });

                        return deferred.promise;
                    }],
                    Enrichments: ['$q', 'DataCloudStore', 'ApiHost', 'EnrichmentCount', function ($q, DataCloudStore, ApiHost, EnrichmentCount) {
                        var deferred = $q.defer();

                        DataCloudStore.setHost(ApiHost);

                        DataCloudStore.getAllEnrichmentsConcurrently(EnrichmentCount).then(function (result) {
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
                    // below resolves are needed. Do not removed
                    // override at child state when needed
                    LookupResponse: [function () {
                        return { attributes: null };
                    }],
                    QueryRestriction: [function () {
                        return null;
                    }],
                    CurrentConfiguration: [function () {
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
                    pageTitle: 'Models',
                    section: 'wizard.ratingsengine_attributes',
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
                    pageTitle: 'Models'
                },
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
                    pageTitle: 'Models',
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
                    engineType: 'CROSS_SELL_FIRST_PURCHASE',
                    displayName: '',
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
                        resolve: {
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
                                } else if (engineType === 'CROSS_SELL_FIRST_PURCHASE') {
                                    title = 'Create Model: Customers that will purchase a product for the first time';
                                } else if (engineType === 'CROSS_SELL_REPEAT_PURCHASE') {
                                    title = 'Create Model: Customers that will purchase again next quarter';
                                }

                                return title;
                            },
                            WizardContainerId: function () {
                                return 'ratingsengine';
                            }
                        },
                        controller: 'ImportWizard',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/wizard.component.html'
                    },
                    'wizard_progress@home.ratingsengine.productpurchase': {
                        resolve: {
                            DisableWizardNavOnLastStep: function () {
                                return null;
                            }
                        },
                        controller: 'ImportWizardProgress',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/progress/progress.component.html'

                    },
                    'wizard_controls@home.ratingsengine.productpurchase': {
                        resolve: {
                            WizardControlsOptions: function (RatingsEngineStore) {
                                return {
                                    backState: 'home.ratingsengine',
                                    secondaryLink: true,
                                    nextState: 'home.ratingsengine.dashboard'
                                };
                            }
                        },
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
                            predictionType = engine.activeModel.AI.predictionType;

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
                    Rating: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        RatingsEngineStore.getRating($stateParams.rating_id).then(function (result) {
                            deferred.resolve(result)
                        });

                        return deferred.promise;
                    },
                    Segments: function (SegmentService) {
                        return SegmentService.GetSegments();
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
                    }
                },
                views: {
                    'wizard_content@home.ratingsengine.productpurchase': {
                        controller: 'RatingsEngineAITraining',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/training/training.component.html'
                    }
                }
            })
            .state('home.ratingsengine.productpurchase.segment.products.prioritization.training.creation', {
                url: '/creation',
                params: {
                    pageIcon: 'ico-model',
                    pageTitle: 'Models',
                },
                resolve: {
                    Rating: function ($q, $stateParams, RatingsEngineStore) {
                        var deferred = $q.defer();

                        RatingsEngineStore.getRating($stateParams.rating_id).then(function (result) {
                            deferred.resolve(result)
                        });

                        return deferred.promise;
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
                    }
                },
                views: {
                    'wizard_progress@home.ratingsengine.productpurchase': {
                        resolve: {
                            DisableWizardNavOnLastStep: function () {
                                return true;
                            }
                        },
                        controller: 'ImportWizardProgress',
                        controllerAs: 'vm',
                        templateUrl: '/components/wizard/progress/progress.component.html'

                    },
                    'wizard_content@home.ratingsengine.productpurchase': {
                        controller: 'RatingsEngineCreation',
                        controllerAs: 'vm',
                        templateUrl: 'app/ratingsengine/content/creation/creation.component.html'
                    }
                }
            });
    });