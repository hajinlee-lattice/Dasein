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
    'lp.ratingsengine.wizard.summary',
    'lp.ratingsengine.ai',
    'lp.ratingsengine.ai.prospect',
    'lp.ratingsengine.ai.prospect.prospect-graph',
    'lp.ratingsengine.ai.products'
])
.config(function($stateProvider, DataCloudResolvesProvider) {
    $stateProvider
        .state('home.ratingsengine', {
            url: '/ratings_engine',
            redirectTo: 'home.ratingsengine.list.ratings'
        })
        .state('home.ratingsengine.list', {
            url: '/list',
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
            redirectTo: 'home.ratingsengine.list.ratings'
        })
        .state('home.ratingsengine.list.ratings', {
            url: '/ratings',
            params: {
                pageIcon: 'ico-model',
                pageTitle: 'Rating Engines'
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
                pageTitle: 'Rating Engines'
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
                pageIcon: 'ico-model',
                pageTitle: 'Rating Engines'
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
                pageIcon: 'ico-model',
                pageTitle: 'Rating Engine'
            },
            resolve: {
                Rating: function($q, $stateParams, RatingsEngineStore) {
                    var deferred = $q.defer();
                    var rating_id = $stateParams.rating_id || RatingsEngineStore.getRatingId();

                    RatingsEngineStore.getRatingDashboard(rating_id).then(function(data) {
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
                        $scope.rating_id = $stateParams.rating_id || '';
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
            url: '/wizard/:rating_id/:wizard_steps',
            params: {
                wizard_steps: 'all'
            },
            resolve: {
                WizardValidationStore: function(RatingsEngineStore) {
                    return RatingsEngineStore;
                },
                WizardProgressContext: function() {
                    return 'ratingsengine';
                },
                WizardProgressItems: function($stateParams, RatingsEngineStore) {
                    var rating_id = $stateParams.rating_id || '';
                    var wizard_steps = $stateParams.wizard_steps;

                    return RatingsEngineStore.getWizardProgressItems(wizard_steps || 'all', rating_id);
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
                        WizardControlsOptions: function(RatingsEngineStore) {
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
            redirectTo: 'home.ratingsengine.wizard.segment'
        })
        .state('home.ratingsengine.ai', {
            url: '/ai',
            params: {
                wizard_steps: 'ai'
            },
            resolve: {
                WizardValidationStore: function(RatingsEngineStore) {
                    return RatingsEngineStore;
                },
                WizardProgressContext: function() {
                    return 'ratingsengine.ai';
                },
                WizardProgressItems: function($stateParams, RatingsEngineStore) {
                    var rating_id = $stateParams.rating_id || '';
                    var wizard_steps = $stateParams.wizard_steps;

                    return RatingsEngineStore.getWizardProgressItems(wizard_steps || 'ai', rating_id);
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
                            return 'ratingsengine.ai';
                        }
                    },
                    controller: 'ImportWizard',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/wizard.component.html'
                },
                'wizard_progress@home.ratingsengine.ai': {
                    controller: 'ImportWizardProgress',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/progress/progress.component.html'
                    
                },
                'wizard_controls@home.ratingsengine.ai': {
                    resolve: {
                        WizardControlsOptions: function(RatingsEngineStore) {
                            return { 
                                suffix : '',
                                backState: 'home.ratingsengine.list', 
                                nextState: 'home.ratingsengine.list'
                            };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            },
            redirectTo: 'home.ratingsengine.ai.segment'
        })
        .state('home.ratingsengine.ai.segment.prospect', {
            url: '/prospect',
            resolve: {
                WizardHeaderTitle: function() {
                    return 'Select the model to build';
                },
                Prospect: function($q, $stateParams, RatingsEngineStore, RatingsEngineAIService, RatingsEngineAIStore) {
                    var deferred = $q.defer();
                    var segment_id = 15;

                    RatingsEngineAIService.getProspect(segment_id).then(function(data) {
                        deferred.resolve(data);
                    });

                    return deferred.promise;
                }
            },
            views: {
                
                'wizard_content@home.ratingsengine.ai': {
                    controller: 'RatingsEngineAIProspect',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/ai/prospect/prospect.component.html'
                },
                'prospect-graph@home.ratingsengine.ai.segment.prospect': {
                    controller: 'RatingsEngineAIProspectGraph',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/ai/prospect/prospect-graph.component.html'
                }
            }
        })
        .state('home.ratingsengine.ai.segment.prospect.products', {
            url: '/products',
            resolve: {
                ProductsCount : function($q, RatingsEngineAIService){
                    var deferred = $q.defer();
                    var segment_id = 15;

                    RatingsEngineAIService.getProductsCount(segment_id).then(function(data) {
                        deferred.resolve(data);
                    });

                    return deferred.promise;
                }
            },
            views: {
                
                'wizard_content@home.ratingsengine.ai': {
                    controller: 'RatingsEngineAIProducts',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/ai/products/products.component.html'
                }
            }
        })
        .state('home.ratingsengine.ai.segment.prospect.products.settings', {
            url: '/products',
            views: {
                
                'wizard_content@home.ratingsengine.ai': {
                    controller: function(RatingsEngineStore, RatingsEngineAIStore){
                        console.log(RatingsEngineAIStore.getProductsSelected());
                        RatingsEngineStore.setValidation('settings', true);
                    },
                    templateUrl: 'app/ratingsengine/content/ai/model/ai-model-settings.component.html'
                }
            }
        })
        .state('home.ratingsengine.ai.segment.prospect.products.settings.model', {
            url: '/products',
            views: {
                
                'wizard_content@home.ratingsengine.ai': {
                    
                    templateUrl: 'app/ratingsengine/content/ai/model/ai-model.component.html'
                }
            }
        })
        .state('home.ratingsengine.ai.segment', {
            url: '/segment',
            
            resolve: {
                Segments: function(SegmentService) {
                    return SegmentService.GetSegments();
                },
                CurrentRatingEngine: function($q, $stateParams, RatingsEngineStore) {
                    var deferred = $q.defer();

                    if (!$stateParams.rating_id) {
                        deferred.resolve(RatingsEngineStore.currentRating);
                    } else {
                        RatingsEngineStore.getRating($stateParams.rating_id).then(function(result) {
                            deferred.resolve(result);
                        });
                    }

                    return deferred.promise;
                },
                RatingsEngineStore : function(RatingsEngineStore){
                    return RatingsEngineStore;
                }
            },
            views: {
                'wizard_content@home.ratingsengine.ai': {
                   
                    controller: function($state){
                        var vm = this;
                        vm.goToCreateSegment = function() {
                            $state.go('home.segment.explorer.attributes');
                        }
                    },
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/ai/segment/ai-segment.component.html',

                },
                'segment_view@home.ratingsengine.ai.segment': {
                    controller: 'RatingsEngineSegment',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/segment/segment.component.html'
                }
            }
        })

        .state('home.ratingsengine.wizard.segment', {
            url: '/segment',
            params: {
                pageIcon: 'ico-model',
                pageTitle: 'Rating Engines',
                section: 'wizard.ratingsengine_segment'
            },
            resolve: {
                Segments: function(SegmentService) {
                    return SegmentService.GetSegments();
                },
                CurrentRatingEngine: function($q, $stateParams, RatingsEngineStore) {
                    var deferred = $q.defer();

                    if (!$stateParams.rating_id) {
                        deferred.resolve(RatingsEngineStore.currentRating);
                    } else {
                        RatingsEngineStore.getRating($stateParams.rating_id).then(function(result) {
                            deferred.resolve(result);
                        });
                    }

                    return deferred.promise;
                }
            },
            views: {
                'wizard_content@home.ratingsengine.wizard': {
                    controller: 'RatingsEngineSegment',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/segment/segment.component.html'
                }
            }
        })
        .state('home.ratingsengine.wizard.segment.attributes', {
            url: '/attributes',
            params: {
                pageIcon: 'ico-model',
                pageTitle: 'Rating Engines',
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
            }
        })
        .state('home.ratingsengine.wizard.segment.attributes.rules', {
            url: '/rules',
            params: {
                pageIcon: 'ico-model',
                pageTitle: 'Rating Engines'
            },
            resolve: {
                Cube: function($q, DataCloudStore){
                    var deferred = $q.defer();

                    DataCloudStore.getCube().then(function(result) {
                        deferred.resolve(result.data.Stats);
                    });
                    
                    return deferred.promise;
                },
                RatingEngineModel: function(DataCloudStore, RatingsEngineModels) {
                    var selectedAttributes = DataCloudStore.getCurrentRatingsEngineAttributes();

                    if (selectedAttributes) {
                        RatingsEngineModels.rule.selectedAttributes = selectedAttributes;
                    }

                    return RatingsEngineModels;
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
                pageIcon: 'ico-model',
                pageTitle: 'Rating Engines',
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
            }
        });
});