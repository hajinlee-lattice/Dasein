angular
.module('mainApp')

// add ability to redirect with redirectTo
.run(['$rootScope', '$state', 'ResourceUtility', function($rootScope, $state, ResourceUtility) {

    $rootScope.$on('$stateChangeStart', function(evt, to, params) {
        var LoadingString = ResourceUtility.getString("GENERAL_LOADING");

        if (to.redirectTo) {
            evt.preventDefault();
            $state.go(to.redirectTo, params)
        }

        // state change spinner
        $('#mainContentView').html(
            '<section id="main-content" class="container">' +
            '<div class="row twelve columns"><div class="loader"></div>' +
            '<h2 class="text-center">' + LoadingString + '</h2></div></section>');
    });
    /*
    $rootScope.$on('$stateChangeSuccess', function(evt, to, params) {
        console.log('END');
    });
    */
}])

// define routes for PD application.
.config(['$stateProvider', '$urlRouterProvider', function($stateProvider, $urlRouterProvider) {
    var UnderConstruction = '<div style="text-align:center;margin-top:5em;"><img src="/assets/images/headbang.gif" /></div>',
        ModelDependencies = {
            Model: function($q, $stateParams, ModelStore) {
                var deferred = $q.defer(),
                    id = $stateParams.modelId;
                
                ModelStore.getModel(id).then(function(result) {
                    deferred.resolve(result);
                });

                return deferred.promise;
            }
        };

    $urlRouterProvider.otherwise('/');

    $stateProvider
        .state('home', {
            url: '/', // '/:tenantId',
            /* eventually... detect tenantId/choose tenantId
            resolve: {
                Tenant: function($q, $stateParams, $scope, $controller) {
                    var deferred = $q.defer(),
                        tenantId = $stateParams.tenantId,
                        LoginCtrl = $controller('LoginController', {});

                    if (tenantId)
                        deferred.resolve(true);
                    
                    console.log('hi',deferred,tenantId,LoginCtrl);
                    return deferred.promise;
                }
            },
            */
            redirectTo: 'models'
        })
        .state('models', {
            url: '/models',
            views: {
                "summary@": {
                    templateUrl: './app/navigation/summary/ModelListView.html'
                },
                "main@": {
                    templateUrl: './app/models/views/ModelListView.html'
                }   
            }
        })
        .state('models.import', {
            url: '/import',
            views: {
                "navigation@": {
                    templateUrl: './app/navigation/sidebar/CreateView.html'
                },
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'First, setup the model.';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: './app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    templateUrl: './app/create/views/SetupImportView.html'
                }   
            }
        })
        .state('models.fields', {
            url: '/fields',
            views: {
                "navigation@": {
                    templateUrl: './app/navigation/sidebar/CreateView.html'
                },
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'Success! The training file has been imported';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: './app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    templateUrl: './app/create/views/CustomFieldsView.html'
                }   
            }
        })
        .state('models.validate', {
            url: '/validate',
            views: {
                "navigation@": {
                    templateUrl: './app/navigation/sidebar/CreateView.html'
                },
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'Summary of Imported Data';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: './app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    templateUrl: './app/create/views/ValidateImportView.html'
                }   
            }
        })
        .state('models.create', {
            url: '/create',
            views: {
                "navigation@": {
                    templateUrl: './app/navigation/sidebar/CreateView.html'
                },
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'The model is ready to be created.';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: './app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    templateUrl: './app/create/views/CreateModelView.html'
                }   
            }
        })
        .state('model', {
            url: '/model/:modelId',
            resolve: ModelDependencies,
            views: {
                "navigation@": {
                    controller: function($scope, Model) {
                        $scope.name = Model.ModelDetails.Name;
                    },
                    templateUrl: './app/navigation/sidebar/ModelView.html'
                },
                "summary@": {
                    controller: 'ModelDetailController',
                    template: '<div id="ModelDetailsArea"></div>'
                },
                "main@": {
                    template: ''
                }
            }
        })
        .state('model.attributes', {
            url: '/attributes',
            views: {
                "main@": {
                    controller: function($scope, $compile, ModelStore) {
                        $scope.data = ModelStore.data;
                        $compile($('#modelDetailContainer').html('<div id="modelDetailsAttributesTab" class="tab-content" data-top-predictor-widget></div>'))($scope);
                    },
                    template: '<div id="modelDetailContainer" class="model-details"></div>'
                }
            }
        })
        .state('model.performance', {
            url: '/performance',
            views: {
                "main@": {
                    controller: function($scope, $compile, ModelStore) {
                        $scope.data = ModelStore.data;
                        $compile($('#modelDetailContainer').html('<div id="performanceTab" class="tab-content" data-performance-tab-widget></div>'))($scope);
                    },
                    template: '<div id="modelDetailContainer" class="model-details"></div>'
                }
            }
        })
        .state('model.leads', {
            url: '/leads',
            views: {
                "main@": {
                    controller: function($scope, $compile, ModelStore) {
                        $scope.data = ModelStore.data;
                        $compile($('#modelDetailContainer').html('<div id="modelDetailsLeadsTab" class="tab-content" data-leads-tab-widget></div>'))($scope);
                    },
                    template: '<div id="modelDetailContainer" class="model-details"></div>'
                }
            }
        })
        .state('model.summary', {
            url: '/summary',
            views: {
                "main@": {
                    controller: function($scope, $compile, ModelStore) {
                        $scope.data = ModelStore.data;
                    },
                    templateUrl: './app/AppCommon/widgets/adminInfoSummaryWidget/AdminInfoSummaryWidgetTemplate.html'
                }   
            }
        })
        .state('model.alerts', {
            url: '/alerts',
            views: {
                "main@": {
                    resolve: {
                        ModelAlertsTmp: function($q, Model, ModelService) {
                            var deferred = $q.defer(),
                                data = Model,
                                id = data.ModelDetails.ModelID,
                                result = {};

                            var suppressedCategories = data.SuppressedCategories;

                            ModelService.GetModelAlertsByModelId(id).then(function(result) {
                                if (result != null && result.success === true) {
                                    data.ModelAlerts = result.resultObj;
                                    data.SuppressedCategories = suppressedCategories;
                                    deferred.resolve(result);
                                } else if (result != null && result.success === false) {
                                    data.ModelAlerts = result.resultObj;
                                    data.SuppressedCategories = null;
                                    deferred.reject('nope');
                                }
                            });

                            return deferred.promise;
                        }
                    },
                    controller: function($scope, ModelStore) {
                        $scope.data = ModelStore.data;
                    },
                    templateUrl: './app/AppCommon/widgets/adminInfoAlertsWidget/AdminInfoAlertsWidgetTemplate.html'
                }   
            }
        })
        .state('model.scoring', {
            url: '/scoring',
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'UNDER CONSTRUCTION';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: './app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    template: UnderConstruction
                }   
            }
        })
        .state('model.refine', {
            url: '/refine',
            views: {
                "main@": {
                    controller: 'SetupController',
                    templateUrl: './app/setup/views/SetupView.html'
                }   
            }
        })
        .state('activate', {
            url: '/activate',
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'ACTIVATE_MODEL_TITLE';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: './app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    templateUrl: './app/models/views/ActivateModelView.html'
                }
            }
        })
        .state('users', {
            url: '/users',
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'USER_MANAGEMENT_TITLE';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: './app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    templateUrl: './app/userManagement/views/UserManagementView.html'
                }   
            }
        })
        .state('setup', {
            url: '/setup',
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'SYSTEM_SETUP_TITLE';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: './app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    templateUrl: './app/config/views/ManageCredentialsView.html'
                }   
            }
        })
        .state('history', {
            url: '/history',
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'MODEL_LIST_CREATION_HISTORY';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: './app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    templateUrl: './app/models/views/ModelCreationHistoryView.html'
                }   
            }
        })
        .state('fields', {
            url: '/fields',
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'SETUP_NAV_NODE_MANAGE_FIELDS';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: './app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    controller: 'SetupController',
                    templateUrl: './app/setup/views/SetupView.html'
                }   
            }
        })
        .state('dashboard', {
            url: '/dashboard',
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'UNDER CONSTRUCTION';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: './app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    template: UnderConstruction
                }   
            }
        })
        .state('enrichment', {
            url: '/enrichment',
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'LEAD_ENRICHMENT_SETUP_TITLE';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: './app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    controller: 'LeadEnrichmentController',
                    templateUrl: './app/setup/views/LeadEnrichmentView.html'
                }   
            }
        });
}]);