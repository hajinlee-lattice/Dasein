angular
.module('mainApp')

// add ability to redirect with redirectTo
.run(['$rootScope', '$state', function($rootScope, $state) {
    $rootScope.$on('$stateChangeStart', function(evt, to, params) {
      if (to.redirectTo) {
        evt.preventDefault();
        $state.go(to.redirectTo, params)
      }
    });
}])

// define routes for PD application.
.config(['$stateProvider', '$urlRouterProvider', function($stateProvider, $urlRouterProvider) {
    var ModelDependencies = {
        Model: function($q, $stateParams, ModelStore) {
            var deferred = $q.defer(),
                id = $stateParams.modelId;
            
            ModelStore.getModel(id).then(function(result) {
                console.log('resolve',id,result);
                deferred.resolve(result);
            });

            return deferred.promise;
        },
        screenWidgetConfig: function($q, $stateParams, WidgetService, WidgetConfigUtility) {
            var widgetConfig = WidgetService.GetApplicationWidgetConfig();

            return WidgetConfigUtility.GetWidgetConfig(
                widgetConfig,
                "modelDetailsScreenWidget"
            );
        },
        ChartData: function(TopPredictorService, Model) {
            return TopPredictorService.FormatDataForTopPredictorChart(Model);
        },
        InternalAttributes: function(TopPredictorService, Model, ChartData) {
            return TopPredictorService.GetNumberOfAttributesByCategory(ChartData.children, false, Model);
        },
        ExternalAttributes: function(TopPredictorService, Model, ChartData) {
            return TopPredictorService.GetNumberOfAttributesByCategory(ChartData.children, true, Model);
        },
        TotalAttributeValues: function(TopPredictorService, Model, InternalAttributes, ExternalAttributes) {
            return InternalAttributes.totalAttributeValues + ExternalAttributes.totalAttributeValues;
        }
    };
    $urlRouterProvider.otherwise('/');

    $stateProvider
        .state('home', {
            url: '/',
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
                        console.log('MODEL', Model);
                        $scope.name = Model.ModelDetails.Name;
                    },
                    templateUrl: './app/navigation/sidebar/ModelView.html'
                },
                "summary@": {
                    controller: 'ModelDetailsWidgetController',
                    templateUrl: './app/AppCommon/widgets/modelDetailsWidget/ModelDetailsWidgetTemplate.html'
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
                    controller: 'TopPredictorWidgetController',
                    templateUrl: './app/AppCommon/widgets/topPredictorWidget/TopPredictorWidgetTemplate.html'
                }
            }
        })
        .state('model.performance', {
            url: '/attributes',
            views: {
                "main@": {
                    controller: 'performanceTabWidgetController',
                    templateUrl: './app/AppCommon/widgets/performanceTabWidget/PerformanceTabTemplate.html'
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
        });
}]);