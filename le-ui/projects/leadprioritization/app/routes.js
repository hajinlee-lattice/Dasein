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
        .state('model', {
            url: '/model/:modelId',
            resolve: ModelDependencies,
            views: {
                "navigation@": {
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
        .state('model.activate', {
            url: '/activate',
            views: {
                "main@": {
                    templateUrl: './app/models/views/ActivateModelView.html'
                }
            }
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
        });
}]);