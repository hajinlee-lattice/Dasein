angular
.module('lp.create.import')
.config(['$stateProvider', '$urlRouterProvider', function($stateProvider, $urlRouterProvider) {
    $stateProvider
        .state('home.models.import', {
            url: '/import',
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/ModelCreateView.html'
                },
                "main@": {
                    templateUrl: 'app/create/views/CSVImportView.html'
                }   
            }
        })
        .state('home.models.pmml', {
            url: '/pmml',
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/ModelListView.html'
                },
                "main@": {
                    templateUrl: 'app/create/views/PMMLImportView.html'
                }
            }
        })
        .state('home.models.import.columns', {
            url: '/:csvFileName/columns',
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/ModelCreateView.html'
                },
                "main@": {
                    resolve: {
                        FieldDocument: function($q, $stateParams, ImportService, ImportStore) {
                            var deferred = $q.defer();

                            ImportService.GetFieldDocument($stateParams.csvFileName).then(function(result) {
                                ImportStore.SetFieldDocument($stateParams.csvFileName, result.Result);
                                deferred.resolve(result.Result);
                            });

                            return deferred.promise;
                        },
                        UnmappedFields: function($q, $stateParams, ImportService, ImportStore) {
                            var deferred = $q.defer();

                            ImportService.GetSchemaToLatticeFields().then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        }
                    },
                    controllerAs: 'vm',
                    controller: 'CustomFieldsController',
                    templateUrl: 'app/create/views/CustomFieldsView.html'
                }   
            }
        })
        .state('home.models.import.job', {
            url: '/:applicationId/job',
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/ModelCreateView.html'
                },
                "main@": {
                    controller: 'ImportJobController',
                    templateUrl: 'app/create/views/ImportJobView.html'
                }
            }
        });
}]);