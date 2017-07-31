angular
.module('lp.create.import')
.config(['$stateProvider', '$urlRouterProvider', function($stateProvider, $urlRouterProvider) {
    $stateProvider
        .state('home.models.import', {
            url: '/import',
            params: {
                pageIcon: 'ico-model',
                pageTitle: 'Create Model - CSV'
            },
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/ModelCreateView.html'
                },
                "main@": {
                    templateUrl: 'app/create/csvform/CSVFormView.html'
                }   
            }
        })
        .state('home.models.pmml', {
            url: '/pmml',
            params: {
                pageIcon: 'ico-model',
                pageTitle: 'Create Model - PMML'
            },
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/PMMLCreateView.html'
                },
                "main@": {
                    templateUrl: 'app/create/pmmlform/PMMLFormView.html'
                }
            }
        })
        .state('home.models.import.columns', {
            url: '/:csvFileName/columns',
            params: {
                pageIcon: 'ico-model',
                pageTitle: 'Create Model - CSV'
            },
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
                    templateUrl: 'app/create/customfields/CustomFieldsView.html'
                }   
            }
        })
        .state('home.model.scoring', {
            url: '/scoring',
            redirectto: 'model.scoring.import',
            views: {
                "summary@": {
                    template: ''
                },
                "main@": {
                    /*
                    resolve: {
                        RequiredFields: function($q, $http, $stateParams) {
                            var deferred = $q.defer(),
                                modelId = $stateParams.modelId;

                            $http({
                                'method': "GET",
                                'url': '/pls/modelsummaries/metadata/required/' + modelId
                            }).then(function(response) {
                                deferred.resolve(response.data);
                            });

                            return deferred.promise; 
                        }
                    },
                    */
                    controller: 'csvBulkUploadController',
                    controllerAs: 'vm',
                    templateUrl: 'app/create/scorefile/ScoreFileView.html'
                }   
            }
        })
        .state('home.model.scoring.mapping', {
            url: '/:csvFileName/mapping',
            params: {
                pageIcon: 'ico-model',
                pageTitle: ''
            },
            views: {
                "summary@": {
                    template: ''
                },
                "main@": {
                    resolve: {
                        FileHeaders: function($q, $stateParams, ImportService, ImportStore) {
                            var deferred = $q.defer();

                            ImportService.GetSchemaToLatticeFields($stateParams.csvFileName).then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        },
                        FieldDocument: function($q, $stateParams, ImportService, ImportStore) {
                            var deferred = $q.defer();

                            ImportService.GetFieldDocument($stateParams.csvFileName, true).then(function(result) {
                                ImportStore.SetFieldDocument($stateParams.csvFileName, result.Result);
                                deferred.resolve(result.Result);
                            });

                            return deferred.promise;
                        }
                    },
                    controllerAs: 'vm',
                    controller: 'ScoreFieldsController',
                    templateUrl: 'app/create/scorefields/ScoreFieldsView.html'
                }   
            }
        })
        .state('home.models.import.job', {
            url: '/:applicationId/job',
            resolve:  {
                BuildProgressConfig: function() {
                    return null;
                }
            },
            params: {
                pageIcon: 'ico-model',
                pageTitle: 'Create Model - CSV'
            },
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/ModelCreateView.html'
                },
                "main@": {
                    controller: 'ImportJobController',
                    templateUrl: 'app/create/buildprogress/BuildProgressView.html'
                }
            }
        })
        .state('home.models.pmml.job', {
            url: '/:applicationId/job',
            params: {
                pageIcon: 'ico-model',
                pageTitle: 'Create Model - PMML'
            },
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/PMMLCreateView.html'
                },
                "main@": {
                    controller: 'ImportJobController',
                    templateUrl: 'app/create/buildprogress/BuildProgressView.html'
                }
            }
        });
}]);