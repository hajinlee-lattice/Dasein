angular
.module('pd.jobs')
.config(['$stateProvider', '$routeProvider', '$httpProvider', function($stateProvider, $routeProvider, $httpProvider) {
    $stateProvider
        .state('home.jobs', {
            url: '/jobs',
            views: {
                "navigation@": {
                    controller: 'SidebarRootController',
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                }
            },
            redirectTo: 'jobs.status'
        })
        .state('home.model.jobs', {
            url: '/jobs/:{jobCreationSuccess}',
            views: {
                "summary@": {
                    resolve: {
                        ResourceString: function(Model) {
                            var sourceSchemaInterpretation = Model.ModelDetails.SourceSchemaInterpretation;   

                            if (sourceSchemaInterpretation == 'SalesforceAccount') {
                                var modelType = 'MODEL_SCORING_ACCOUNTS_SUMMARY_HEADER';
                            } else {
                                var modelType = 'MODEL_SCORING_LEADS_SUMMARY_HEADER';
                            }

                            return modelType;
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    controller: function($scope, IsPmml) {
                        $scope.IsPmml = IsPmml || false;
                    },
                    templateUrl: 'app/jobs/status/StatusView.html'
                }
            }
        })
        .state('home.jobs.status', {
            url: '/status/:{jobCreationSuccess}',
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'SUMMARY_JOBS_STATUS';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneTabView.html'
                },
                "main@": {
                    templateUrl: 'app/jobs/status/StatusView.html'
                }
            }
        })
        .state('home.jobs.status.ready', {
            url: '/ready/:jobId',
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/table/TableView.html'
                },
                "main@": {
                    templateUrl: 'app/jobs/import/ready/ReadyView.html'
                }
            }
        })
        .state('home.jobs.status.csv', {
            url: '/csv/:jobId',
            resolve: {
                JobResult: function($q, $stateParams, JobsStore, ServiceErrorUtility) {
                    var deferred = $q.defer();

                    JobsStore.getJob($stateParams.jobId).then(function(result) {
                        ServiceErrorUtility.check({ data: result, config: { headers: { ErrorDisplayMethod: 'banner' } } });
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                } 
            },
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'SUMMARY_JOBS_IMPORT_CSV';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    controller: 'CSVReportController',
                    templateUrl: 'app/create/views/ValidateImportView.html'
                }   
            }
        })
        .state('home.jobs.import', {
            url: '/import'
        })
        .state('home.jobs.import.credentials', {
            url: '/credentials',
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/message/MessageView.html'
                },
                "main@": {
                    templateUrl: 'app/jobs/import/credentials/CredentialsView.html'
                }
            }
        })
        .state('home.jobs.import.file', {
            url: '/file',
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/message/MessageView.html'
                },
                "main@": {
                    templateUrl: 'app/jobs/import/file/FileView.html'
                }
            }
        })
        .state('home.jobs.import.processing', {
            url: '/processing',
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/message/MessageView.html'
                },
                "main@": {
                    templateUrl: 'app/jobs/import/processing/ProcessingView.html'
                }
            }
        });
}]);