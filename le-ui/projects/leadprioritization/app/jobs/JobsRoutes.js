angular
.module('lp.jobs')
.config(function($stateProvider, $routeProvider, $httpProvider) {
    $stateProvider
        .state('home.jobs', {
            url: '/jobs',
            views: {
                // "navigation@": {
                //     controller: 'SidebarRootController',
                //     templateUrl: 'app/navigation/sidebar/RootView.html'
                // },
                "summary@": {
                    controller: 'JobsTabsController',
                    controllerAs: 'vm',
                    templateUrl: 'app/jobs/tabs/jobstabs.component.html'
                }
            },
            redirectTo: 'jobs.status'
        })
        .state('home.model.jobs', {
            url: '/jobs',
            params: {
                pageIcon: 'ico-scoring',
                pageTitle: 'Jobs',
                jobCreationSuccess: null
            },
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
                    controller: function($scope, $rootScope, Model, IsPmml) {
                        $scope.IsPmml = IsPmml || false;

                        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });

                    },
                    templateUrl: 'app/jobs/views/ListView.html'
                }
            }
        })
        .state('home.jobs.status', {
            url: '/status',
            params: {
                pageIcon: 'ico-cog',
                pageTitle: 'Jobs',
                jobCreationSuccess: null
            },
            resolve: {
                // This a temporary fix so the Jobs page does not 
                // enable the sidebar when no data is available
                AttributesCount: function($q, $state, DataCloudStore, ApiHost) {
                    var deferred = $q.defer();

                    DataCloudStore.setHost(ApiHost);

                    DataCloudStore.getAttributesCount().then(function(result) {
                        DataCloudStore.setMetadata('enrichmentsTotal', result.data);
                        deferred.resolve(result.data);
                    });
                    return deferred.promise;
                }
            },
            views: {
                "main@": {
                    templateUrl: 'app/jobs/views/ListView.html'
                }
            }
        })
        .state('home.jobs.data', {
            url: '/status',
            views: {
                "main@": {
                    controller: 'DataImportJobsCtrl',
                    templateUrl: 'app/jobs/views/DataImportJobsView.html'
                }
            }
        })
        /*
        .state('home.jobs.status.ready', {
            url: '/ready/:jobId',
            params: {
                pageIcon: 'ico-cog',
                pageTitle: 'View Report'
            },
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/table/TableView.html'
                },
                "main@": {
                    templateUrl: 'app/jobs/import/ready/ReadyView.html'
                }
            }
        })
        */
        .state('home.jobs.status.csv', {
            url: '/csv/:jobId',
            params: {
                pageIcon: 'ico-cog',
                pageTitle: 'View Report'
            },
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
                    templateUrl: 'app/create/jobreport/JobReportView.html'
                }
            }
        });
});