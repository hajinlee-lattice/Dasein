angular
    .module('pd.jobs')
    .config(['$stateProvider', function($stateProvider) {
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
                url: '/jobs',
                views: {
                },
                redirectTo: 'home.model.jobs.status'
            })
            .state('home.model.jobs.status', {
                url: '/status',
                views: {
                    "main@": {
                        /*
                        resolve: {
                            Model: function(ModelStore) {
                                return ModelStore.data;
                            }
                        }
                        */
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
                        templateUrl: 'app/navigation/summary/OneLineView.html'
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
                    JobReport: function($q, $stateParams, JobsService) {
                        var deferred = $q.defer();

                        JobsService.getJobStatus($stateParams.jobId).then(function(result) {
                            var reports = result.resultObj.reports,
                                report = null;

                            reports.forEach(function(item) {
                                if (item.purpose = "EVENT_TABLE_IMPORT_SUMMARY") {
                                    report = item;
                                }
                            });


                            deferred.resolve(report);
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