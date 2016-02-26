angular
    .module('pd.jobs')
    .config(['$stateProvider', function($stateProvider) {
        $stateProvider
            .state('jobs', {
                url: '/jobs',
                redirectTo: 'jobs.status'
            })
            .state('jobs.status', {
                url: '/status',
                views: {
                    "summary@": {
                        resolve: { 
                            ResourceString: function() {
                                return 'Jobs Page';
                            }
                        },
                        controller: 'OneLineController',
                        templateUrl: './app/navigation/summary/OneLineView.html'
                    },
                    "main@": {
                        templateUrl: './app/jobs/status/StatusView.html'
                    }
                }
            })
            .state('jobs.status.ready', {
                url: '/ready/:jobId',
                views: {
                    "summary@": {
                        templateUrl: './app/navigation/table/TableView.html'
                    },
                    "main@": {
                        templateUrl: './app/jobs/import/ready/ReadyView.html'
                    }
                }
            })
            .state('jobs.status.csv', {
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
                                return 'Summary of Imported Data';
                            }
                        },
                        controller: 'OneLineController',
                        templateUrl: './app/navigation/summary/OneLineView.html'
                    },
                    "main@": {
                        controller: function($scope, JobsService, JobReport) {
                            $scope.report = JobReport;
                            $scope.data = data = JSON.parse(JobReport.json.Payload);
                            $scope.data.used_records = data.imported_records - data.ignored_records;
                            $scope.errorlog = '/pls/fileuploads/' + JobReport.name.replace('_Report','') + '/import/errors';

                            $scope.clickGetErrorLog = function($event) {
                                console.log('click', JobReport);

                                JobsService.getErrorLog(JobReport).then(function(result) {
                                    console.log('success',result);

                                    var blob = new Blob([ JSON.stringify(result) ], {
                                        type: "text/plain;charset=utf-8"
                                    });
                                    
                                    saveAs(blob, "errorlog.json");
                                });
                            }
                        },
                        templateUrl: './app/create/views/ValidateImportView.html'
                    }   
                }
            })
            .state('jobs.import', {
                url: '/import'
            })
            .state('jobs.import.credentials', {
                url: '/credentials',
                views: {
                    "summary@": {
                        templateUrl: './app/navigation/message/MessageView.html'
                    },
                    "main@": {
                        templateUrl: './app/jobs/import/credentials/CredentialsView.html'
                    }
                }
            })
            .state('jobs.import.file', {
                url: '/file',
                views: {
                    "summary@": {
                        templateUrl: './app/navigation/message/MessageView.html'
                    },
                    "main@": {
                        templateUrl: './app/jobs/import/file/FileView.html'
                    }
                }
            })
            .state('jobs.import.processing', {
                url: '/processing',
                views: {
                    "summary@": {
                        templateUrl: './app/navigation/message/MessageView.html'
                    },
                    "main@": {
                        templateUrl: './app/jobs/import/processing/ProcessingView.html'
                    }
                }
            });
    }]);