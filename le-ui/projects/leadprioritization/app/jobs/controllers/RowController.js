angular
.module('lp.jobs.status', [
    'lp.jobs.modals.cancelmodal',
    'lp.create.import.report',
    'mainApp.models.services.ModelService'
])
.directive('jobStatusRow', function(ModelStore) {
    return {
        restrict: 'EA',
        templateUrl: 'app/jobs/views/RowView.html',
        scope: {
            job: '=',
            state: '=',
            statuses: '=',
            expanded: '=',
            cancelling: '=',
            admin: '=',
            auth: '='
        },
        controller: function ($http, $scope, $rootScope, $state, $location, JobsStore, JobsService, CancelJobModal, BrowserStorageUtility) {
            var job = $scope.job;
            $scope.showProgress = false;
            $scope.jobType = job.jobType ? job.jobType : 'placeholder';
            $scope.jobRunning = false;
            $scope.jobCompleted = false;
            $scope.jobRowExpanded = $scope.expanded[job.id] ? true : false;
            job.cancelling = $scope.cancelling[job.id] ? true : false;
            $scope.cancelClicked = $scope.cancelling[job.id] ? true : false;

            var clientSession = BrowserStorageUtility.getClientSession();
            $scope.TenantId = clientSession.Tenant.Identifier;

            switch ($scope.jobType.toLowerCase()) {
                case "scoreworkflow": $scope.job.displayName = "Bulk Scoring"; break;
                case "placeholder": $scope.job.displayName = "Pending..."; break;
                case "importmatchandscoreworkflow": $scope.job.displayName = "Bulk Scoring"; break;
                case "importandrtsbulkscoreworkflow": $scope.job.displayName = "Bulk Scoring"; break;
                case "rtsbulkscoreworkflow": $scope.job.displayName = "Bulk Scoring"; break;
                case "importmatchandmodelworkflow": $scope.job.displayName = "Create Model (Training Set)"; break;
                case "modelandemailworkflow": $scope.job.displayName = "Create Model (Remodel)"; break;
                case "pmmlmodelworkflow": $scope.job.displayName = "Create Model (PMML File)"; break;
                default: $scope.job.displayName = "Create Model";
            }

            if ($scope.job.displayName == "Bulk Scoring") {
                $scope.isScoringJob = true;
            }
            $scope.jobFailed = $scope.job.status == 'Failed';
            $scope.stepsCompletedTimes;

            var periodicQueryId;
            var TIME_INTERVAL_BETWEEN_JOB_STATUS_CHECKS = 8 * 1000;

            $scope.cancelJobClick = function ($event) {
                if ($event != null) {
                    $event.stopPropagation();
                }
                CancelJobModal.show(job.id);
            };

            $scope.$on("updateAsCancelledJob", function(event, args){
                JobsService.cancelJob(args);
                $scope.cancelClicked = true;
                if(job.id === args) {
                    job.cancelling = true;
                }
                $scope.cancelling[args] = true;
            });

            $scope.downloadErrorLogClick = function($event){

                JobsService.downloadErrorLog();

            };

            if (! $scope.jobRowExpanded || $scope.statuses[job.id] == null) {
                $scope.jobStepsRunningStates = { 
                    load_data: false, match_data: false, generate_insights: false, 
                    create_global_model: false, create_global_target_market: false,
                    score_training_set: false
                };
                $scope.jobStepsCompletedStates = { 
                    load_data: false, match_data: false, generate_insights: false, 
                    create_global_model: false, create_global_target_market: false,
                    score_training_set: false
                };
            } else {
                $scope.jobStepsRunningStates = $scope.statuses[job.id].running;
                $scope.jobStepsCompletedStates = $scope.statuses[job.id].completed;
                $scope.stepsCompletedTimes = $scope.statuses[job.id].completedTimes;
                $scope.stepFailed = $scope.statuses[job.id].stepFailed;
            }
            if ($scope.job.status == "Running") {
                $scope.jobRunning = true;
                periodicQueryJobStatus($scope.job.id);
            } else if ($scope.job.status == "Completed") {
                $scope.jobCompleted = true;
            } else if ($scope.stepFailed) {
                $scope.jobStepsRunningStates[$scope.stepFailed] = false;
                $scope.jobStepsCompletedStates[$scope.stepFailed] = false;
                if ($scope.stepsCompletedTimes[$scope.stepFailed]) {
                    delete $scope.stepsCompletedTimes[$scope.stepFailed];
                }
            }
            
            $scope.expandJobStatus = function() {
                $scope.jobRowExpanded = true;
                $scope.expanded[job.id] = true;

                JobsStore.getJob($scope.job.id).then(function(result) {
                    updateStatesBasedOnJobStatus(result);
                });
            };

            // Use this in JobStatusRow.html
            // <a href="javascript:void(0)" data-ng-click="rescoreFailedJob({jobId: job.id})" ng-show="job.status == 'Failed'"><i class="fa fa-refresh"></i>Restart</a>
            $scope.rescoreFailedJob = function() {
                $http({
                    method: 'POST',
                    url: '/pls/jobs/' + job.id +'/restart'
                }).then(
                    function onSuccess(response) {
                        var jobId = $scope.job.id;
                        JobsStore.getJob(jobId);
                    }, function onError(response) {
                        console.log("error");
                    }
                );
            };

            /* $scope.downloadSourceClicked = function() {
                var downloadUrl = '/files/datafiles/sourcefilecsv/';
                downloadUrl += job.applicationId;
                downloadUrl += '?fileName=' + job.source;
                downloadUrl += '&Authorization=' + $scope.auth;
                $http({
                    method: 'GET',
                    url: downloadUrl,
                    headers: {
                        'ErrorDisplayMethod': 'modal|home.models'
                    }
                }).then(
                    function onSuccess(response) {
                        if (response.status == 200) {
                            var blob = new Blob([ response.data ], { type: "application/csv" });
                            saveAs(blob, job.source);
                        }
                    }
                );
            } 
            */
            /*
            $scope.clickDownloadErrorReport = function($event) {

                console.log(reports);

                reports.forEach(function(item) {
                    if (item.purpose == "IMPORT_DATA_SUMMARY") {
                        JobReport = item;
                    }
                });
  
                if (JobReport) {
                    JobReport.name = JobReport.name.substr(0, JobReport.name.indexOf('.csv') + 4);
                    
                    $scope.report = JobReport;
                    $scope.data.total_records = data.imported_records + data.ignored_records;
                    $scope.errorlog = '/pls/fileuploads/' + JobReport.name + '/import/errors';

                    $scope.showProgress = true;
                }

                JobsService.getErrorLog(JobReport, $scope.job.jobType).then(function(result) {
                    var blob = new Blob([ result ], { type: "application/csv" }),
                        date = new Date(),
                        year = date.getFullYear(),
                        month = (1 + date.getMonth()).toString(),
                        month = month.length > 1 ? month : '0' + month,
                        day = date.getDate().toString(),
                        day = day.length > 1 ? day : '0' + day,
                        filename = 'import_errors.' + year + month + day + '.csv';
                    
                    saveAs(blob, filename);
                    $scope.showProgress = false;

                }, function(reason) {});
            };
            */
            $scope.clickGetScoringResults = function($event) {

                $scope.showProgress = true;

                JobsService.getScoringResults($scope.job).then(function(result) {
                    var blob = new Blob([ result ], { type: "application/csv" }),
                        date = new Date(),
                        year = date.getFullYear(),
                        month = (1 + date.getMonth()).toString(),
                        month = month.length > 1 ? month : '0' + month,
                        day = date.getDate().toString(),
                        day = day.length > 1 ? day : '0' + day,
                        filename = 'score.' + $scope.job.id + '.' + year + month + day + '.csv';
                
                    saveAs(blob, filename);
                    $scope.showProgress = false;

                }, function(reason) {});
            };

            function isCompleted() {
                for (var step in $scope.jobStepsCompletedStates) {
                    if (! $scope.jobStepsCompletedStates[step]) {
                        return false;
                    }
                }
                return true;
            }
            // need this to get the status of job that is expanded after refresh
            if ($scope.jobRowExpanded) {
                $scope.expandJobStatus();
            }
            
            $scope.unexpandJobStatus = function() {
                $scope.jobRowExpanded = false;
                $scope.expanded[job.id] = false;
            };

            function cancelPeriodJobStatusQuery() {
                clearInterval(periodicQueryId);
                periodicQueryId = null;
            }

            $scope.showFileName = false;
            var JobReport = null;

            function updateStatesBasedOnJobStatus(jobStatus) {
                $scope.job.status = jobStatus.jobStatus;
                $scope.job.user = jobStatus.user;
                $scope.job.sourceFileExists = jobStatus.sourceFileExists;
                $scope.job.source = jobStatus.source;
                $scope.job.applicationId = jobStatus.applicationId;
                $scope.job.applicationLogUrl = jobStatus.applicationLogUrl;
                $scope.isPMML = (['PmmlModel'].indexOf(jobStatus.modelType) > -1);
                for (var i = 0; i < jobStatus.stepsCompleted.length; i++) {
                    $scope.jobStepsCompletedStates[jobStatus.stepsCompleted[i]] = true;
                    $scope.jobStepsRunningStates[jobStatus.stepsCompleted[i]] = false;
                }
                
                if (jobStatus.jobStatus == "Running") {
                    $scope.jobStepsRunningStates[jobStatus.stepRunning] = true;
                    $scope.jobStepsCompletedStates[jobStatus.stepRunning] = false;
                }
                
                if ($scope.jobType.toLowerCase() == "importmatchandscoreworkflow" || $scope.jobType.toLowerCase() == "importandrtsbulkscoreworkflow"
                        || $scope.jobType.toLowerCase() == "importmatchandmodelworkflow") {
                    if (jobStatus.applicationId != null && jobStatus.source != null) {
                        $scope.showFileName = true;
                    }
                }
                $scope.stepsCompletedTimes = jobStatus.completedTimes;

                var stepFailed = jobStatus.stepFailed;
                if (stepFailed) {
                    $scope.jobStepsRunningStates[stepFailed] = false;
                    $scope.jobStepsCompletedStates[stepFailed] = false;
                    $scope.stepFailed = stepFailed;

                    if ($scope.stepsCompletedTimes[stepFailed]) {
                        delete $scope.stepsCompletedTimes[stepFailed];
                    }
                }

                saveJobStatusInParentScope();

                if (jobStatus.jobStatus == "Completed") {
                    $scope.jobRunning = false;
                    $scope.jobCompleted = true;
                } else if (jobStatus.jobStatus == "Failed" || jobStatus.jobStatus == "Cancelled") {
                    $scope.jobRunning = false;
                    for (var jobState in $scope.jobStepsRunningStates) {
                        $scope.jobStepsRunningStates[jobState] = false;
                    }
                }

                var reports = jobStatus.reports;
                if (reports != null) {
                    reports.forEach(function(item) {
                        if (item.purpose == "IMPORT_DATA_SUMMARY") {
                            $scope.data = data = JSON.parse(item.json.Payload);
                            JobReport = item;

                            JobReport.name = JobReport.name.substr(0, JobReport.name.indexOf('.csv') + 4);

                            $scope.report = JobReport;
                            $scope.data.total_records = data.imported_records + data.ignored_records;
                            $scope.errorlog = '/files/fileuploads/' + JobReport.name + '/import/errors';
                        }
                    });
                }
            }

            function saveJobStatusInParentScope() {
                if (! $scope.statuses[job.id]) {
                    $scope.statuses[job.id] = {};
                }
                $scope.statuses[job.id]["running"] = $scope.jobStepsRunningStates;
                $scope.statuses[job.id]["completed"] = $scope.jobStepsCompletedStates;
                $scope.statuses[job.id]["completedTimes"] = $scope.stepsCompletedTimes;
                $scope.statuses[job.id]["stepFailed"] = $scope.stepFailed;
            }

            function periodicQueryJobStatus(jobId) {
                periodicQueryId = setInterval(function() {
                        queryJobStatusAndSetStatesVariables(jobId);
                    }, TIME_INTERVAL_BETWEEN_JOB_STATUS_CHECKS);
            }

            function queryJobStatusAndSetStatesVariables(jobId) {

                JobsStore.getJob(jobId).then(function(response) {
                    var jobStatus = response.jobStatus;
                    if (jobStatus == "Completed" || jobStatus == "Failed" || jobStatus == "Cancelled") {
                        cancelPeriodJobStatusQuery();
                    }
                    if (jobStatus == "Completed") {
                        $rootScope.$broadcast("JobCompleted");
                    }
                    updateStatesBasedOnJobStatus(response);
                });
            }

            $scope.$on("$destroy", function() {
                cancelPeriodJobStatusQuery();
            });
        }
    };
});
