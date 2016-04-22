angular
    .module('pd.jobs.status', [
        'mainApp.core.utilities.BrowserStorageUtility',
    ])
    .directive('jobStatusRow', function(BrowserStorageUtility) {
        return {
            restrict: 'EA',
            templateUrl: 'app/jobs/status/JobStatusRow.html',
            scope: {
                job: '=',
                state: '=',
                statuses: '=',
                expanded: '=',
                cancelling: '='
            },
            controller: ['$scope', '$rootScope', '$state', 'JobsService', function ($scope, $rootScope, $state, JobsService) {
                var job = $scope.job;

                $scope.jobType = job.jobType ? job.jobType : 'placeholder';
                $scope.jobRunning = false;
                $scope.jobCompleted = false;
                $scope.jobRowExpanded = $scope.expanded[job.id] ? true : false;
                $scope.cancelClicked = $scope.cancelling[job.id] ? true : false;

                switch ($scope.jobType.toLowerCase()) {
                    case "scoreworkflow": $scope.job.displayName = "Batch Scoring"; break;
                    case "placeholder": $scope.job.displayName = "Pending..."; break;
                    case "importmatchandscoreworkflow": $scope.job.displayName = "Bulk Scoring"; break;
                    default: $scope.job.displayName = "Create Model";
                }

                $scope.jobFailed = $scope.job.status == 'Failed';
                $scope.stepsCompletedTimes;

                var periodicQueryId;
                var TIME_INTERVAL_BETWEEN_JOB_STATUS_CHECKS = 8 * 1000;

                $scope.cancelJob = function(jobId) {
                    $scope.cancelClicked = true;
                    $scope.cancelling[job.id] = true;
                    JobsService.cancelJob(jobId);
                };

                if (! $scope.jobRowExpanded || $scope.statuses[job.id] == null) {
                    $scope.jobStepsRunningStates = { 
                        load_data: false, match_data: false, generate_insights: false, 
                        create_global_model: false, create_global_target_market: false 
                    };
                    $scope.jobStepsCompletedStates = { 
                        load_data: false, match_data: false, generate_insights: false, 
                        create_global_model: false, create_global_target_market: false 
                    };
                } else {
                    $scope.jobStepsRunningStates = $scope.statuses[job.id].running;
                    $scope.jobStepsCompletedStates = $scope.statuses[job.id].completed;
                    $scope.stepsCompletedTimes = $scope.statuses[job.id].completedTimes;
                }
                if ($scope.job.status == "Running") {
                    $scope.jobRunning = true;
                    periodicQueryJobStatus($scope.job.id);
                } else if ($scope.job.status == "Completed") {
                    $scope.jobCompleted = true;
                }
                
                $scope.expandJobStatus = function() {
                    $scope.jobRowExpanded = true;
                    $scope.expanded[job.id] = true;

                    if (! isCompleted() && $scope.job.id != null) {
                        JobsService.getJobStatus($scope.job.id).then(function(jobStatus) {
                            if (jobStatus.success) {
                                updateStatesBasedOnJobStatus(jobStatus.resultObj);
                            }
                        });
                    }
                };

                $scope.clickGetScoringResults = function($event) {
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
                    }, function(reason) {
                        alert('Failed: ' + reason);
                    });
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

                function updateStatesBasedOnJobStatus(jobStatus) {
                    $scope.job.status = jobStatus.jobStatus;
                    for (var i = 0; i < jobStatus.stepsCompleted.length; i++) {
                        $scope.jobStepsCompletedStates[jobStatus.stepsCompleted[i]] = true;
                        $scope.jobStepsRunningStates[jobStatus.stepsCompleted[i]] = false;
                    }
                    
                    if (jobStatus.jobStatus == "Running") {
                        $scope.jobStepsRunningStates[jobStatus.stepRunning] = true;
                        $scope.jobStepsCompletedStates[jobStatus.stepRunning] = false;
                    }

                    $scope.stepsCompletedTimes = jobStatus.completedTimes;

                    var stepFailed = jobStatus.stepFailed;
                    if (stepFailed) {
                        $scope.jobStepsRunningStates[stepFailed] = false;
                        $scope.jobStepsCompletedStates[stepFailed] = false;

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
                    }
                }

                function saveJobStatusInParentScope() {
                    if (! $scope.statuses[job.id]) {
                        $scope.statuses[job.id] = {};
                    }
                    $scope.statuses[job.id]["running"] = $scope.jobStepsRunningStates;
                    $scope.statuses[job.id]["completed"] = $scope.jobStepsCompletedStates;
                    $scope.statuses[job.id]["completedTimes"] = $scope.stepsCompletedTimes;
                }

                function periodicQueryJobStatus(jobId) {
                    periodicQueryId = setInterval(function() {
                            queryJobStatusAndSetStatesVariables(jobId);
                        }, TIME_INTERVAL_BETWEEN_JOB_STATUS_CHECKS);
                }

                function queryJobStatusAndSetStatesVariables(jobId) {
                    JobsService.getJobStatus(jobId).then(function(response) {
                        if (response.success) {
                            var jobStatus = response.resultObj.jobStatus;
                            if (jobStatus == "Completed" || jobStatus == "Failed" || jobStatus == "Cancelled") {
                                cancelPeriodJobStatusQuery();
                            }
                            if (jobStatus == "Completed") {
                                $rootScope.$broadcast("JobCompleted");
                            }
                            updateStatesBasedOnJobStatus(response.resultObj);
                        }
                    });
                }

                $scope.$on("$destroy", function() {
                    cancelPeriodJobStatusQuery();
                });
            }]
        };
    }
);
