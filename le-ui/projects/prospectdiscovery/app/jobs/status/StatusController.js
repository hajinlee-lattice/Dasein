angular
    .module('pd.jobs.status', [
    ])
    .directive('jobStatusRow', function() {
        return {
            restrict: 'EA',
            templateUrl: 'app/jobs/status/JobStatusRow.html',
            scope: {
                job: '=',
                statuses: '=',
                expanded: '='
            },
            controller: ['$scope', '$state', 'JobsService', function ($scope, $state, JobsService) {
                $scope.showStatusLink = false;
                $scope.jobRowExpanded = $scope.expanded[$scope.job.id] ? true : false;
                $scope.jobCompleted = false;
                $scope.statusLinkText;
                $scope.jobId = $scope.job.id;
                
                $scope.statusLinkClicked = function(jobId) {
                    if ($scope.job.status == "Completed") {
                        $state.go("jobs.import.ready", { 'jobId': jobId });
                    } else if ($scope.job.status == "Running") {
                        JobsService.cancelJob(jobId);
                    }
                }
                
                if (! $scope.jobRowExpanded || $scope.statuses[$scope.jobId] == null) {
                    $scope.jobStepsRunningStates = { load_data: false, match_data: false,
                            generate_insights: false, create_model: false, create_global_target_market: false };
                    $scope.jobStepsCompletedStates = { load_data: false, match_data: false,
                            generate_insights: false, create_model: false, create_global_target_market: false };
                } else {
                    $scope.jobStepsRunningStates = $scope.statuses[$scope.jobId].running;
                    $scope.jobStepsCompletedStates = $scope.statuses[$scope.jobId].completed;
                }

                if ($scope.job.status == "Running") {
                    $scope.showStatusLink = true;
                    $scope.statusLinkText = "Cancel Job";
                } else if ($scope.job.status == "Completed") {
                    $scope.showStatusLink = true;
                    $scope.jobCompleted = true;
                    $scope.statusLinkText = "View Report";
                }
                
                $scope.expandJobStatus = function() {
                    $scope.jobRowExpanded = true;
                    $scope.expanded[$scope.jobId] = true;

                    if (! isCompleted()) {
                        JobsService.getJobStatus($scope.job.id).then(function(jobStatus) {
                            if (jobStatus.success) {
                                if (jobStatus.resultObj.jobStatus == "Running") {
                                    queryJobStatusAndSetStatesVariables($scope.job.id);
                                    periodicQueryJobStatus($scope.job.id);
                                } else {
                                    updateStatesBasedOnJobStatus(jobStatus.resultObj);
                                }
                            }
                        });
                    }
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
                    $scope.expanded[$scope.jobId] = false;
                    cancelPeriodJobStatusQuery();
                };
                
                var periodicQueryId;
                var TIME_INTERVAL_BETWEEN_JOB_STATUS_CHECKS = 5 * 1000;

                function cancelPeriodJobStatusQuery() {
                    clearInterval(periodicQueryId);
                    periodicQueryId = null;
                }

                function updateStatesBasedOnJobStatus(jobStatus) {
                    for (var i = 0; i < jobStatus.stepsCompleted.length; i++) {
                        $scope.jobStepsCompletedStates[jobStatus.stepsCompleted[i]] = true;
                    }
                    
                    if (jobStatus.jobStatus == "Running") {
                        $scope.jobStepsRunningStates[jobStatus.stepRunning] = true;
                        $scope.jobStepsCompletedStates[jobStatus.stepRunning] = false;
                    }
                    saveJobStatusInParentScope();
                    
                    if (jobStatus.jobStatus == "Complete") {
                        $scope.jobCompleted = true;
                        $scope.showStatusLink = true;
                        $scope.statusLinkText = "View Report";
                    }
                }
                
                function saveJobStatusInParentScope() {
                    if (! $scope.statuses[$scope.jobId]) {
                        $scope.statuses[$scope.jobId] = {};
                    }
                    $scope.statuses[$scope.jobId]["running"] = $scope.jobStepsRunningStates;
                    $scope.statuses[$scope.jobId]["completed"] = $scope.jobStepsCompletedStates;
                }
                
                function periodicQueryJobStatus(jobId) {
                    periodicQueryId = setInterval(function() {
                            queryJobStatusAndSetStatesVariables(jobId);
                        }, TIME_INTERVAL_BETWEEN_JOB_STATUS_CHECKS);
                }
                
                function queryJobStatusAndSetStatesVariables(jobId) {
                    console.log("querying job status for individual job");
                    JobsService.getJobStatus(jobId).then(function(response) {
                        if (response.success) {
                            if (response.resultObj.jobStatus == "Complete") {
                                cancelPeriodJobStatusQuery();
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