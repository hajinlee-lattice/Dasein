angular
    .module('pd.jobs.status', [
    ])
    .directive('jobStatusRow', function() {
        return {
            restrict: 'EA',
            templateUrl: 'app/jobs/status/JobStatusRow.html',
            scope: {
                job: '=',
                statuses: '='
            },
            controller: ['$scope', 'JobsService', function ($scope, JobsService) {
                $scope.showStatusLink = false;
                $scope.jobRowExpanded = $scope.statuses[$scope.job.id];
                $scope.jobCompleted = false;
                $scope.statusLinkText;
                $scope.statusLinkState;
                $scope.jobId = $scope.job.id;

                $scope.jobStepsRunningStates = { load_data: false, match_data: false,
                        generate_insights: false, create_model: false, create_global_target_market: false };
                $scope.jobStepsCompletedStates = { load_data: false, match_data: false,
                        generate_insights: false, create_model: false, create_global_target_market: false };

                if ($scope.job.status == "Running") {
                    $scope.showStatusLink = true;
                    $scope.statusLinkText = "Cancel Job";
                } else if ($scope.job.status == "Completed") {
                    $scope.showStatusLink = true;
                    $scope.jobCompleted = true;
                    $scope.statusLinkText = "View Report";
                    $scope.statusLinkState = "jobs.import.ready";
                }
                
                $scope.expandJobStatus = function() {
                    $scope.jobRowExpanded = true;
                    $scope.statuses[$scope.jobId] = true;
                    JobsService.getJobStatus($scope.job.id).then(function(jobStatus) {
                        if (jobStatus.success) {
                            if (jobStatus.resultObj.jobStatus == "Running") {
                                periodicQueryJobStatus($scope.job.id);
                            } else {
                                updateStatesBasedOnJobStatus(jobStatus.resultObj);
                            }
                        }
                    });
                };
                
                // need this to get the status of job that is expanded after refresh
                if ($scope.jobRowExpanded) {
                    $scope.expandJobStatus();
                }
                
                $scope.unexpandJobStatus = function() {
                    $scope.jobRowExpanded = false;
                    $scope.statuses[$scope.jobId] = false;
                    cancelPeriodJobStatusQuery();
                };
                
                var periodicQueryId;
                var TIME_INTERVAL_BETWEEN_JOB_STATUS_CHECKS = 5 * 1000;

                function cancelPeriodJobStatusQuery() {
                    clearInterval(periodicQueryId);
                    periodicQueryId = null;
                }

                function updateStatesBasedOnJobStatus(jobStatus) {
                    if (jobStatus.jobStatus == "Running") {
                        $scope.jobStepsRunningStates[jobStatus.stepRunning.toLowerCase()] = true;
                    }

                    for (var i = 0; i < jobStatus.stepsCompleted.length; i++) {
                        $scope.jobStepsCompletedStates[jobStatus.stepsCompleted[i].toLowerCase()] = true;
                        $scope.jobStepsRunningStates[jobStatus.stepsCompleted[i].toLowerCase()] = false;
                    }
                    
                    if (jobStatus.jobStatus == "Complete") {
                        $scope.jobCompleted = true;
                        $scope.showStatusLink = true;
                        $scope.statusLinkText = "View Report";
                    }
                }
                
                function periodicQueryJobStatus(jobId) {
                    periodicQueryId = setInterval(queryJobStatusAndSetStatesVariables(jobId), TIME_INTERVAL_BETWEEN_JOB_STATUS_CHECKS);
                }
                
                function queryJobStatusAndSetStatesVariables(jobId) {
                    JobsService.getJobStatus(jobId).then(function(response) {
                        if (response.success) {
                            if (response.resultObj.jobStatus == "Complete") {
                                cancelPeriodJobStatusQuery();
                            }
                            updateStatesBasedOnJobStatus(response.resultObj);
                        }
                    });
                }
            }]
        };
    }
);