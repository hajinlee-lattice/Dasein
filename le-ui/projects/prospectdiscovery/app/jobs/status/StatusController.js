angular.module('controllers.jobs.status', [
    'services.jobs'
])

.directive('jobStatusRow', function() {
    return {
        restrict: 'E',
        templateUrl: 'app/jobs/status/JobStatusRow.html',
        scope: {
            job: '=',
        },
        controller: ['$scope', 'JobsService', function ($scope, JobsService) {
            $scope.showStatusLink = false;
            $scope.jobRowExpanded = false;
            $scope.jobCompleted = false;
            $scope.statusLinkText;

            /** State variables to track which steps have been completed and which steps is running */
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
            }
            
            $scope.expandJobStatus = function() {
                $scope.jobRowExpanded = true;
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
            
            $scope.unexpandJobStatus = function() {
                $scope.jobRowExpanded = false;
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
                    $scope.jobStepsCompletedStates[jobStatus.stepsCompleted[i].toLowerCase()] = true;
                    $scope.jobStepsRunningStates[jobStatus.stepsCompleted[i].toLowerCase()] = false;
                }
                
                if (jobStatus.jobStatus == "Running") {
                    $scope.jobStepsRunningStates[jobStatus.stepRunning.toLowerCase()] = true;
                } else if (jobStatus.jobStatus == "Complete") {
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
})