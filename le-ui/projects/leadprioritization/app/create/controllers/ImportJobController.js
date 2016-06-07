angular.module('mainApp.create.controller.ImportJobController', [
    'mainApp.create.csvImport',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.NavUtility',
    'pd.jobs'
])
.controller('ImportJobController', function($scope, $state, $stateParams, ResourceUtility, JobsService, csvImportStore) {
    $scope.applicationId = $stateParams.applicationId;
    var REFRESH_JOB_INTERVAL_ID;
    var TIME_BETWEEN_JOB_REFRESH = 10 * 1000;

    getJobStatusFromAppId();
    REFRESH_JOB_INTERVAL_ID = setInterval(getJobStatusFromAppId, TIME_BETWEEN_JOB_REFRESH);
    $scope.jobStepsRunningStates = {
        load_data: false, generate_insights: false, create_global_target_market: false
    };
    $scope.jobStepsCompletedStates = {
        load_data: false, generate_insights: false, create_global_target_market: false
    };

    function updateStatesBasedOnJobStatus(jobStatus) {
        $scope.startTimestamp = jobStatus.startTimestamp;
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
            $scope.stepFailed = stepFailed;

            if ($scope.stepsCompletedTimes[stepFailed]) {
                delete $scope.stepsCompletedTimes[stepFailed];
            }
        }

        if (jobStatus.jobStatus == "Completed") {
            $scope.jobRunning = false;
            $scope.jobCompleted = true;
        } else if (jobStatus.jobStatus == "Failed" || jobStatus.jobStatus == "Cancelled") {
            $scope.jobRunning = false;
            for (var jobState in $scope.jobStepsRunningStates) {
                $scope.jobStepsRunningStates[jobState] = false;
            }
        }
    }

    function cancelPeriodJobStatusQuery() {
        clearInterval(REFRESH_JOB_INTERVAL_ID);
        REFRESH_JOB_INTERVAL_ID = null;
    }

    function getJobStatusFromAppId() {
        JobsService.getJobStatusFromApplicationId($scope.applicationId).then(function(response) {
            if (response.success) {
                $scope.jobStatus = response.resultObj.jobStatus;
                if ($scope.jobStatus == "Completed" || $scope.jobStatus == "Failed" || $scope.jobStatus == "Cancelled") {
                    cancelPeriodJobStatusQuery();
                }

                updateStatesBasedOnJobStatus(response.resultObj);
            } else {

            }
        });
    }
});
