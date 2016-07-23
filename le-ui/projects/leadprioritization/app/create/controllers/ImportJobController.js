angular.module('lp.create.import.job', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'lp.create.import',
    'pd.jobs'
])
.controller('ImportJobController', function(
    $scope, $state, $stateParams, ResourceUtility, JobsService, 
    JobsStore, ImportStore
) {
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


    $scope.isPMMLJob = $state.includes('home.models.pmml.job');
    $scope.compress_percent = 4;

    var value = 0,
        increment = 2,
        ceiling = 90;

    function PerformCalc(){
        if (value <= ceiling){
            value += increment
        }
        $scope.compress_percent = value;
    }
    setInterval(PerformCalc, 10000);




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
                if ($scope.jobStatus == response.resultObj.jobStatus) {
                    return;
                }

                $scope.jobStatus = response.resultObj.jobStatus;
                $scope.jobId = response.resultObj.jobId;
                
                if ($scope.jobStatus == "Completed" || $scope.jobStatus == "Failed" || $scope.jobStatus == "Cancelled") {
                    cancelPeriodJobStatusQuery();
                }

                updateStatesBasedOnJobStatus(response.resultObj);

                JobsStore.getJobs();
            } else {

            }
        });
    }
});
