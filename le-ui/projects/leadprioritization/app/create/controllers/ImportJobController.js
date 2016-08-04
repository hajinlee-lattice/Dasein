angular.module('lp.create.import.job', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'lp.create.import',
    'pd.jobs'
])
.controller('ImportJobController', function(
    $scope, $state, $stateParams, $interval, ResourceUtility, 
    JobsStore, JobsService, ImportStore, ServiceErrorUtility
) {
    $scope.applicationId = $stateParams.applicationId;
    var REFRESH_JOB_INTERVAL_ID;
    var REFRESH_PERFORM_CALC_ID; 
    var TIME_BETWEEN_JOB_REFRESH = 500;

    getJobStatusFromAppId();

    REFRESH_JOB_INTERVAL_ID = $interval(getJobStatusFromAppId, TIME_BETWEEN_JOB_REFRESH);

    $scope.jobStepsRunningStates = {
        load_data: false, generate_insights: false, create_global_target_market: false
    };

    $scope.jobStepsCompletedStates = {
        load_data: false, generate_insights: false, create_global_target_market: false
    };

    $scope.isPMMLJob = $state.includes('home.models.pmml.job');
    $scope.compress_percent = 0;

    $scope.create_new_sref = ($scope.isPMMLJob ? 'home.models.pmml' : 'home.models.import');

    var up = true,
        value = 0,
        increment = 4,
        ceiling = 10;

    $scope.$on("$destroy", function() {
        $interval.cancel(REFRESH_JOB_INTERVAL_ID);
        $interval.cancel(REFRESH_PERFORM_CALC_ID);
    });

    function performCalc() {
        JobsStore.getJobs(true).then(function(jobs) {
            if (jobs.length > 0) {
                for (var i=0; i<jobs.length; i++) {
                    if (jobs.applicationId == $scope.applicationId) {
                        var resultObj = jobs[i];
                    }
                }

                if (!resultObj) {
                    return;
                }
                
                ServiceErrorUtility.process({ data: resultObj });

                var jobStatus = job.resultObj;

                if (jobStatus.stepRunning == 'load_data'){
                    ceiling = 30;
                } else if (jobStatus.stepRunning == 'generate_insights'){
                    ceiling = 60;
                } else if (jobStatus.stepRunning == 'create_global_target_market'){
                    ceiling = 90;
                }

                if (up == true && value <= ceiling){
                    value += increment
                }
                if (value == ceiling){
                    up = false;
                }
                $scope.compress_percent = value;
            };
        });
    };

    REFRESH_PERFORM_CALC_ID = $interval(performCalc, TIME_BETWEEN_JOB_REFRESH);

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
        $interval.cancel(REFRESH_JOB_INTERVAL_ID);
        REFRESH_JOB_INTERVAL_ID = null;
    }

    function getJobStatusFromAppId() {
        JobsStore.getJobs(true).then(function(jobs) {
            if (jobs.length > 0) {
                for (var i=0; i<jobs.length; i++) {
                    if (jobs[i].applicationId == $scope.applicationId) {
                        var resultObj = jobs[i];
                    }
                }

                if (!resultObj) {
                    return;
                }

                if ($scope.jobStatus == resultObj.jobStatus) {
                    return;
                }

                $scope.jobStatus = resultObj.jobStatus;
                $scope.jobId = resultObj.id;
                
                if ($scope.jobStatus == "Completed" || $scope.jobStatus == "Failed" || $scope.jobStatus == "Cancelled") {
                    ServiceErrorUtility.process({ data: resultObj });
                    cancelPeriodJobStatusQuery();
                }

                updateStatesBasedOnJobStatus(resultObj);
                performCalc(resultObj);

                JobsStore.getJobs();
            } else {

            }
        });
    }
});
