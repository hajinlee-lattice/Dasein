angular.module('lp.create.import.job', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'lp.create.import',
    'lp.jobs'
])
.controller('ImportJobController', function(
    $scope, $state, $stateParams, $interval, ResourceUtility, 
    JobsStore, JobsService, ImportStore, ServiceErrorUtility,
    CancelJobModal
) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.applicationId = $stateParams.applicationId;
    var REFRESH_JOB_INTERVAL_ID;
    var REFRESH_PERFORM_CALC_ID; 
    var TIME_BETWEEN_JOB_REFRESH = 10 * 1000;

    getJobStatusFromAppIdAndPerformCalc();

    REFRESH_JOB_INTERVAL_ID = $interval(getJobStatusFromAppIdAndPerformCalc, TIME_BETWEEN_JOB_REFRESH);

    $scope.jobStepsRunningStates = {
        load_data: false, 
        generate_insights: false, 
        create_global_target_market: false,
        score_training_set: false
    };

    $scope.jobStepsCompletedStates = {
        load_data: false, 
        generate_insights: false, 
        create_global_target_market: false,
        score_training_set: false
    };

    $scope.isPMMLJob = $state.includes('home.models.pmml.job');
    $scope.isPMMLCompleted = false;
    $scope.compress_percent = 0;

    $scope.create_new_sref = ($scope.isPMMLJob ? 'home.models.pmml' : 'home.models.import');

    var up = true,
        value = 0,
        increment = 4,
        ceiling = 10,
        initialized = false;

    $scope.$on("$destroy", function() {
        $interval.cancel(REFRESH_JOB_INTERVAL_ID);
        $interval.cancel(REFRESH_PERFORM_CALC_ID);
    });

    function performCalc(job) {
        ServiceErrorUtility.process({ data: job });

        if (job.jobStatus == 'Pending') {
            ceiling = 10;
        } else if (job.stepRunning == 'load_data'){
            ceiling = 35;
        } else if (job.stepRunning == 'generate_insights'){
            ceiling = 60;
        } else if (job.stepRunning == 'create_global_target_market'){
            ceiling = 80;
        } else if (job.stepRunning == 'score_training_set') {
            ceiling = 90;
        } else if (job.jobStatus == 'Completed') {
            ceiling = 100;
        }

        $scope.isPMMLCompleted = ($scope.isPMMLJob && ($scope.jobStepsCompletedStates && $scope.jobStepsCompletedStates.create_global_target_market));

        if (initialized) {
            value = ceiling;
        } else if (up == true && value <= ceiling){
            value += increment
        }

        if (value == ceiling){
            up = false;
        }

        $scope.compress_percent = value;
        initialized = true;
    };

    function updateStatesBasedOnJobStatus(job) {
        $scope.startTimestamp = job.startTimestamp;
        for (var i = 0; i < job.stepsCompleted.length; i++) {
            $scope.jobStepsCompletedStates[job.stepsCompleted[i]] = true;
            $scope.jobStepsRunningStates[job.stepsCompleted[i]] = false;
        }

        if (job.jobStatus == "Running") {
            $scope.jobStepsRunningStates[job.stepRunning] = true;
            $scope.jobStepsCompletedStates[job.stepRunning] = false;
        }

        $scope.stepsCompletedTimes = job.completedTimes;

        var stepFailed = job.stepFailed;
        if (stepFailed) {
            $scope.jobStepsRunningStates[stepFailed] = false;
            $scope.jobStepsCompletedStates[stepFailed] = false;
            $scope.stepFailed = stepFailed;

            if ($scope.stepsCompletedTimes[stepFailed]) {
                delete $scope.stepsCompletedTimes[stepFailed];
            }
        }

        if (job.jobStatus == "Completed") {
            $scope.jobRunning = false;
            $scope.jobCompleted = true;
            $scope.compress_percent = 100;
            $scope.jobStatus = job.jobStatus;
        } else if (job.jobStatus == "Failed" || job.jobStatus == "Cancelled") {
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

    function getJobStatusFromAppIdAndPerformCalc() {
        JobsService.getJobStatusFromApplicationId($scope.applicationId).then(function(response) {
            if (response.success) {
                var resultObj = response.resultObj;

                $scope.jobStatus = resultObj.jobStatus;
                $scope.jobId = resultObj.id;

                if ($scope.jobStatus == "Completed" || $scope.jobStatus == "Failed" || $scope.jobStatus == "Cancelled") {
                    ServiceErrorUtility.process({ data: resultObj });
                    cancelPeriodJobStatusQuery();
                }

                updateStatesBasedOnJobStatus(resultObj);
                performCalc(resultObj);
            }
        });
    }

    $scope.cancelJob = function($event, jobId) {
        if ($event != null) {
            $event.stopPropagation();
        }
        CancelJobModal.show(jobId, {}, function(){
            $state.go('home.jobs.status');
        });
    }
});
