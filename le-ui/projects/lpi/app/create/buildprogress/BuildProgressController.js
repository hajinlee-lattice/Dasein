angular.module('lp.create.import.job', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'lp.create.import',
    'lp.jobs'
])
.controller('ImportJobController', function(
    $scope, $state, $stateParams, $interval, ResourceUtility, 
    JobsStore, JobsService, ImportStore, ServiceErrorUtility,
    CancelJobModal, BuildProgressConfig
) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.applicationId = $stateParams.applicationId;

    BuildProgressConfig = BuildProgressConfig || {};

    $scope.config = BuildProgressConfig;
    $scope.button_goto_sref = BuildProgressConfig.button_goto_sref || 'home.models';

    var REFRESH_JOB_INTERVAL_ID;
    var REFRESH_PERFORM_CALC_ID; 
    var TIME_BETWEEN_JOB_REFRESH = 30 * 1000;

    getJobStatusFromAppIdAndPerformCalc();

    REFRESH_JOB_INTERVAL_ID = $interval(getJobStatusFromAppIdAndPerformCalc, TIME_BETWEEN_JOB_REFRESH);

    var lastKnownStepBeforeCancel = null;
    var jobSteps = ['no_mapped_step_name', 'load_data', 'generate_insights', 'create_global_target_market', 'score_training_set'];

    $scope.jobStepsRunningStates = jobSteps.reduce(function(state, step) {
        state[step] = false;
        return state;
    }, {});

    $scope.jobStepsCompletedStates = jobSteps.reduce(function(state, step) {
        state[step] = false;
        return state;
    }, {});

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

        // console.log(job);

        if (job.jobStatus === 'Pending' || job.stepRunning === 'no_mapped_step_name') {
            ceiling = 10;
        } else if (job.stepRunning === 'load_data'){
            ceiling = 35;
        } else if (job.stepRunning === 'generate_insights'){
            ceiling = 60;
        } else if (job.stepRunning === 'create_global_target_market'){
            ceiling = 80;
        } else if (job.stepRunning === 'score_training_set') {
            ceiling = 90;
        } else if (job.jobStatus === 'Completed') {
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

            if ($scope.jobStepsCompletedStates["score_training_set"]) {
                $scope.jobStepsCompletedStates['generate_insights'] = true;
                $scope.jobStepsCompletedStates['create_global_target_market'] = true;

                $scope.jobStepsRunningStates['generate_insights'] = false;
                $scope.jobStepsRunningStates['create_global_target_market'] = false;

                $scope.jobStepsRunningStates['score_training_set'] = true;
                $scope.jobStepsCompletedStates['score_training_set'] = false;
            }
        }

        $scope.stepsCompletedTimes = job.completedTimes;

        var stepFailed = lastKnownStepBeforeCancel || job.stepFailed;
        if ((stepFailed === "no_mapped_step_name" ||
            stepFailed === "load_data" ||
            stepFailed === "generate_insights" ||
            stepFailed === "create_global_target_market") &&
            $scope.jobStepsCompletedStates["score_training_set"]) {
            stepFailed = "score_training_set";
        }
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

    $scope.$on('updateAsCancelledJob', function() {

        for (var i = 0; i < jobSteps.length; i++) {
            var step = jobSteps[i];
            if ($scope.jobStepsRunningStates[step] && !$scope.jobStepsCompletedStates[step]) {
                lastKnownStepBeforeCancel = step;
                break;
            }
        }

    });
});
