angular.module('lp.jobs.import.row', [])

    .directive('importJobRow', [function () {
        var controller = ['$scope', '$interval', '$q', '$timeout', 'JobsStore', function ($scope, $interval, $q, $timeout, JobsStore) {

            $scope.disableButton = false;
            $scope.subjobs = [];
            $scope.stepscompleted = [];
            $scope.jobStatus = '';
            var POOLING_INTERVAL = 60 * 1000;
            var INTERVAL_ID;

            $scope.stepsConfig = {
                "Merging, De-duping & matching to Lattice Data Cloud": { position: 1, label: 'Merging, De-duping & Matching' },
                'Analyzing': { position: 2, label: 'Analyzing' },
                'Publishing': { position: 3, label: 'Publishing' },
                'Scoring': { position: 4, label: 'Scoring' }
            };

            function resetCollapsedRow() {
                $scope.subjobs = [];
                $scope.stepscompleted = [];
            }

            function cancelInterval() {
                // console.log('STOP the timer');
                $interval.cancel(INTERVAL_ID);
                INTERVAL_ID = null;
            }

            function updateSubjobs(subJobsUpdated) {
                $scope.subjobs.splice(0, $scope.subjobs.length);
                for (var i = 0; i < subJobsUpdated.length; i++) {
                    $scope.subjobs.push(subJobsUpdated[i]);
                }
                $scope.job.subJobs = subJobsUpdated;
                $scope.job.actions =  subJobsUpdated;
            }

            function updateStepsCompleted(steps) {
                if (steps.length == 0) {
                    $scope.stepscompleted = [];
                } else {
                    for (var i = 0; i < steps.length; i++) {
                        var step = steps[i];
                        var stepObj = $scope.stepsConfig[step];
                        if ($scope.stepscompleted.length < stepObj.position) {
                            $scope.stepscompleted.push(step);
                        }
                    }
                }
            }

            function fetchJobData() {
                // console.log('Pinging the server', $scope.job.id);
                if ($scope.job.id != null) {
                    JobsStore.getJob($scope.job.id).then(function (ret) {
                        updateSubjobs(ret.subJobs);
                        updateStepsCompleted(ret.stepsCompleted);
                        checkIfPooling();
                    });
                }
            }
            
            function areSubJobsCompleted(){
                var subJobs = $scope.job.subJobs;
                if(subJobs != undefined && subJobs != null){
                    var allCompleted = true;
                    for(var i = 0; i<subJobs.length; i++){
                        if(subJobs[i].jobStatus != 'Completed'){
                            allCompleted = false;
                            break;
                        }
                    }
                    // console.log('All subjobs Completed', allCompleted);
                    return allCompleted;
                }else{
                    // console.log('All subjobs Completed', false);
                    return false;
                }

            }

            function checkIfPooling() {
                // console.log('checkIfPooling', $scope.job.jobStatus, $scope.expanded);
                if (($scope.job.jobStatus === 'Running') && $scope.expanded || 
                        (areSubJobsCompleted() == false && ($scope.job.jobStatus === 'Running' || $scope.job.jobStatus === 'Pending'))) {
                    if (INTERVAL_ID === undefined || INTERVAL_ID == null) {
                        // console.log('Create the timer');
                        INTERVAL_ID = $interval(fetchJobData, POOLING_INTERVAL);
                    }
                } else {
                    cancelInterval();
                }
            }
            function callbackModalWindow(action) {
                if (action && action.action === 'run') {
                    $scope.disableButton = true;
                    JobsStore.runJob($scope.job).then(function (updatedJob) {
                        checkIfPooling();
                    });
                }
            }
            function init() {
                // console.log('init', $scope.job);
                $scope.vm.callback = callbackModalWindow;
                $scope.loading = false;
                $scope.expanded = false;
                $scope.jobStatus = $scope.job.jobStatus;
                checkIfPooling();
            }

            $scope.expandRow = function () {
                if ($scope.job.id != null) {
                    if ($scope.expanded) {
                        $scope.expanded = false;
                        cancelInterval();
                        resetCollapsedRow();
                    } else {
                        $scope.loading = true;
                        var jobId = $scope.job.id;

                        JobsStore.getJob(jobId).then(function (ret) {
                            // console.log('RET',ret);
                            $scope.loading = false;
                            $scope.expanded = !$scope.expanded || false;
                            // console.log(ret);
                            updateSubjobs(ret.subJobs);
                            updateStepsCompleted(ret.stepsCompleted);
                            checkIfPooling();
                        });
                    }
                }
            };

            $scope.vm.run = function () {
                var show = $scope.showWarningRun($scope.job);
                if (show) {
                    $scope.vm.toggleModal();

                } else {
                    $scope.vm.callback({ 'action': 'run' });
                }
            }

            $scope.showWarningRun = function (job) {
                var actions = job.actions;
                var allCompleted = true;
                if (actions) {
                    for (var i = 0; i < actions.length; i++) {
                        if (actions[i].jobStatus === 'Running') {
                            allCompleted = false;
                            break;
                        }
                    }
                }
                return !allCompleted;
            }

            $scope.showRunButton = function (job) {
                if (job.jobStatus === 'Pending') {
                    return true;
                } else {
                    return false;
                }
            }

            $scope.showReport = function (job) {

                if (job.jobStatus === 'Completed' || job.jobStatus === 'Failed') {
                    return true;
                } else {
                    return false;
                }
            }

            $scope.isJobPending = function (job) {
                if (job.jobStatus === 'Pending') {
                    return true;
                } else {
                    return false;
                }

            }
            $scope.isJobCompleted = function (job) {
                if ('Completed' === job.jobStatus) {
                    return true;
                } else {
                    return true;
                }
            }

            $scope.isJobFailed = function (job) {
                if (job.jobStatus === 'Failed') {
                    return true;
                } else {
                    return false;
                }
            }
            $scope.isJobRunning = function (job) {
                if (job.jobStatus === 'Running') {
                    return true;
                } else {
                    return false;
                }
            }

            $scope.getActionsCount = function(){
                if($scope.job.inputs){
                    var idsString = $scope.job.inputs.ACTION_IDS;
                    var ids = JSON.parse(idsString);
                    return ids.length;
                }else{
                    return '-';
                }
            }

            $scope.$on("$destroy", function () {
                $interval.cancel(INTERVAL_ID);
            });

            init();

        }];
        return {
            restrict: 'E',
            transclude: false,
            scope: {
                job: '=', vm: '='
            },
            controller: controller,
            templateUrl: "app/jobs/processing/job/import-jobs-row.component.html",
        };
    }]);

