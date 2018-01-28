angular.module('lp.jobs.import.row', [])

    .directive('importJobRow', [function () {
        var controller = ['$scope', '$interval', '$q', '$timeout', 'JobsStore', function ($scope, $interval, $q, $timeout, JobsStore) {

            $scope.subjobs = [];
            $scope.stepscompleted = [];
            $scope.jobStatus = '';
            var mapSubJobs = {};
            var POOLING_INTERVAL = 15 * 1000;
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
                console.log('STOP the timer');
                $interval.cancel(INTERVAL_ID);
            }

            function updateSubjobs(subJobsUpdated) {
                for (var i = 0; i < subJobsUpdated.length; i++) {
                    var jobid = subJobsUpdated[i].id;
                    var inMap = mapSubJobs[jobid];
                    if (inMap === undefined) {
                        $scope.subjobs.push(subJobsUpdated[i]);
                        mapSubJobs[jobid] = $scope.subjobs.length - 1;
                    } else {
                        $scope.subjobs[inMap] = subJobsUpdated[i];
                    }
                }
            }

            function updateJobData(jobUpdated) {
                $scope.jobStatus = jobUpdated.jobStatus;
                console.log('UPDATED ----------------> ', jobUpdated);
                // $scope.job.subJobs = jobUpdated.subJobs;
                updateSubjobs(jobUpdated.subJobs);
                // $scope.job.stepsCompleted = jobUpdated.stepsCompleted;
                updateStepsCompleted(jobUpdated.stepsCompleted);
                if ($scope.job.jobStatus !== 'Running') {
                    cancelInterval();
                }
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
                console.log('Pinging the server');
                JobsStore.getJob($scope.job.id).then(function (ret) {
                    updateJobData(ret);
                });
            }

            function checkIfPooling() {
                if (($scope.job.jobStatus === 'Running' || $scope.job.jobStatus === 'Pending') && $scope.expanded) {
                    if (INTERVAL_ID === undefined) {
                        console.log('Create the timer');
                        INTERVAL_ID = $interval(fetchJobData, POOLING_INTERVAL);
                    }
                } else {
                    cancelInterval();
                }
            }
            function callbackModalWindow(action) {
                if (action && action.action === 'run') {
                    JobsStore.runJob($scope.job).then(function (updatedJob) {
                        checkIfPooling();
                    });
                }
            }
            function init() {
                console.log('init');
                $scope.vm.callback = callbackModalWindow;
                $scope.loading = false;
                $scope.expanded = false;
                $scope.jobStatus = $scope.job.jobStatus;
            }

            $scope.expandRow = function () {
                if ($scope.expanded) {
                    $scope.expanded = false;
                    resetCollapsedRow();
                } else {
                    $scope.loading = true;
                    JobsStore.getJob($scope.job.id).then(function (ret) {
                        $scope.loading = false;
                        $scope.expanded = !$scope.expanded || false;
                        checkIfPooling();
                        updateJobData(ret);
                    });
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
            templateUrl: "app/jobs/processing/import-jobs-row.component.html",
        };
    }]);

