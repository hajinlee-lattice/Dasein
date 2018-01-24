angular.module('lp.jobs.import.row', [])

    .directive('importJobRow', [function () {
        var controller = ['$scope', '$interval', '$q', '$timeout', 'JobsStore', function ($scope, $interval, $q, $timeout, JobsStore) {
            $scope.loading = false;
            $scope.expanded = false;
            $scope.subjobs = [];
            $scope.stepscompleted = [];
            $scope.jobStatus = '';
            var POOLING_INTERVAL = 15 * 1000;
            var INTERVAL_ID;
            $scope.stepsConfig = {
                "Merging, De-duping & matching to Lattice Data Cloud": { position: 1, label: 'Merging, De-duping & Matching' },
                'Analyzing': { position: 2, label: 'Analyzing' },
                'Publishing': { position: 3, label: 'Publishing' },
                'Scoring': { position: 4, label: 'Scoring' }
            };

            function updateJobData(job) {
                $scope.job.jobStatus = job.jobStatus;
                $scope.jobStatus = job.jobStatus;
                $scope.subjobs = job.subJobs;
                updateStepsCompleted(job.stepsCompleted);
            }

            function updateStepsCompleted(steps) {
                for (var i = 0; i < steps.length; i++) {
                    var step = steps[i];
                    var stepObj = $scope.stepsConfig[step];
                    if ($scope.stepscompleted.length < stepObj.position) {
                        $scope.stepscompleted.push(step);
                    }

                }
            }

            function fetchJobData() {
                JobsStore.getJob($scope.job.id).then(function (ret) {
                    updateJobData(ret);
                });
            }
            function checkIfPooling() {
                if ($scope.job.jobStatus === 'Running' && $scope.expanded) {
                    INTERVAL_ID = $interval(fetchJobData, POOLING_INTERVAL);
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
                $scope.job = angular.copy($scope.job);
                $scope.jobStatus = $scope.job.jobStatus;
                // console.log($scope.stepscompleted);
                $scope.vm.callback = callbackModalWindow;
            }

            $scope.expandRow = function () {
                if ($scope.expanded) {
                    $scope.expanded = false;
                } else {
                    $scope.loading = true;
                    JobsStore.getJob($scope.job.id).then(function (ret) {
                        updateJobData(ret);
                        $scope.loading = false;
                        $scope.expanded = !$scope.expanded || false;
                        checkIfPooling();
                    });
                }

            };

            $scope.vm.run = function () {
                var show = $scope.vm.showWarningRun($scope.job);
                if (show) {
                    $scope.vm.toggleModal();

                } else {
                    $scope.vm.callback({ 'action': 'run' });
                }
            }



            $scope.$on("$destroy", function () {
                $interval.cancel(INTERVAL_ID);
            });

            init();

        }];
        return {
            restrict: 'E',
            transclude: true,
            scope: {
                job: '=', vm: '='
            },
            controller: controller,
            templateUrl: "app/jobs/processing/import-jobs-row.component.html",
        };
    }]);

