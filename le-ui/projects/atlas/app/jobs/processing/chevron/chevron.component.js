angular.module('lp.jobs.chevron', [])

    .directive('chevron', [function () {
        var controller = ['$scope', function ($scope) {
            var vm = this;
            vm.failed = false;
            vm.chevronConfig = $scope.chevronconfig;

            function init() {
            }
            
            init();

            function isStepStatusMatching(stepName, status){
                for (var i = 0; i < $scope.stepscompleted.length; i++) {
                    if ($scope.stepscompleted[i].name == stepName && $scope.stepscompleted[i].stepStatus == status) {
                        return true;
                    }
                }
                return false;
            }


            vm.isStepDone = function (index) {
                if (isStepStatusMatching(vm.chevronConfig[index].name, 'Completed') || $scope.jobstatus == 'Completed') {
                    return true;
                } else {
                    return false;
                }
            };

            vm.isStepRunning = function (index) {
                if ($scope.jobstatus === 'Running') {
                    if (isStepStatusMatching(vm.chevronConfig[index].name, 'Running') ) {
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            };

            vm.isStepFailed = function (index) {
                if ($scope.jobstatus === 'Failed') {
                    if (isStepStatusMatching(vm.chevronConfig[index].name, 'Failed') ) {
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            };
            
            /**
             * We expect the list in Ascending order
             * @param {*} listSteps 
             */
            function getLatestEndTime(listSteps) {
                var latest = '';
                if(listSteps.length > 0){
                    latest = listSteps[listSteps.length - 1].endTimestamp;
                }
                return latest;
            }

            vm.getEndTime = function(index){
                var stepName = vm.chevronConfig[index].name;
                var listSteps = [];
                for (var i = 0; i < $scope.stepscompleted.length; i++) {
                    if ($scope.stepscompleted[i].name == stepName && $scope.stepscompleted[i].endTimestamp && $scope.stepscompleted[i].endTimestamp != null) {
                        listSteps.push($scope.stepscompleted[i]);
                    }
                }
                return getLatestEndTime(listSteps);
            };
        }];

        return {
            restrict: 'E',
            scope: {
                stepscompleted: '=', jobstatus: '=', chevronconfig: '='
            },
            controller: controller,
            controllerAs: 'vm',
            templateUrl: "app/jobs/processing/chevron/chevron.component.html",
        };
    }]);

