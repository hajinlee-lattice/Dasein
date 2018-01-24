angular.module('lp.jobs.chevron', [])

    .directive('chevron', [function () {
        var controller = ['$scope', function ($scope) {
            var vm = this;
            vm.failed = false;
            vm.jobstatus = $scope.jobstatus;
            vm.stepscompleted = $scope.stepscompleted || [];
            vm.stepsconfig = [];


            function init() {
                // console.log($scope.stepsconfig);
                var keys = Object.keys($scope.stepsconfig);
                for (var i = 0; i < keys.length; i++) {
                    // console.log($scope.stepsconfig[keys[i]].position);
                    vm.stepsconfig[$scope.stepsconfig[keys[i]].position - 1] = $scope.stepsconfig[keys[i]];
                }


                // console.log(vm.stepsconfig);

            }
            init();

            vm.getSteps = function () {
                var tmp = [];
            }
            vm.isStepDone = function (index) {
                if (index <= vm.stepscompleted.length - 1) {
                    return true;
                } else {
                    return false;
                }
            }
            vm.isStepRunning = function (index) {
                if (vm.jobstatus === 'Running' && index === (vm.stepscompleted.length - 1)) {
                    return true;
                } else {
                    return false;
                }
            }
            vm.isStepFailed = function (index) {
                if (vm.jobstatus === 'Failed' && index === vm.stepscompleted.length - 1) {
                    return true;
                } else {
                    return false;
                }
            }
            // init();
        }];

        return {
            restrict: 'E',
            scope: {
                stepscompleted: '=', jobstatus: '=', stepsconfig: '='
            },
            controller: controller,
            controllerAs: 'vm',
            templateUrl: "app/jobs/processing/chevron.component.html",
        };
    }]);

