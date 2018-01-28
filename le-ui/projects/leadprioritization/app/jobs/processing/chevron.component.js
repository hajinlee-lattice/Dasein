angular.module('lp.jobs.chevron', [])

    .directive('chevron', [function () {
        var controller = ['$scope', function ($scope) {
            var vm = this;
            vm.failed = false;
            // vm.jobstatus = $scope.jobstatus;
            // vm.stepscompleted = $scope.stepscompleted || [];
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

            vm.isStepDone = function (index) {
                if (index <= $scope.stepscompleted.length - 1 || $scope.jobStatus == 'Completed') {
                    return true;
                } else {
                    return false;
                }
            }
            vm.isStepRunning = function (index) {
                if($scope.jobstatus === 'Running'){
                    if($scope.stepscompleted.length === index){
                        return true;
                    }else {
                        return false;
                    }
                } else{
                    return false;
                }
            }
            vm.isStepFailed = function (index) {
                if($scope.jobstatus === 'Failed'){
                    if($scope.stepscompleted.length === index){
                        return true;
                    }else {
                        return false;
                    }
                } else{
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

