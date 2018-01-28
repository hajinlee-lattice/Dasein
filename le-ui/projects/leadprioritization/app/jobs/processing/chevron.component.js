angular.module('lp.jobs.chevron', [])

    .directive('chevron', [function () {
        var controller = ['$scope', function ($scope) {
            var vm = this;
            vm.failed = false;
            vm.stepsconfig = [];


            function init() {
                var keys = Object.keys($scope.stepsconfig);
                for (var i = 0; i < keys.length; i++) {
                    vm.stepsconfig[$scope.stepsconfig[keys[i]].position - 1] = $scope.stepsconfig[keys[i]];
                }
            }
            init();

            vm.isStepDone = function (index) {
                console.log('Steps completed', $scope.stepscompleted, index);
                if ((index +1 )  < $scope.stepscompleted.length  || $scope.jobstatus == 'Completed') {
                    return true;
                } else {
                    return false;
                }
            }
            vm.isStepRunning = function (index) {
                if($scope.jobstatus === 'Running'){
                    if( (index + 1 ) == $scope.stepscompleted.length){
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
                    if( (index + 1 ) == $scope.stepscompleted.length){
                        return true;
                    }else {
                        return false;
                    }
                } else{
                    return false;
                }
            }
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

