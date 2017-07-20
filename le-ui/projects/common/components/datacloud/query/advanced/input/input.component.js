angular
.module('common.datacloud.query.advanced.input', [])
.directive('queryInputDirective',function() {
    return {
        restrict: 'AE',
        scope: {
            tree: '=',
            items: '=',
            operator: '@'
        },
        templateUrl: '/components/datacloud/query/advanced/input/input.component.html',
        controllerAs: 'vm',
        controller: function ($scope, $document, $timeout, $interval) {
            var vm = this;

            angular.extend(vm, {
                tree: $scope.tree,
                items: $scope.items,
                operator: $scope.operator
            });

            vm.init = function (type, value) {
                console.log('queryInputDirective', vm.tree, vm.items, vm.operator);
            }

            vm.init();
        }
    };
});