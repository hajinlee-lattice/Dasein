angular
.module('common.datacloud.query.advanced.tree', [])
.directive('queryTreeDirective',function() {
    return {
        restrict: 'AE',
        scope: {
            tree: '=',
            items: '=',
            operator: '@'
        },
        templateUrl: '/components/datacloud/query/advanced/tree/tree.component.html',
        controllerAs: 'vm',
        controller: function ($scope, $document, $timeout, $interval) {
            var vm = this;

            angular.extend(vm, {
                tree: $scope.tree,
                items: $scope.items,
                operator: $scope.operator
            });

            vm.init = function (type, value) {
                //console.log('queryTreeDirective', vm.tree, vm.items, vm.operator);
            }

            vm.init();
        }
    };
});