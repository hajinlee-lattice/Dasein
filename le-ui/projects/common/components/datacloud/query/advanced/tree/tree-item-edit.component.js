angular
    .module('common.datacloud.query.builder.tree')
    .directive('queryItemEditDirective', function () {
        return {
            restrict: 'E',
            scope: {
                vm: '='
            },
            templateUrl: '/components/datacloud/query/advanced/tree/tree-item-edit.component.html',
            controllerAs: 'vm',
            controller: function ($scope, $timeout, DataCloudStore, QueryStore, QueryTreeService) {
                var vm = $scope.vm;
            }
        }
    });