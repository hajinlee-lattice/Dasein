angular
    .module('common.datacloud.query.builder.tree')
    .directive('queryItemDirective', function () {
        return {
            restrict: 'E',
            scope: {
                vm: '='
            },
            templateUrl: '/components/datacloud/query/advanced/tree/tree-item.component.html',
            controllerAs: 'vm',
            controller: function ($scope, $timeout, DataCloudStore, QueryStore, QueryTreeService) {
                var vm = $scope.vm;
            }
        }
    });