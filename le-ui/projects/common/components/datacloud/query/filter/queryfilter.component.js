angular.module('common.datacloud.queryfilter', [])
.directive('queryFilter', function() {
    return {
        restrict: 'E',
        replace: true,
        scope: {
            attribute: '=',
            onMove: '&',
            onDelete: '&',
            group: "@"
        },
        templateUrl: '/components/datacloud/query/filter/queryfilter.component.html',
        controller: 'QueryFilterController',
        controllerAs: 'vm'
    }
})
.controller('QueryFilterController', function($scope) {
    var vm = this;
    angular.extend(this, {});

    vm.deleteBucket = function (index) {
        $scope.attribute.buckets.splice(index, 1);
        if ($scope.attribute.buckets.length === 0) {
            $scope.onDelete();
        }
    };
});
