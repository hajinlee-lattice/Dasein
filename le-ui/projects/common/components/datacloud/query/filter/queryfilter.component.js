angular.module('common.datacloud.query.filter', [])
.directive('queryFilter', function() {
    return {
        restrict: 'E',
        replace: true,
        scope: {
            attribute: '=',
            onMove: '&',
            onDelete: '=',
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

    vm.deleteBucket = function(index) {
        $scope.onDelete($scope.group, $scope.attribute.columnName, index);
    };
})
.filter('QueryRange', function() {
    return function(range) {
        if (!range) {
            return null;
        }
        if (range.is_null_only) {
            return 'null';
        }
        return range.max === range.min ? range.max : range.min + ' - ' + range.max;
    };
});
