angular
.module('common.datacloud.explorer.attributetile', [])
.directive('explorerAttributeTile',function() {
    return {
        restrict: 'A',
        scope: {
            vm: '=',
            count: '=',
            enrichment: '='
        },
        controllerAs: 'vm',
        templateUrl: '/components/datacloud/explorer/attributetile/attributetile.component.html',
        controller: function ($scope, $document, $timeout, $interval, DataCloudStore) {
            var vm = $scope.vm;
            
            angular.extend(vm, { });

        }
    };
});