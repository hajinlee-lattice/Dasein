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
        controller: function ($scope, $document, $timeout, $interval, DataCloudStore, NumberUtility) {
            var vm = $scope.vm;
            
            angular.extend(vm, { });

            vm.booleanStats = function(stats) {
                var booleans = {};
                for(var i in stats) {
                    var stat = stats[i];
                    booleans[stat.Lbl] = stat;
                }
                return booleans;
            }

            vm.NumberUtility = NumberUtility;
        }
    };
});