angular
.module('common.datacloud.explorer.attributetile', ['mainApp.appCommon.utilities.NumberUtility'])
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
        controller: function (
            $scope, $state, $document, $timeout, $interval, 
            DataCloudStore, NumberUtility, QueryTreeService
        ) {
            var vm = $scope.vm;
            
            angular.extend(vm, { });

            vm.booleanStats = function(stats) {
                var booleans = {};

                for (var i in stats) {
                    var stat = stats[i];
                    booleans[stat.Lbl] = stat;
                }

                return booleans;
            }

            vm.goToEnumPicker = function(bucket, enrichment) {
                QueryTreeService.setPickerObject({
                    item: enrichment
                    //restriction: bucket 
                });

                var entity = enrichment.Entity;
                var fieldname = enrichment.ColumnId;

                console.log(entity, fieldname, bucket, enrichment);

                $state.go('home.segment.explorer.enumpicker', { entity: entity, fieldname: fieldname });
            }

            vm.NumberUtility = NumberUtility;
        }
    };
});