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

                $state.go('home.segment.explorer.enumpicker', { entity: entity, fieldname: fieldname });
            }

            vm.getOperationLabel = function(type, bucketRestriction) {
                return QueryTreeService.getOperationLabel(type, bucketRestriction);
            }

            vm.getOperationValue = function(bucketRestriction, type) {
                return QueryTreeService.getOperationValue(bucketRestriction, type);
            }

            vm.getQuerySnippet = function(enrichment, rule, type) {
                var querySnippet = enrichment.DisplayName + ' ';
                if (vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId].Bkts) { //bucketable attributes
                    querySnippet += 'is ';
                    querySnippet += (type == 'Enum' && rule.bucketRestriction.bkt.Vals.length > 1) ? vm.generateBucketLabel(rule.bucketRestriction.bkt) : rule.bucketRestriction.bkt.Lbl;
                } else { //non-bucketable attributes e.g. pure-string
                    querySnippet += vm.getOperationLabel('String', rule.bucketRestriction) + ' '  + vm.getOperationValue(rule.bucketRestriction, 'String');
                }
                return querySnippet;
            }

            vm.NumberUtility = NumberUtility;
        }
    };
});