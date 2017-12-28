angular
    .module('common.datacloud.query.builder.tree.edit', [])
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
                vm.booleanValue = QueryTreeService.getBooleanModel(vm.tree.bucketRestriction);
                vm.enumCmpModel = QueryTreeService.getEnumCmpModel(vm.tree.bucketRestriction);
                vm.numericalCmpModel = QueryTreeService.getNumericalCmpModel(vm.tree.bucketRestriction);
                vm.bktVals0 = QueryTreeService.getBktValue(vm.tree.bucketRestriction, 0);
                vm.bktVals1 = QueryTreeService.getBktValue(vm.tree.bucketRestriction, 1);

                vm.getOperationLabel = function () {
                    return QueryTreeService.getOperationLabel(vm.type, vm.tree.bucketRestriction);
                }

                vm.getOperationValue = function (operatorType, position) {
                    return QueryTreeService.getOperationValue(vm.tree.bucketRestriction, operatorType, position);
                }
                vm.showItem = function (typeToShow) {
                    var ret = QueryTreeService.showType(vm.tree.bucketRestriction, vm.type, typeToShow);
                    return ret;
                }
                vm.showTo = function () {
                    return QueryTreeService.showTo(vm.tree.bucketRestriction);
                }

                vm.changeBooleanValue = function () {
                    QueryTreeService.changeBooleanValue(vm.tree.bucketRestriction, vm.booleanValue);
                }
                vm.changeEnumCmpValue = function () {
                    QueryTreeService.changeEnumCmpValue(vm.tree.bucketRestriction, vm.enumCmpModel);
                }

                vm.getBktValue = function (position) {
                    return QueryTreeService.getBktValue(vm.tree.bucketRestriction, position);
                }
                vm.getCubeBktList = function () {
                    return QueryTreeService.getCubeBktList(vm.tree.bucketRestriction, vm.item.cube);
                }

                vm.changeNumericalCmpValue = function(){
                    QueryTreeService.changeNumericalCmpValue(vm.tree.bucketRestriction, vm.numericalCmpModel);
                }

                vm.changeBktVal = function(position){
                    var val = vm['bktVals'+position];
                    QueryTreeService.changeBktValue(vm.tree.bucketRestriction, val, position);
                }
            }
        }
    });