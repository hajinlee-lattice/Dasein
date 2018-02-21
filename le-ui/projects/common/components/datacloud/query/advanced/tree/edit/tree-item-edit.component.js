angular
    .module('common.datacloud.query.builder.tree.edit', [])
    .directive('queryItemEditDirective', function () {
        return {
            restrict: 'E',
            scope: {
                vm: '='
            },
            require: 'ngModel',
            templateUrl: '/components/datacloud/query/advanced/tree/edit/tree-item-edit.component.html',
            controllerAs: 'vm',
            controller: function ($scope, $timeout, $state, DataCloudStore, QueryStore, QueryTreeService) {
                var vm = $scope.vm;
                vm.booleanValue = QueryTreeService.getBooleanModel(vm.tree.bucketRestriction);
                vm.enumCmpModel = QueryTreeService.getEnumCmpModel(vm.tree.bucketRestriction);
                vm.numericalCmpModel = QueryTreeService.getNumericalCmpModel(vm.tree.bucketRestriction);
                vm.bktVals0 = QueryTreeService.getBktValue(vm.tree.bucketRestriction, 0);
                vm.bktVals1 = QueryTreeService.getBktValue(vm.tree.bucketRestriction, 1);
                vm.vals = vm.tree.bucketRestriction.bkt.Vals;
                
                vm.showEmptyOption = function () {
                    return QueryTreeService.showEmptyOption(vm.tree.bucketRestriction);
                }

                vm.getOperationLabel = function () {
                    return QueryTreeService.getOperationLabel(vm.type, vm.tree.bucketRestriction);
                }

                vm.getOperationValue = function (operatorType, position) {
                    return QueryTreeService.getOperationValue(vm.tree.bucketRestriction, operatorType, position);
                }

                vm.showItem = function (typeToShow) {
                    return QueryTreeService.showType(vm.tree.bucketRestriction, vm.type, typeToShow);
                }

                vm.showTo = function () {
                    return QueryTreeService.showTo(vm.tree.bucketRestriction);
                }

                vm.changeBooleanValue = function () {
                    QueryTreeService.changeBooleanValue(vm.tree.bucketRestriction, vm.booleanValue);
                }

                vm.changeEnumCmpValue = function () {
                    if (vm.enumCmpModel == 'is empty') {
                        vm.enumCmpModel = 'IS_EMPTY';
                        vm.vals.length = 0;
                    } else if (vm.enumCmpModel == 'is') {
                        vm.enumCmpModel = vm.vals.length == 1 ? 'EQUAL' : 'IN_COLLECTION';
                    } else if (vm.enumCmpModel == 'is not') {
                        vm.enumCmpModel = vm.vals.length == 1 ? 'NOT_EQUAL' : 'NOT_IN_COLLECTION';
                    }

                    console.log(vm.enumCmpModel, vm.vals);
                    QueryTreeService.changeEnumCmpValue(vm.tree.bucketRestriction, vm.enumCmpModel);
                }

                vm.getBktValue = function (position) {
                    return QueryTreeService.getBktValue(vm.tree.bucketRestriction, position);
                }
                vm.getCubeBktList = function () {
                    return QueryTreeService.getCubeBktList(vm.tree.bucketRestriction, vm.item.cube);
                }

                vm.changeNumericalCmpValue = function () {
                    QueryTreeService.changeNumericalCmpValue(vm.tree.bucketRestriction, vm.numericalCmpModel);
                }

                vm.changeBktVal = function (position) {
                    var val = vm['bktVals' + position];
                    QueryTreeService.changeBktValue(vm.tree.bucketRestriction, val, position);
                }

                vm.goToEnumPicker = function () {
                    QueryTreeService.setPickerObject({
                        item: vm.item,
                        restriction: vm.tree
                    });

                    $state.go('home.segment.explorer.enumpicker', { entity: vm.item.Entity, fieldname: vm.item.ColumnId });
                }
            }
        }
    });