angular
    .module('common.datacloud.query.builder.tree.info', [])
    .directive('queryItemDirective', function () {
        return {
            restrict: 'E',
            scope: {
                vm: '='
            },
            templateUrl: '/components/datacloud/query/advanced/tree/item/tree-item.component.html',
            controllerAs: 'vm',
            controller: function ($scope, QueryTreeService) {
                var vm = $scope.vm;
                vm.chipsOperations = ['EQUAL', 'IN_COLLECTION', 'NOT_EQUAL', 'NOT_IN_COLLECTION'];

                vm.getOperationLabel = function () {
                    return QueryTreeService.getOperationLabel(vm.type, vm.tree.bucketRestriction);
                }

                vm.getOperationValue = function (type, pos) {
                    var val = QueryTreeService.getOperationValue(vm.tree.bucketRestriction, type, pos);
                    return type == 'String' && typeof val == 'string' ? [val] : val;
                }

                vm.isNumericalChips = function () {
                    let ret = vm.chipsOperations.indexOf(vm.tree.bucketRestriction.bkt.Cmp) > -1;
                    //console.log('[tree-item] isNumericalChips()', ret);
                    return ret;
                }

                vm.getSuffixLabel = function (index) {
                    let suffix = vm.showTo() ? ' - ' : ', ';
                    return vm.getOperationValue('Enum').length > 1 && index != 0 ? suffix : '';
                }

                vm.showItem = function (typeToShow) {
                    return QueryTreeService.showType(vm.tree.bucketRestriction, vm.type, typeToShow);
                }

                vm.showTo = function () {
                    return QueryTreeService.showTo(vm.tree.bucketRestriction);
                }
            }
        }
    });