angular
    .module('common.datacloud.query.builder.tree.info', [])
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

                vm.getOperationLabel = function() {
                    return QueryTreeService.getOperationLabel(vm.type, vm.tree.bucketRestriction);
                }

                vm.getOperationValue = function(operatorType, position) {
                    return QueryTreeService.getOperationValue(vm.tree.bucketRestriction, operatorType, position);
                }
                vm.showItem = function(typeToShow){
                    var ret = QueryTreeService.showType(vm.tree.bucketRestriction, vm.type, typeToShow);
                    return ret;
                }
                vm.showTo = function(){
                    return QueryTreeService.showTo(vm.tree.bucketRestriction);
                }
            }
        }
    });