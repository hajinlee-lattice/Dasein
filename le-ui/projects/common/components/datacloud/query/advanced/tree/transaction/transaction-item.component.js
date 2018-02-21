angular
    .module('common.datacloud.query.builder.tree.edit.transaction', [])
    .directive('transactionDirective', function () {
        return {
            restrict: 'E',
            scope: {
                type: '=',
                bucketrestriction: '='
            },
            templateUrl: '/components/datacloud/query/advanced/tree/transaction/transaction-item.component.html',
            controllerAs: 'vm',
            controller: function ($scope, $timeout, $state, DataCloudStore, QueryStore, QueryTreeService) {
                var vm = $scope.vm;

                vm.init = function () {
                    vm.values = {
                        time1: { val: 0, position: 0, type: 'Time' },
                        time2: { val: 0, position: 1, type: 'Time' },
                        qty1: { val: 0, position: 0, type: 'Qty' },
                        qty2: { val: 0, position: 1, type: 'Qty' },
                        amt1: { val: 0, position: 0, type: 'Amt' },
                        amt2: { val: 0, position: 1, type: 'Amt' },
                    }

                    var keys = Object.keys(vm.values);
                    keys.forEach(function (element) {
                        vm.values[element].val = QueryTreeService.getValue($scope.bucketrestriction, $scope.type, vm.values[element].position, vm.values[element].type);
                    });
                }
                vm.init();


                vm.getCmp = function (subType) {
                    var ret = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, subType);
                    switch (subType) {
                        case 'Time': {
                            return ret === '' ? 'Ever' : QueryTreeService.cmpMap[ret];
                        }
                        case 'Amt':
                        case 'Qty': {
                            return ret === '' ? 'Any' : QueryTreeService.cmpMap[ret];
                        }
                        default: {
                            return ret;
                        }
                    }
                }

                vm.getValues = function (subType) {
                    var ret = QueryTreeService.getValues($scope.bucketrestriction, $scope.type, subType);
                    switch (ret.length) {
                        case 0: {
                            return '';
                        }
                        case 1: {
                            if (subType === 'Time' && ret[0] === -1) {
                                return '';
                            }
                            return ret[0];
                        }
                        case 2: {
                            return ret[0] + ' - ' + ret[1];
                        }
                        default:
                            return '';
                    }
                }
            }
        }
    });