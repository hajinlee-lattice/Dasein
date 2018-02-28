angular
    .module('common.datacloud.query.builder.tree.edit.transaction', ['common.datacloud.query.builder.tree.transaction.service'])
    .directive('transactionDirective', function () {
        return {
            restrict: 'E',
            scope: {
                type: '=',
                bucketrestriction: '='
            },
            templateUrl: '/components/datacloud/query/advanced/tree/transaction/transaction-item.component.html',
            controllerAs: 'vm',
            controller: function ($scope, $timeout, $state, QueryTreeTransactionService, QueryTreeService) {
                var vm = $scope.vm;

                vm.init = function () {

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