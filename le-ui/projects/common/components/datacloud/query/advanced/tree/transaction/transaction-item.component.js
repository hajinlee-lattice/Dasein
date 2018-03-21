angular
    .module('common.datacloud.query.builder.tree.edit.transaction', ['common.datacloud.query.builder.tree.transaction.service'])
    .directive('transactionDirective', function () {
        return {
            restrict: 'E',
            scope: {
                type: '=',
                bucketrestriction: '=',
                purchased: '='
            },
            templateUrl: '/components/datacloud/query/advanced/tree/transaction/transaction-item.component.html',
            controllerAs: 'vm',
            controller: function ($scope, $timeout, $state, QueryTreeService) {
                var vm = $scope.vm;

                vm.init = function () {

                }
                vm.init();

                vm.getPeriod = function(){
                    var period = QueryTreeService.getPeriodValue($scope.bucketrestriction, $scope.type, 'Time');
                    if(period != 'Date' && vm.getCmp('Time') !== 'Ever'){
                        return period+'(s)';
                    }
                }

                vm.showSubTypeSelection = function(subType){
                    switch(subType){
                        case 'Amt':
                        case 'Qty': {
                            if($scope.purchased == true){
                                return true;
                            }else{
                                return false;
                            }
                        }
                        default:
                            return true;
                    }
                }

                vm.getCmp = function (subType) {
                    var ret = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, subType);
                    switch (subType) {
                        case 'Time': {
                            var cmp = ret === '' ? 'Ever' : QueryTreeService.cmpMap[ret];
                            return cmp;
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
                vm.showLabel = function(subType){
                    var cmp = vm.getCmp(subType);
                    switch(subType){
                        case 'Amt': {
                            if('Any' === cmp){
                                return false;
                            }
                            return true;
                        }
                        default:
                        return fasle;
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