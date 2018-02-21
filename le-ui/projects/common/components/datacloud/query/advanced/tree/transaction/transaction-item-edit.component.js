angular
    .module('common.datacloud.query.builder.tree.edit.transaction.edit', [])
    .directive('transactionEditDirective', function () {
        return {
            restrict: 'E',
            scope: {
                type: '=',
                bucketrestriction: '='
            },
            templateUrl: '/components/datacloud/query/advanced/tree/transaction/transaction-item-edit.component.html',
            controllerAs: 'vm',
            controller: function ($scope, $timeout, $state, DataCloudStore, QueryStore, QueryTreeService) {
                var vm = $scope.vm;

                vm.cmpsList = [
                    { 'name': 'EVER', 'displayName': 'Ever' },
                    { 'name': 'IN_CURRENT', 'displayName': 'Current' },
                    { 'name': 'PREVIOUS', 'displayName': 'Previous' },
                    { 'name': 'PRIOR_OLY_LT', 'displayName': 'Only Prior to Last' },
                    { 'name': 'BETWEEN_LT', 'displayName': 'Between Last' },
                    { 'name': 'BETWEEN', 'displayName': 'Between' },
                    { 'name': 'BEFORE', 'displayName': 'Before' },
                    { 'name': 'AFTER', 'displayName': 'After' }
                ];

                vm.periodList = [
                    { 'name': 'Week', 'displayName': 'Week' },
                    { 'name': 'Month', 'displayName': 'Month' },
                    { 'name': 'Quarter', 'displayName': 'Quarter' },
                    { 'name': 'Year', 'displayName': 'Year' }
                ];
                vm.unitPurchasedCmpChoises = [
                    { 'name': 'EQUAL', 'displayName': 'Equal' },
                    { 'name': 'NOT_EQUAL', 'displayName': 'Not Equal' },
                    { 'name': 'GREATER_THAN', 'displayName': 'Greater Than' },
                    { 'name': 'GREATER_OR_EQUAL', 'displayName': 'Greater or Equal' },
                    { 'name': 'LESS_THAN', 'displayName': 'Less Than' },
                    { 'name': 'LESS_OR_EQUAL', 'displayName': 'Lesser or Equal' },
                    { 'name': 'GTE_AND_LT', 'displayName': 'Between' },
                    { 'name': 'ANY', 'displayName': 'Any' },
                ];
                vm.amountSpentCmpChoises = [
                    { 'name': 'GREATER_THAN', 'displayName': 'Greater Than' },
                    { 'name': 'GREATER_OR_EQUAL', 'displayName': 'Greater or Equal' },
                    { 'name': 'LESS_THAN', 'displayName': 'Less Than' },
                    { 'name': 'LESS_THAN', 'displayName': 'Lesser or Equal' },
                    { 'name': 'GTE_AND_LT', 'displayName': 'Between' },
                    { 'name': 'ANY', 'displayName': 'Any' },
                ];

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

                    console.log(vm.values);

                    var tmp = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, 'Time');
                    vm.timeCmp = tmp !== '' ? tmp : 'EVER';
                    var periodTmp = QueryTreeService.getPeriodValue($scope.bucketrestriction, $scope.type, 'Time');
                    vm.timeframePeriod = periodTmp !== '' ? periodTmp : vm.periodList[0].name;

                    var tmpUnit = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, 'Qty');
                    vm.unitPurchasedCmp = tmpUnit !== '' ? tmpUnit : 'ANY';

                    var tmpAmt = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, 'Amt');
                    vm.amtCmp = tmpAmt !== '' ? tmpAmt : 'ANY';
                    
                }
                vm.init();

                vm.changeVal = function (keyName) {
                    switch (keyName) {
                        case 'time1':
                        case 'time2': {
                            QueryTreeService.changeBktValue($scope.bucketrestriction, $scope.type, vm.values[keyName].val, vm.values[keyName].position, 'Time');
                            break;
                        }

                        case 'qty1':
                        case 'qty2': {
                            QueryTreeService.changeBktValue($scope.bucketrestriction, $scope.type, vm.values[keyName].val, vm.values[keyName].position, 'Qty');
                            break;
                        }
                        case 'amt1':
                        case 'amt2': {
                            QueryTreeService.changeBktValue($scope.bucketrestriction, $scope.type, vm.values[keyName].val, vm.values[keyName].position, 'Amt');
                            break;
                        }
                    }
                }

                vm.changeCmp = function (value, type) {
                    QueryTreeService.changeCmp($scope.bucketrestriction, $scope.type, value, type);
                }

                /************************************************** */
                // vm.cmpsList = [
                //     { 'name': 'EVER', 'displayName': 'Ever' },
                //     { 'name': 'IN_CURRENT', 'displayName': 'Current' },
                //     { 'name': 'PREVIOUS', 'displayName': 'Previous' },
                //     { 'name': 'PRIOR_OLY_LT', 'displayName': 'Only Prior to Last' },
                //     { 'name': 'BETWEEN_LT', 'displayName': 'Between Last' },
                //     { 'name': 'BETWEEN', 'displayName': 'Between' },
                //     { 'name': 'BEFORE', 'displayName': 'Before' },
                //     { 'name': 'AFTER', 'displayName': 'After' }
                // ];

                // vm.periodList = [
                //     { 'name': 'Week', 'displayName': 'Week' },
                //     { 'name': 'Month', 'displayName': 'Month' },
                //     { 'name': 'Quarter', 'displayName': 'Quarter' },
                //     { 'name': 'Year', 'displayName': 'Year' }
                // ]

                // var tmp = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, 'Time');
                // vm.timeCmp = tmp !== '' ? tmp : 'EVER';
                // var periodTmp = QueryTreeService.getPeriodValue($scope.bucketrestriction, $scope.type, 'Time');
                // vm.timeframePeriod = periodTmp !== '' ? periodTmp : vm.periodList[0].name;

                //************************ Txn *********************/

                vm.getFromId = function () {
                    var id = $scope.bucketrestriction.attr;
                    return id + '.txn_from';
                }

                vm.getToId = function () {
                    var id = $scope.bucketrestriction.attr;
                    return id + '.txn_to';
                }

                vm.showTimeframePeriod = function () {
                    switch (vm.timeCmp) {
                        case 'EVER':
                        case 'BETWEEN':
                        case 'BEFORE':
                        case 'AFTER': {
                            return false;
                        }
                        default:
                            return true;

                    }
                }

                vm.showTimeFrom = function () {
                    switch (vm.timeCmp) {
                        case 'BETWEEN':
                        case 'BEFORE': {
                            return true;
                        }
                        default:
                            return false;

                    }
                }

                vm.showTimeTo = function () {
                    switch (vm.timeCmp) {
                        case 'AFTER':
                        case 'BETWEEN':
                        case 'PRIOR_ONLY': {
                            return true;
                        }
                        default:
                            return false;
                    }
                }

                vm.showQtyTimeframeFrom = function () {
                    switch (vm.timeCmp) {
                        case 'EVER':
                        case 'IN_CURRENT':
                        case 'BETWEEN':
                        case 'BEFORE':
                        case 'AFTER': {
                            return false;
                        }
                        default:
                            return true;
                    }
                }
                vm.showQtyTimeframeTo = function () {
                    switch (vm.timeCmp) {
                        case 'BETWEEN_LT': {
                            return true;
                        }
                        default:
                            return false;
                    }
                }

                vm.changeTimeFramePeriod = function () {
                    QueryTreeService.changeTimeframePeriod($scope.bucketrestriction, $scope.type, vm.timeframePeriod);
                }

                vm.changeTimeCmp = function () {
                    QueryTreeService.changeCmp($scope.bucketrestriction, $scope.type, vm.timeCmp, 'Time');
                    $timeout(initDatePicker, 0);

                }

                function initDatePicker() {
                    var from = document.getElementById(vm.getFromId());

                    if (from != null) {
                        console.log('Found the picker');
                        var fromPicker = new Pikaday({ field: from });
                    }
                    var to = document.getElementById(vm.getToId());

                    if (to != null) {
                        console.log('Found the picker');
                        var toPicker = new Pikaday({ field: to });
                    }
                }
                $timeout(initDatePicker, 0);


                /*********************** Qty *************************/

                // vm.unitPurchasedCmpChoises = [
                //     { 'name': 'EQUAL', 'displayName': 'Equal' },
                //     { 'name': 'NOT_EQUAL', 'displayName': 'Not Equal' },
                //     { 'name': 'GREATER_THAN', 'displayName': 'Greater Than' },
                //     { 'name': 'GREATER_OR_EQUAL', 'displayName': 'Greater or Equal' },
                //     { 'name': 'LESS_THAN', 'displayName': 'Less Than' },
                //     { 'name': 'LESS_OR_EQUAL', 'displayName': 'Lesser or Equal' },
                //     { 'name': 'GTE_AND_LT', 'displayName': 'Between' },
                //     { 'name': 'ANY', 'displayName': 'Any' },
                // ];
                // var tmp = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, 'Qty');
                // vm.unitPurchasedCmp = tmp !== '' ? tmp : 'ANY';


                vm.showUnitFrom = function () {
                    switch (vm.unitPurchasedCmp) {
                        case 'EQUAL':
                        case 'GREATER_OR_EQUAL':
                        case 'GREATER_THAN':
                        case 'GTE_AND_LT':
                        case 'NOT_EQUAL':
                            {
                                return true;
                            }
                        default: return false;
                    }
                }
                vm.showUnitTo = function () {
                    switch (vm.unitPurchasedCmp) {
                        case 'GTE_AND_LT':
                        case 'LESS_OR_EQUAL':
                        case 'LESS_THAN': {
                            return true;
                        }
                        default: return false;
                    }
                }


                // vm.changeUnitPurchasedCmp = function () {
                //     QueryTreeService.changeCmp($scope.bucketrestriction, $scope.type, vm.unitPurchasedCmp, 'Qty');
                // }




                /************************ Amt *******************************/
                // vm.amountSpentCmpChoises = [
                //     { 'name': 'GREATER_THAN', 'displayName': 'Greater Than' },
                //     { 'name': 'GREATER_OR_EQUAL', 'displayName': 'Greater or Equal' },
                //     { 'name': 'LESS_THAN', 'displayName': 'Less Than' },
                //     { 'name': 'LESS_THAN', 'displayName': 'Lesser or Equal' },
                //     { 'name': 'GTE_AND_LT', 'displayName': 'Between' },
                //     { 'name': 'ANY', 'displayName': 'Any' },
                // ];

                // var tmpAmt = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, 'Amt');
                // vm.amtCmp = tmpAmt !== '' ? tmpAmt : 'ANY';
                console.log(vm.amtCmp);

                // vm.changeAmtCmp = function () {
                //     QueryTreeService.changeCmp($scope.bucketrestriction, $scope.type, vm.amtCmp, 'Amt');
                // }

                vm.showAmtFrom = function () {
                    switch (vm.amtCmp) {
                        case 'GTE_AND_LT':
                        case 'GREATER_THAN':
                        case 'GREATER_OR_EQUAL': {
                            return true;
                        }
                        default:
                            return false;

                    }

                }
                vm.showAmtTo = function () {
                    switch (vm.amtCmp) {
                        case 'LESS_THAN':
                        case 'LESS_THAN':
                        case 'GTE_AND_LT': {
                            return true;
                        }
                        default:
                            return false;

                    }
                }


            }
        }
    });