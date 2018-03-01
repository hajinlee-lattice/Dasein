angular
    .module('common.datacloud.query.builder.tree.edit.transaction.edit', ['common.datacloud.query.builder.tree.edit.transaction.edit.numerical.range',
        'common.datacloud.query.builder.tree.transaction.service'])
    .directive('transactionEditDirective', function () {
        return {
            restrict: 'E',
            scope: {
                type: '=',
                bucketrestriction: '=',
                form: '='
            },
            templateUrl: '/components/datacloud/query/advanced/tree/transaction/transaction-item-edit.component.html',
            controllerAs: 'vm',
            controller: function ($scope, $timeout, $state, QueryTreeTransactionService, QueryTreeService) {
                var vm = $scope.vm;
                vm.type = $scope.type;
                vm.form = $scope.form;

                vm.bucketrestriction = $scope.bucketrestriction;
                /******************** Qty **************/
                vm.showFromUnit = false;
                vm.showToUnit = false;
                /***************************************/

                /******************* Amt ***************/
                vm.showFromAmt = false;
                vm.showToAmt = false;
                /***************************************/

                /************** Date Range ************/

                vm.showFromTime = false;
                vm.showToTime = false;
                /**************************************/

                /*********** Numerical range **********/
                vm.showFromPeriod = false;
                vm.showToPeriod = false;
                /**************************************/

                vm.qtyConf = QueryTreeTransactionService.getQtyConfig();
                vm.amtConf = QueryTreeTransactionService.getAmtConfig();
                vm.periodNumericalConf = QueryTreeTransactionService.getPeriodNumericalConfig();

                vm.cmpsList = QueryTreeTransactionService.getCmpsList()
                vm.periodList = QueryTreeTransactionService.periodList();
                vm.unitPurchasedCmpChoises = QueryTreeTransactionService.unitPurchasedCmpChoises();
                vm.amountSpentCmpChoises = QueryTreeTransactionService.amountSpentCmpChoises();

                function initQty(reset) {
                    // console.log($scope.bucketrestriction);
                    var tmpUnit = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, 'Qty');
                    vm.unitPurchasedCmp = tmpUnit !== '' ? tmpUnit : 'ANY';

                    if (!reset) {
                        var fromQty = QueryTreeService.getValue($scope.bucketrestriction, $scope.type, vm.qtyConf['from'].position, 'Qty');
                        vm.qtyConf['from'].value = (fromQty != null && fromQty != 0) ? Number(fromQty) : undefined;

                        var toQty = QueryTreeService.getValue($scope.bucketrestriction, $scope.type, vm.qtyConf['to'].position, 'Qty');
                        vm.qtyConf['to'].value = (toQty != null && toQty != 0) ? Number(toQty) : undefined;

                        vm.showFromUnit = vm.showUnitFrom();
                        vm.showToUnit = vm.showUnitTo();
                    } else {
                        vm.qtyConf['from'].value = undefined;
                        vm.qtyConf['to'].value = undefined;
                        vm.showFromUnit = false;
                        vm.showToUnit = false;
                        QueryTreeService.resetBktValues($scope.bucketrestriction, $scope.type, 'Qty');
                        setTimeout(function () {
                            vm.showFromUnit = vm.showUnitFrom();
                            vm.showToUnit = vm.showUnitTo();
                        }, 0);
                    }

                }

                function initAmt(reset) {
                    // console.log($scope.bucketrestriction);
                    var tmpAmt = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, 'Amt');
                    vm.amtCmp = tmpAmt !== '' ? tmpAmt : 'ANY';

                    vm.showFromAmt = false;
                    vm.showToAmt = false;

                    if (!reset) {
                        var fromAmt = QueryTreeService.getValue($scope.bucketrestriction, $scope.type, vm.amtConf['from'].position, 'Amt');
                        vm.amtConf['from'].value = fromAmt != 0 ? Number(fromAmt) : undefined;

                        var toAmt = QueryTreeService.getValue($scope.bucketrestriction, $scope.type, vm.amtConf['to'].position, 'Amt');
                        vm.amtConf['to'].value = toAmt != 0 ? Number(toAmt) : undefined;

                        vm.showFromAmt = vm.showAmtFrom();
                        vm.showToAmt = vm.showAmtTo();

                    } else {
                        vm.amtConf['from'].value = undefined;
                        vm.amtConf['to'].value = undefined;
                        QueryTreeService.resetBktValues($scope.bucketrestriction, $scope.type, 'Amt');
                        setTimeout(function () {
                            vm.showFromAmt = vm.showAmtFrom();
                            vm.showToAmt = vm.showAmtTo();
                        }, 0);
                    }
                }

                function initTime() {
                    var tmpTimeCmp = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, 'Time');
                    vm.timeCmp = tmpTimeCmp !== '' ? tmpTimeCmp : 'Month';
                }

                function initTimePeriod(reset) {


                    vm.showFromPeriod = false;
                    vm.showToPeriod = false;

                    if (!reset) {
                        var fromPeriod = QueryTreeService.getValue($scope.bucketrestriction, $scope.type, vm.periodNumericalConf['from'].position, 'Time');
                        vm.periodNumericalConf['from'].value = fromPeriod != 0 ? Number(fromPeriod) : undefined;

                        var toPeriod = QueryTreeService.getValue($scope.bucketrestriction, $scope.type, vm.periodNumericalConf['to'].position, 'Time');
                        vm.periodNumericalConf['to'].value = toPeriod != 0 ? Number(toPeriod) : undefined;

                        vm.showFromPeriod = vm.showPeriodFrom();
                        vm.showToPeriod = vm.showPeriodTo();

                    } else {
                        vm.periodNumericalConf['from'].value = undefined;
                        vm.periodNumericalConf['to'].value = undefined;
                        QueryTreeService.resetBktValues($scope.bucketrestriction, $scope.type, 'Time');
                        setTimeout(function () {
                            vm.showFromPeriod = vm.showPeriodFrom();
                            vm.showToPeriod = vm.showPeriodTo();
                        }, 0);
                    }
                }

                function removeKey(cmpValue, subType) {

                    switch (cmpValue) {
                        case 'ANY': {
                            // console.log('CMP value ', cmpValue);
                            QueryTreeService.removeKey($scope.bucketrestriction, $scope.type, subType);
                            break;
                        }
                    }
                }

                function initDatePicker() {
                    var from = document.getElementById(vm.getFromDateId());

                    if (from != null) {
                        var fromPicker = new Pikaday({ field: from });
                    }
                    var to = document.getElementById(vm.getToDateId());

                    if (to != null) {
                        var toPicker = new Pikaday({ field: to });
                    }
                }

                vm.init = function () {
                    // console.log(vm.form);
                    vm.values = {
                        time1: { val: undefined, position: 0, type: 'Time' },
                        time2: { val: undefined, position: 1, type: 'Time' }
                    }

                    var tmp = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, 'Time');
                    vm.timeCmp = tmp !== '' ? tmp : 'EVER';
                    var periodTmp = QueryTreeService.getPeriodValue($scope.bucketrestriction, $scope.type, 'Time');
                    vm.timeframePeriod = periodTmp !== '' ? periodTmp : vm.periodList[0].name;


                    initQty();
                    initAmt();
                    initTime();
                    initTimePeriod();
                }


                vm.getQtyConfigString = function () {
                    var ret = JSON.stringify(vm.qtyConf);
                    return ret;
                }

                vm.getAmtConfigString = function () {
                    var ret = JSON.stringify(vm.amtConf);
                    return ret;
                }
                vm.getPeriodNumericalConfString = function () {
                    var ret = JSON.stringify(vm.periodNumericalConf);
                    return ret;
                }

                vm.callbackChangedValue = function (type, position, value) {
                    QueryTreeService.changeValue($scope.bucketrestriction, $scope.type, value, position, type);
                }

                vm.changeCmp = function (value, type) {
                    // console.log('TYPE ==> ', type);
                    QueryTreeService.changeCmp($scope.bucketrestriction, $scope.type, value, type);

                    switch (type) {
                        case 'Qty': {
                            vm.showFromUnit = vm.showUnitFrom();
                            vm.showToUnit = vm.showUnitTo();
                            initQty(true);
                            removeKey(value, 'Qty');
                            break;
                        }
                        case 'Amt': {
                            vm.showFromAmt = vm.showAmtFrom();
                            vm.showToAmt = vm.showAmtTo();
                            initAmt(true);
                            removeKey(value, 'Amt');
                            break;
                        }
                        case 'Time': {
                            vm.showFromPeriod = vm.showPeriodFrom();
                            vm.showToPeriod = vm.showPeriodTo();
                            initTimePeriod(true);
                            $timeout(initDatePicker, 0);
                            // removeKey(value, 'Qty');
                            break;
                        }
                        default: {
                            vm.showFromUnit = vm.showUnitFrom();
                            vm.showToUnit = vm.showUnitTo();

                            vm.showFromAmt = vm.showAmtFrom();
                            vm.showToAmt = vm.showAmtTo();
                        }
                    }

                }

                //************************ Txn *********************/
                function getConfigField(position) {
                    // console.log('CONF ', vm.getPeriodNumericalConfString());
                    var values = JSON.parse(vm.getPeriodNumericalConfString());
                    var config = values[Object.keys(values)[position]];
                    return config;
                }
                vm.isPeriodRangeValid = function () {
                    var valid = true;
                    var confFrom = getConfigField(0);
                    var confTo = getConfigField(1);
                    if (vm.form[confFrom.name] && !vm.form[confFrom.name].$valid) {
                        valid = false;
                    }
                    if (vm.form[confTo.name] && !vm.form[confTo.name].$valid) {
                        valid = false;
                    }
                    return valid;
                }

                vm.getFromDateId = function () {
                    var id = $scope.bucketrestriction.attr;
                    return id + '.txn_from';
                }

                vm.getToDateId = function () {
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
                
                vm.showTimeFrameDate = function(){
                    switch (vm.timeCmp) {
                        case 'BETWEEN':
                        case 'BEFORE':
                        case 'AFTER': {
                            return true;
                        }
                        default:
                            return false;

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


                vm.showPeriodFrom = function () {
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
                vm.showPeriodTo = function () {
                    switch (vm.timeCmp) {
                        case 'AFTER':
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



                


                /*********************** Qty *************************/

                vm.showUnitFrom = function () {
                    switch (vm.unitPurchasedCmp) {
                        case 'EQUAL':
                        case 'GREATER_OR_EQUAL':
                        case 'GREATER_THAN':
                        case 'GTE_AND_LT':
                        case 'NOT_EQUAL': {
                            return true;
                        }
                        default: {
                            // vm.values.qty1.val = 1;
                            return false
                        };
                    }
                }
                vm.showUnitTo = function () {
                    switch (vm.unitPurchasedCmp) {
                        case 'GTE_AND_LT':
                        case 'LESS_OR_EQUAL':
                        case 'LESS_THAN': {

                            return true;
                        }
                        default: {
                            // vm.values.qty2.val = 1;
                            return false;
                        }
                    }
                }

                /************************ Amt *******************************/

                vm.showAmtFrom = function () {
                    switch (vm.amtCmp) {
                        case 'GREATER_OR_EQUAL':
                        case 'GREATER_THAN':
                        case 'GTE_AND_LT': {
                            return true;
                        }
                        default:
                            return false;
                    }
                }
                vm.showAmtTo = function () {
                    switch (vm.amtCmp) {
                        case 'GTE_AND_LT':
                        case 'LESS_OR_EQUAL':
                        case 'LESS_THAN': {
                            return true;
                        }
                        default:
                            return false;
                    }
                }
                vm.init();
            }
        }
    });