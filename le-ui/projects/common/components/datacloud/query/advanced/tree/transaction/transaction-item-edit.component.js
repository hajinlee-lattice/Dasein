angular
    .module('common.datacloud.query.builder.tree.edit.transaction.edit', [
        'common.datacloud.query.builder.tree.edit.transaction.edit.numerical.range',
        'common.datacloud.query.builder.tree.edit.transaction.edit.date.range',
        'common.datacloud.query.builder.tree.transaction.service', 'angularMoment'])
    .directive('transactionEditDirective', function () {
        return {
            restrict: 'E',
            scope: {
                type: '=',
                bucketrestriction: '=',
                form: '=',
                purchased: '=',
                booleanchanged: '='

            },
            templateUrl: '/components/datacloud/query/advanced/tree/transaction/transaction-item-edit.component.html',
            controllerAs: 'vm',
            controller: function ($scope, $timeout, moment, QueryTreeTransactionStore, QueryTreeService) {
                var vm = $scope.vm;
                vm.type = $scope.type;
                vm.form = $scope.form;
                vm.purchased = $scope.purchased;
                vm.moment = moment;

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
                vm.showTimeFrame = false;
                /**************************************/

                /*********** Numerical range **********/
                vm.showFromPeriod = false;
                vm.showToPeriod = false;
                /**************************************/

                vm.qtyConf = QueryTreeTransactionStore.getQtyConfig();
                vm.amtConf = QueryTreeTransactionStore.getAmtConfig();
                vm.periodNumericalConf = QueryTreeTransactionStore.getPeriodNumericalConfig();
                vm.periodTimeConf = QueryTreeTransactionStore.getPeriodTimeConfig();


                vm.cmpsList = QueryTreeTransactionStore.getCmpsList()
                vm.periodList = QueryTreeTransactionStore.periodList();
                vm.unitPurchasedCmpChoises = QueryTreeTransactionStore.unitPurchasedCmpChoises();
                vm.amountSpentCmpChoises = QueryTreeTransactionStore.amountSpentCmpChoises();


                function initQty(reset) {
                    var tmpUnit = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, 'Qty');
                    vm.unitPurchasedCmp = tmpUnit !== '' ? tmpUnit : 'ANY';

                    if (!reset) {
                        var fromQty = QueryTreeService.getValue($scope.bucketrestriction, $scope.type, vm.qtyConf.from.position, 'Qty');
                        vm.qtyConf.from.value = (fromQty != null && fromQty >= 0) ? Number(fromQty) : undefined;

                        var toQty = QueryTreeService.getValue($scope.bucketrestriction, $scope.type, vm.qtyConf.to.position, 'Qty');
                        vm.qtyConf.to.value = (toQty != null && toQty >= 0) ? Number(toQty) : undefined;

                        vm.showFromUnit = vm.showUnitFrom();
                        vm.showToUnit = vm.showUnitTo();
                    } else {
                        vm.qtyConf.from.value = undefined;
                        vm.qtyConf.to.value = undefined;
                        vm.showFromUnit = false;
                        vm.showToUnit = false;
                        QueryTreeService.resetBktValues($scope.bucketrestriction, $scope.type, 'Qty');
                        $timeout(function () {
                            vm.showFromUnit = vm.showUnitFrom();
                            vm.showToUnit = vm.showUnitTo();
                        }, 0);
                    }

                }

                function initAmt(reset) {
                    var tmpAmt = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, 'Amt');
                    vm.amtCmp = tmpAmt !== '' ? tmpAmt : 'ANY';

                    vm.showFromAmt = false;
                    vm.showToAmt = false;

                    if (!reset) {
                        var fromAmt = QueryTreeService.getValue($scope.bucketrestriction, $scope.type, vm.amtConf.from.position, 'Amt');
                        vm.amtConf.from.value = fromAmt >= 0 ? Number(fromAmt) : undefined;

                        var toAmt = QueryTreeService.getValue($scope.bucketrestriction, $scope.type, vm.amtConf.to.position, 'Amt');
                        vm.amtConf.to.value = toAmt >= 0 ? Number(toAmt) : undefined;

                        vm.showFromAmt = vm.showAmtFrom();
                        vm.showToAmt = vm.showAmtTo();

                    } else {
                        vm.amtConf.from.value = undefined;
                        vm.amtConf.to.value = undefined;
                        QueryTreeService.resetBktValues($scope.bucketrestriction, $scope.type, 'Amt');

                        $timeout(function () {
                            vm.showFromAmt = vm.showAmtFrom();
                            vm.showToAmt = vm.showAmtTo();
                        }, 0);
                    }
                }

                function initTime() {
                    var tmpTimeCmp = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, 'Time');
                    vm.timeCmp = tmpTimeCmp !== '' ? tmpTimeCmp : 'Month';
                }

                function setInitialValueNumericalPeriod() {
                    if (vm.showPeriodFrom() && vm.showPeriodTo()) {
                        vm.periodNumericalConf.from.value = 1;
                        vm.periodNumericalConf.to.value = 2;
                        QueryTreeService.changeValue($scope.bucketrestriction, $scope.type, vm.periodNumericalConf.from.value, vm.periodNumericalConf.from.position, 'Time');
                        QueryTreeService.changeValue($scope.bucketrestriction, $scope.type, vm.periodNumericalConf.to.value, vm.periodNumericalConf.to.position, 'Time');
                    } else if (vm.showPeriodFrom() && !vm.showPeriodTo()) {
                        vm.periodNumericalConf.from.value = 1;
                        vm.periodNumericalConf.to.value = undefined;
                        QueryTreeService.resetBktValues($scope.bucketrestriction, $scope.type, 'Time');
                        QueryTreeService.changeValue($scope.bucketrestriction, $scope.type, vm.periodNumericalConf.from.value, vm.periodNumericalConf.from.position, 'Time');
                    } else if (!vm.showPeriodFrom() && vm.showPeriodTo()) {
                        vm.periodNumericalConf.from.value = undefined;
                        vm.periodNumericalConf.to.value = 1;
                        QueryTreeService.resetBktValues($scope.bucketrestriction, $scope.type, 'Time');
                        QueryTreeService.changeValue($scope.bucketrestriction, $scope.type, vm.periodNumericalConf.to.value, vm.periodNumericalConf.to.position, 'Time');
                    } else {
                        vm.periodNumericalConf.from.value = undefined;
                        vm.periodNumericalConf.to.value = undefined;
                        QueryTreeService.resetBktValues($scope.bucketrestriction, $scope.type, 'Time');
                    }
                }

                function initTimePeriod(reset) {
                    vm.showFromPeriod = false;
                    vm.showToPeriod = false;
                    vm.showTimeFrame = false;

                    if (!reset) {
                        var fromPeriod = QueryTreeService.getValue($scope.bucketrestriction, $scope.type, vm.periodNumericalConf.from.position, 'Time');
                        vm.periodNumericalConf.from.value = Number(fromPeriod);

                        var toPeriod = QueryTreeService.getValue($scope.bucketrestriction, $scope.type, vm.periodNumericalConf.to.position, 'Time');
                        vm.periodNumericalConf.to.value = toPeriod != 0 ? Number(toPeriod) : undefined;

                        vm.showFromPeriod = vm.showPeriodFrom();
                        vm.showToPeriod = vm.showPeriodTo();
                        vm.showTimeFrame = vm.showTimeFrameDate();

                    } else {
                        vm.showFromTime = false;
                        vm.showToTime = false;
                        vm.showTimeFrame = false;
                        setInitialValueNumericalPeriod();
                        $timeout(function () {
                            vm.showFromPeriod = vm.showPeriodFrom();
                            vm.showToPeriod = vm.showPeriodTo();
                            vm.showTimeFrame = vm.showTimeFrameDate();
                        }, 0);
                    }
                }
                function initDateRange() {
                    if (QueryTreeService.getPeriodValue($scope.bucketrestriction, $scope.type, 'Time') === 'Date') {
                        var tmpFrom = QueryTreeService.getValue($scope.bucketrestriction, $scope.type, 0, 'Time');
                        vm.periodTimeConf.from.initial = (tmpFrom != undefined && tmpFrom != 0) ? vm.moment(tmpFrom).format('YYYY-MM-DD') : undefined;
                        var tmpTo = QueryTreeService.getValue($scope.bucketrestriction, $scope.type, 1, 'Time');
                        vm.periodTimeConf.to.initial = (tmpTo != undefined && tmpTo != 0) ? vm.moment(tmpTo).format('YYYY-MM-DD') : undefined;
                        // console.log('+++++++++++++++++++++> fromDate ', vm.fromDate, 'toDate ', vm.toDate);
                    } else {
                        // console.log('----------->NO DATE <------------');
                        vm.periodTimeConf.from.initial = undefined;
                        vm.periodTimeConf.to.initial = undefined;
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

                vm.init = function () {

                    var tmp = QueryTreeService.getCmp($scope.bucketrestriction, $scope.type, 'Time');
                    vm.timeCmp = tmp !== '' ? tmp : 'EVER';
                    var periodTmp = QueryTreeService.getPeriodValue($scope.bucketrestriction, $scope.type, 'Time');
                    vm.timeframePeriod = periodTmp !== '' ? periodTmp : vm.periodList[0].name;

                    initQty();
                    initAmt();
                    initTime();
                    initTimePeriod();
                }

                vm.showSubTypeSelection = function (subType) {
                    if ($scope.booleanchanged === true) {
                        QueryTreeService.removeKey($scope.bucketrestriction, $scope.type, 'Qty');
                        QueryTreeService.removeKey($scope.bucketrestriction, $scope.type, 'Amt');
                        $scope.booleanchanged = false;
                    }
                    switch (subType) {
                        case 'Amt':
                        case 'Qty': {
                            if ($scope.purchased == 'Yes') {
                                return true;
                            } else {
                                return false;
                            }
                        }
                        default:
                            return true;
                    }
                }

                vm.getQtyConfigString = function () {
                    var ret = vm.qtyConf;
                    return ret;
                }

                vm.getAmtConfigString = function () {
                    var ret = vm.amtConf;
                    return ret;
                }
                vm.getPeriodNumericalConfString = function () {
                    var ret = vm.periodNumericalConf;
                    return ret;
                }
                vm.getPeriodTimeConfString = function () {
                    initDateRange();
                    vm.periodTimeConf.from.visible = vm.showTimeFrom();
                    vm.periodTimeConf.to.visible = vm.showTimeTo();
                    var ret = vm.periodTimeConf;
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
                            initTimePeriod(true);
                            if (vm.showTimeFrameDate()) {
                                vm.timeframePeriod = 'Date';
                                vm.changeTimeFramePeriod();
                            } else {
                                vm.timeframePeriod = vm.periodList[0].name;
                                vm.changeTimeFramePeriod();
                            }
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
                    var values = vm.getPeriodNumericalConfString();
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

                vm.getErrorMsg = function () {
                    if (vm.showPeriodFrom() && vm.showPeriodTo()) {
                        return 'Enter a valid range';
                    } else {
                        return 'Enter a valid number';
                    }
                }

                vm.showTimeframePeriod = function () {
                    switch (vm.timeCmp) {
                        case 'EVER':
                        case 'BETWEEN_DATE':
                        case 'BEFORE':
                        case 'AFTER': {
                            return false;
                        }
                        default:
                            return true;

                    }
                }

                vm.showTimeFrameDate = function () {
                    switch (vm.timeCmp) {
                        case 'BETWEEN_DATE':
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
                        case 'BETWEEN_DATE':
                        case 'AFTER': {
                            return true;
                        }
                        default:
                            return false;

                    }
                }

                vm.showTimeTo = function () {
                    switch (vm.timeCmp) {
                        case 'BEFORE':
                        case 'BETWEEN_DATE':
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
                        case 'IN_CURRENT_PERIOD':
                        case 'BETWEEN_DATE':
                        case 'BEFORE':
                        case 'AFTER':
                        case 'WITHIN':
                        case 'PRIOR_ONLY': {
                            return false;
                        }
                        default:
                            return true;
                    }
                }
                vm.showPeriodTo = function () {
                    switch (vm.timeCmp) {
                        case 'BETWEEN':
                        case 'WITHIN':
                        case 'PRIOR_ONLY': {
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