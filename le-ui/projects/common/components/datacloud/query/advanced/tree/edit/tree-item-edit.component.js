angular.module('common.datacloud.query.builder.tree.edit', [])
    .directive('queryItemEditDirective', function () {
        return {
            restrict: 'E',
            scope: {
                vm: '='
            },
            require: 'ngModel',
            templateUrl: '/components/datacloud/query/advanced/tree/edit/tree-item-edit.component.html',
            controllerAs: 'vm',
            controller: function ($scope, $http, $timeout, QueryStore, QueryTreeService) {
                var vm = $scope.vm;
                vm.presetOperation;
                vm.booleanChanged = false;
                vm.showFromNumerical = false;
                vm.showToNumerical = false;
                vm.vals = vm.vals || [];
                vm.buckets = vm.buckets || [];
                vm.loading = vm.buckets.length === 0;
                vm.form = $scope.form || {};
                vm.header = ['VALUE', 'RECORDS'];
                vm.chipsOperations = ['EQUAL', 'IN_COLLECTION', 'NOT_EQUAL', 'NOT_IN_COLLECTION'];

                vm.init = function () {
                    console.log('[tree-edit] init start', vm.tree.bucketRestriction.bkt.Vals, vm.tree.bucketRestriction.bkt, vm);
                    vm.initVariables();
                    vm.resetCmp();
                    console.log('[tree-edit] init end', vm.tree.bucketRestriction.bkt.Vals, vm.tree.bucketRestriction.bkt, vm);
                }

                vm.initVariables = function () {
                    vm.string_operations = QueryTreeService.string_operations;
                    if (vm.showItem('Boolean') && !vm.showItem('Percent')) {
                        vm.booleanValue = QueryTreeService.getBooleanModel(vm.tree.bucketRestriction);
                    }
                    if (vm.showItem('String')) {
                        convertEqualToCollection();
                        let value = QueryTreeService.getOperationValue(vm.tree.bucketRestriction, 'String');
                        vm.operation = QueryTreeService.getStringCmpModel(vm.tree.bucketRestriction);
                        console.log('[tree-edit] initVariables()', value, vm.vals, vm.tree);
                        vm.vals = value;
                    }
                    if (vm.showItem('Enum')) {
                        convertEqualToCollection();
                        switch (vm.item.FundamentalType) {
                            case 'alpha':
                                vm.operation = QueryTreeService.getStringCmpModel(vm.tree.bucketRestriction);
                                break;
                            case 'numeric':
                            case 'enum':
                                vm.operation = QueryTreeService.getNumericalCmpModel(vm.tree.bucketRestriction);
                                vm.string_operations = QueryTreeService.numerical_operations;
                                initNumericalRange();
                                break;
                        }
                        vm.header[1] = vm.item.Entity + 's';
                        vm.vals = vm.tree.bucketRestriction.bkt.Vals;
                        vm.getBuckets();
                    }
                    if (vm.showItem('Numerical')) {
                        vm.operation = QueryTreeService.getNumericalCmpModel(vm.tree.bucketRestriction);
                        vm.vals = vm.tree.bucketRestriction.bkt.Vals;
                        initNumericalRange();
                    }
                    if (vm.showItem('Date')) { }
                };

                vm.changeBooleanValue = function ($event) {
                    vm.booleanChanged = true;

                    let value = $event.currentTarget.value;
                    let fn = (operation) => {
                        vm.tree.bucketRestriction.bkt.Cmp = vm.operation = operation;
                        QueryTreeService.changeCmpValue(vm.tree.bucketRestriction, vm.operation);
                        return '';
                    };

                    vm.booleanValue = value;

                    switch (value) {
                        case "Present":
                            value = fn('IS_NOT_NULL');
                            break;
                        case "Empty":
                            value = fn('IS_NULL');
                            break;
                        case "Yes":
                            fn('EQUAL');
                            break;
                        case "No":
                            fn('EQUAL');
                            break;
                    }

                    QueryTreeService.changeBooleanValue(vm.tree.bucketRestriction, value);
                }

                vm.checkBoolOpSelected = function (value) {
                    let bkt = vm.tree.bucketRestriction.bkt;

                    switch (bkt.Cmp) {
                        case "EQUAL": return value == (bkt.Vals.length > 0 ? bkt.Vals[0] : '');
                        case "IS_NULL": return value == 'Empty';
                        case "IS_NOT_NULL": return value == 'Present';
                    }

                    return false;
                }

                vm.changeVals = function () {
                    let bkt = vm.tree.bucketRestriction.bkt;
                    //console.log('[tree-edit] changeCmpValue start', vm.operation, vm.vals, bkt.Cmp, bkt.Vals);
                    QueryTreeService.changeVals(vm.tree.bucketRestriction, vm.vals);
                    // console.log('[tree-edit] changeVals end', vm.operation, vm.vals, bkt.Cmp, bkt.Vals);
                }

                vm.changeNumericalCmpValue = function () {
                    vm.changeCmpValue(true);
                    initNumericalRange(true);
                }

                vm.changeCmpValue = function (numerical) {
                    console.log('[tree-edit] changeCmpValue start', numerical, vm.operation, vm.vals, vm.tree.bucketRestriction.bkt.Vals);
                    vm.clear = true;
                    let _operation = vm.tree.bucketRestriction.bkt.Cmp;

                    switch (vm.operation) {
                        case 'EQUAL':
                        case 'IN_COLLECTION':
                            vm.operation = 'IN_COLLECTION';
                            break;
                        case 'NOT_EQUAL':
                        case 'NOT_IN_COLLECTION':
                            vm.operation = 'NOT_IN_COLLECTION';
                            break;
                        default:
                            if (vm.vals != '') {
                                vm.vals.length = 0;
                            }
                    }

                    if (vm.chipsOperations.indexOf(_operation) < 0 && vm.chipsOperations.indexOf(vm.operation) > -1) {
                        vm.vals.length = 0;
                    }

                    if (numerical) {
                        initNumericalRange(true);
                        QueryTreeService.changeNumericalCmpValue(vm.tree.bucketRestriction, vm.operation);
                    } else {
                        QueryTreeService.changeCmpValue(vm.tree.bucketRestriction, vm.operation);
                    }

                    console.log(3, vm.vals, vm.tree.bucketRestriction.bkt.Vals);
                }

                vm.showChips = function () {
                    let equals = ['EQUAL', 'IN_COLLECTION', 'NOT_EQUAL', 'NOT_IN_COLLECTION'];
                    let isPreset = vm.editMode == 'Preset';
                    let check = vm.showItem;
                    let cmp = equals.indexOf(vm.operation) > -1;
                    return !isPreset && cmp && (check('Enum') || check('Numerical') || check('String'));
                }

                vm.updateChips = function (items) {
                    // console.log('[tree-edit] updateChips start', vm.operation, items, vm.vals);
                    items = items || [];
                    vm.changeChips(items);
                    vm.changeVals();
                    vm.changeCmpValue(vm.item.FundamentalType == 'numeric');
                    // console.log('[tree-edit] updateChips end', vm.operation, items, vm.tree.bucketRestriction.bkt.Vals, vm.tree.bucketRestriction.bkt.Cmp, vm.tree.bucketRestriction);
                }

                vm.changeChips = function (items) {
                    // console.log('[tree-edit] changeChips start', vm.operation, vm.vals);
                    if (vm.clear) {
                        vm.vals.length = 0;
                        vm.clear = false;
                    }

                    items.forEach(item => {
                        if (vm.vals.indexOf(item.Lbl) < 0) {
                            vm.vals.push(item.Lbl);
                        }
                    });
                    // console.log('[tree-edit] changeChips end', vm.operation, vm.vals);
                }

                vm.getBuckets = function () {
                    var id = vm.item.ColumnId;
                    var entity = vm.item.Entity;

                    $http({
                        'url': '/pls/datacollection/statistics/attrs/' + entity + '/' + id,
                        method: 'GET'
                    }).then(function (response) {
                        vm.loading = false;
                        vm.buckets.length = 0;

                        if (response.data && response.data.Bkts && response.data.Bkts.List) {
                            response.data.Bkts.List.forEach(item => vm.buckets.push(item));
                        }

                        vm.clear = true;
                        vm.ChipsController.init();
                    });
                }

                vm.getOperationValue = function (operation, position) {
                    return QueryTreeService.getOperationValue(vm.tree.bucketRestriction, operation, position);
                }

                vm.getCubeBktList = function () {
                    return QueryTreeService.getCubeBktList(vm.tree.bucketRestriction, vm.item.cube);
                }

                vm.showInput = function (operation) {
                    let not_hidden = QueryTreeService.no_inputs.indexOf(operation) < 0;
                    let is_alpha = vm.checkAlphaNumeric(operation) === 'alpha';
                    return not_hidden && is_alpha;
                }

                vm.showNumericRange = function (operation) {
                    let not_hidden = QueryTreeService.no_inputs.indexOf(operation) < 0;
                    let is_numeric = vm.checkAlphaNumeric(operation) === 'numeric';
                    showNumericalRange(operation);
                    return not_hidden && is_numeric;
                }

                vm.checkAlphaNumeric = function (operation) {
                    return {
                        'STARTS_WITH': 'alpha',
                        'ENDS_WITH': 'alpha',
                        'CONTAINS': 'alpha',
                        'NOT_CONTAINS': 'alpha',
                        'GREATER_THAN': 'numeric',
                        'GREATER_OR_EQUAL': 'numeric',
                        'LESS_THAN': 'numeric',
                        'LESS_OR_EQUAL': 'numeric',
                        'GTE_AND_LT': 'numeric'
                    }[operation];
                }

                vm.showUnsetButton = function () {
                    return vm.root.mode === 'rules' || vm.root.mode === 'dashboardrules';
                }

                vm.showEmptyOption = function () {
                    return QueryTreeService.showEmptyOption(vm.tree.bucketRestriction);
                }

                vm.showItem = function (typeToShow) {
                    return QueryTreeService.showType(vm.tree.bucketRestriction, vm.type, typeToShow);
                }

                vm.showTo = function () {
                    return QueryTreeService.showTo(vm.tree.bucketRestriction);
                }

                vm.isValid = function () {
                    QueryStore.setPublicProperty('enableSaveSegmentButton', $scope.form.$valid === true);
                    return $scope.form.$valid;
                }

                vm.callbackChangedNumericalValue = function (type, position, value) {
                    QueryTreeService.changeValue(vm.tree.bucketRestriction, vm.type, value, position);
                }

                // hacky fix for vanishing operation value -lazarus
                vm.resetCmp = function () {
                    let operation = vm.operation;
                    vm.operation = '';
                    $timeout(() => vm.operation = operation, 16);
                }

                vm.clickSet = function ($event, unset) {
                    // console.log('[tree] clickSet() start', vm.tree.bucketRestriction.bkt.Cmp, vm.tree.bucketRestriction.bkt.Vals, vm.tree.bucketRestriction)

                    // add any free text in ChipsController query as an item
                    if (vm.ChipsController && vm.ChipsController.query && vm.ChipsController.customVals) {
                        vm.ChipsController.addCustomValue(vm.ChipsController.query);
                    }

                    // if vals is empty, set operation to check IF PRESENT
                    let vals = vm.tree.bucketRestriction.bkt.Vals || [];

                    if (['IS_NULL', 'IS_NOT_NULL'].indexOf(vm.operation) == -1 && vals.length == 0) {
                        vm.tree.bucketRestriction.bkt.Cmp = vm.operation = 'IS_NOT_NULL';

                        if (vm.item.FundamentalType == 'numeric') {
                            vm.changeNumericalCmpValue();
                        } else {
                            vm.changeCmpValue();
                        }
                    }

                    vm.editing = false;

                    if (unset) {
                        vm.unused = true;
                        vm.tree.bucketRestriction.bkt = {};
                        vm.tree.bucketRestriction.ignored = true;
                    } else {
                        vm.unused = false;
                        vm.tree.bucketRestriction.ignored = false;
                    }

                    vm.records_updating = true;

                    vm.root.updateCount();
                    vm.updateBucketCount();

                    $event.preventDefault();
                    $event.stopPropagation();

                    // console.log('[tree] clickSet() end', vm.tree.bucketRestriction.bkt.Cmp, vm.tree.bucketRestriction.bkt.Vals, vm.tree.bucketRestriction)
                }

                vm.clickEditMode = function (value) {
                    vm.editMode = value;
                    if (value !== 'Custom') {
                        var bucket = vm.getCubeBktList()[0];
                        if (bucket) {
                            vm.vm.editMode == 'Preset' == bucket.Lbl;
                        }
                        vm.changePreset(bucket);
                    } else {
                        QueryTreeService.changeCmpValue(vm.tree.bucketRestriction, vm.type);
                        vm.initVariables();
                    }
                }

                function initNumericalRange(reset) {
                    if (!reset) {
                        let fromNumerical = QueryTreeService.getValue(vm.tree.bucketRestriction, vm.type, 0);
                        let toNumerical = QueryTreeService.getValue(vm.tree.bucketRestriction, vm.type, 1);
                        let from = (fromNumerical != null) ? Number(fromNumerical) : undefined;
                        let to = (toNumerical != null) ? Number(toNumerical) : undefined;
                        vm.rangeConfig = {
                            from: { name: 'from-numerical', value: from, position: 0, type: 'Numerical', step: 1 },
                            to: { name: 'to-numerical', value: to, position: 1, type: 'Numerical', step: 1 }
                        };
                        showNumericalRange();
                    } else {
                        vm.rangeConfig = {
                            from: { name: 'from-numerical', value: undefined, position: 0, type: 'Numerical', step: 1 },
                            to: { name: 'to-numerical', value: undefined, position: 1, type: 'Numerical', step: 1 }
                        };
                        QueryTreeService.resetBktValues(vm.tree.bucketRestriction, vm.type);
                        setTimeout(() => { showNumericalRange() }, 0);
                    }

                    return vm.rangeConfig;
                }

                function showNumericalRange(operation) {
                    operation = operation || vm.operation;
                    switch (operation) {
                        case 'EQUAL':
                        case 'GREATER_OR_EQUAL':
                        case 'GREATER_THAN':
                        case 'NOT_EQUAL': {
                            vm.showFromNumerical = true;
                            vm.showToNumerical = false;
                            break;
                        }
                        case 'LESS_THAN':
                        case 'LESS_OR_EQUAL': {
                            vm.showFromNumerical = false;
                            vm.showToNumerical = true;
                            break;
                        }
                        case 'GTE_AND_LT': {
                            vm.showFromNumerical = true;
                            vm.showToNumerical = true;
                            break;
                        }
                        default: {
                            vm.showFromNumerical = false;
                            vm.showToNumerical = false;
                        }
                    }
                }

                function convertEqualToCollection() {
                    let map = {
                        'EQUAL': 'IN_COLLECTION',
                        'NOT_EQUAL': 'NOT_IN_COLLECTION'
                    };
                    let bucket = vm.tree.bucketRestriction.bkt;
                    bucket.Cmp = map[bucket.Cmp] || bucket.Cmp;
                }

                vm.init();
            }
        }
    });