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
            controller: function ($scope, $timeout, $state, DataCloudStore, QueryStore, QueryTreeService) {
                var vm = $scope.vm;
                vm.booleanChanged = false;
                

                function showNumericalRange() {
                    switch (vm.numericalCmpModel) {
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
                        };
                    }
                }

                function initNumericalRange(reset) {

                    if (!reset) {
                        showNumericalRange();
                        var fromNumerical = QueryTreeService.getValue(vm.tree.bucketRestriction, vm.type, vm.numericalConfiguration.from.position);
                        vm.numericalConfiguration.from.value = (fromNumerical != null && fromNumerical != 0) ? Number(fromNumerical) : undefined;

                        var toNumerical = QueryTreeService.getValue(vm.tree.bucketRestriction, vm.type, vm.numericalConfiguration.to.position);
                        vm.numericalConfiguration.to.value = (toNumerical != null && toNumerical != 0) ? Number(toNumerical) : undefined;
                    } else {
                        vm.showFromNumerical = false;
                        vm.showToNumerical = false;
                        vm.numericalConfiguration.from.value = undefined;
                        vm.numericalConfiguration.to.value = undefined;
                        QueryTreeService.resetBktValues(vm.tree.bucketRestriction, vm.type);
                        setTimeout(function () {
                            showNumericalRange();
                        }, 0);
                    }
                }

                vm.initVariables = function(){
                    vm.numericalConfiguration = {
                        from: { name: 'from-numerical', value: undefined, position: 0, type: 'Numerical' },
                        to: { name: 'to-numerical', value: undefined, position: 1, type: 'Numerical' }
                    };
    
                    vm.booleanValue = QueryTreeService.getBooleanModel(vm.tree.bucketRestriction);
                    vm.enumCmpModel = QueryTreeService.getEnumCmpModel(vm.tree.bucketRestriction);
                    vm.stringValue = QueryTreeService.getOperationValue(vm.tree.bucketRestriction, 'String');
                    vm.stringCmpModel = QueryTreeService.getStringCmpModel(vm.tree.bucketRestriction);
                    // vm.numericalCmpModel = QueryTreeService.getNumericalCmpModel(vm.tree.bucketRestriction);
                    vm.bktVals0 = QueryTreeService.getBktValue(vm.tree.bucketRestriction, 0);
                    vm.bktVals1 = QueryTreeService.getBktValue(vm.tree.bucketRestriction, 1);
                    vm.vals = vm.tree.bucketRestriction.bkt.Vals;

                    vm.string_operations = QueryTreeService.string_operations;
    
    
                    vm.showFromNumerical = false;
                    vm.showToNumerical = false;
                    if (QueryTreeService.showType(vm.tree.bucketRestriction, vm.type, 'Numerical')) {
                        vm.numericalCmpModel = QueryTreeService.getNumericalCmpModel(vm.tree.bucketRestriction);
                        initNumericalRange();

                    }
                }
                vm.init = function () {

                    vm.initVariables();
                }
                
                vm.init();

                vm.showInput = function(cmpModel) {
                    return QueryTreeService.no_inputs.indexOf(cmpModel) < 0;
                }


                vm.clickEditMode = function(value) {
                    vm.editMode = value;
                    if(value !== 'Custom'){
                        console.log('Preset');
                        var bucket = vm.getCubeBktList()[0]
                        vm.changePreset(bucket);
                    }else{
                        QueryTreeService.resetBktValues(vm.tree.bucketRestriction, vm.type);
                        vm.initVariables();
                    }
                }

                vm.showNumericalFrom = function () {
                    return vm.showFromNumerical;
                }

                vm.showNumericalTo = function () {
                    return vm.showToNumerical;
                }

                vm.getForm = function () {
                    return $scope.form;
                }

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
                    vm.booleanChanged = true;
                    QueryTreeService.changeBooleanValue(vm.tree.bucketRestriction, vm.booleanValue);
                }

                vm.changeEnumCmpValue = function () {
                    switch (vm.enumCmpModel) {
                        case 'is empty': 
                            vm.enumCmpModel = 'IS_NULL'; 
                            vm.vals.length = 0; 
                            break;
                        case 'is present': 
                            vm.enumCmpModel = 'IS_NOT_NULL'; 
                            vm.vals.length = 0; 
                            break;
                        case 'is': 
                            vm.enumCmpModel = vm.vals.length == 1 
                                ? 'EQUAL' 
                                : 'IN_COLLECTION'; 
                            break;
                        case 'is not': 
                            vm.enumCmpModel = vm.vals.length == 1 
                                ? 'NOT_EQUAL' 
                                : 'NOT_IN_COLLECTION';
                            break;
                    }

                    // if (vm.enumCmpModel == 'is empty') {
                    //     vm.enumCmpModel = 'IS_NULL';
                    //     vm.vals.length = 0;
                    // } else if (vm.enumCmpModel == 'is') {
                    //     vm.enumCmpModel = vm.vals.length == 1 ? 'EQUAL' : 'IN_COLLECTION';
                    // } else if (vm.enumCmpModel == 'is not') {
                    //     vm.enumCmpModel = vm.vals.length == 1 ? 'NOT_EQUAL' : 'NOT_IN_COLLECTION';
                    // }

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
                    initNumericalRange(true);
                }

                vm.changeStringValue = function () {
                    QueryTreeService.changeStringValue(vm.tree.bucketRestriction, vm.stringValue);
                }

                vm.changeStringCmpValue = function () {
                    QueryTreeService.changeStringCmpValue(vm.tree.bucketRestriction, vm.stringCmpModel);
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

                    var state = (vm.root.mode == 'rules')
                        ? 'home.ratingsengine.rulesprospects.segment.attributes.rules.picker'
                        : 'home.segment.explorer.enumpicker';

                    $state.go(state, { entity: vm.item.Entity, fieldname: vm.item.ColumnId });
                }

                vm.isValid = function () {
                    return $scope.form.$valid;
                }

                vm.showUsetButton = function(){
                    if(vm.root.mode === 'rules'){
                        return true;
                    } else {
                        return false;
                    }
                }

                //================= Numerical ==============================

                vm.getNumericalConfigString = function () {
                    return vm.numericalConfiguration;
                }

                vm.callbackChangedNumericalValue = function (type, position, value) {
                    QueryTreeService.changeValue(vm.tree.bucketRestriction, vm.type, value, position);
                }
            }
        }
    });