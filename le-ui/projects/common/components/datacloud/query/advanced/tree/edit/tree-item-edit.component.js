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
                vm.presetOperation;
                vm.showFromNumerical = false;
                vm.showToNumerical = false;
                // console.log(vm.tree.bucketRestriction);

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
                        }
                    }
                }

                function initNumericalRange(reset) {
                    if (!reset) {
                        
                        var fromNumerical = QueryTreeService.getValue(vm.tree.bucketRestriction, vm.type, 0);
                        let from = (fromNumerical != null) ? Number(fromNumerical) : undefined;
                        var toNumerical = QueryTreeService.getValue(vm.tree.bucketRestriction, vm.type, 1);
                        let to = (toNumerical != null) ? Number(toNumerical) : undefined;
                        vm.numericalConfiguration = {
                            from: { name: 'from-numerical', value: from, position: 0, type: 'Numerical' },
                            to: { name: 'to-numerical', value: to, position: 1, type: 'Numerical' }
                        };
                        showNumericalRange();
                        
                    } else {
                        vm.showFromNumerical = false;
                        vm.showToNumerical = false;
                        vm.numericalConfiguration = {
                            from: { name: 'from-numerical', value: undefined, position: 0, type: 'Numerical' },
                            to: { name: 'to-numerical', value: undefined, position: 1, type: 'Numerical' }
                        };
                        QueryTreeService.resetBktValues(vm.tree.bucketRestriction, vm.type);
                        showNumericalRange();
                    }
                }

                vm.initVariables = function(){
                    if(vm.showItem('Boolean') && !vm.showItem('Percent')){
                        vm.booleanValue = QueryTreeService.getBooleanModel(vm.tree.bucketRestriction);
                    }
                    if(vm.showItem('String')){
                        vm.stringValue = QueryTreeService.getOperationValue(vm.tree.bucketRestriction, 'String');
                        vm.stringCmpModel = QueryTreeService.getStringCmpModel(vm.tree.bucketRestriction);
                    }
                    if(vm.showItem('Enum')){
                        vm.enumCmpModel = QueryTreeService.getEnumCmpModel(vm.tree.bucketRestriction);
                        vm.vals = vm.tree.bucketRestriction.bkt.Vals;
                    }
                    if(vm.showItem('Numerical')){
                        vm.numericalCmpModel = QueryTreeService.getNumericalCmpModel(vm.tree.bucketRestriction);
                        vm.showFromNumerical = false;
                        vm.showToNumerical = false;
                        vm.numericalCmpModel = QueryTreeService.getNumericalCmpModel(vm.tree.bucketRestriction);
                        // setTimeout(() => {
                            initNumericalRange();
                        // });

                    }
                    if(vm.showItem('Date')){
                        // console.log('=== DATE ===');
                    }

                    vm.string_operations = QueryTreeService.string_operations;
                    // console.log(vm.string_operations);
                   
                };

                vm.init = function () {
                    // console.log('INIT ',vm.tree.bucketRestriction);
                    setTimeout(() => {
                        vm.initVariables();
                    },0);
                    
                }
                
                vm.init();

                vm.showInput = function(cmpModel) {
                    return QueryTreeService.no_inputs.indexOf(cmpModel) < 0;
                }


                vm.clickEditMode = function(value) {
                    vm.editMode = value;
                    if(value !== 'Custom'){
                        var bucket = vm.getCubeBktList()[0];
                        if(bucket){
                            vm.presetOperation = bucket.Lbl;
                        }
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
                        case 'IS_NULL':
                        case 'IS_NOT_NULL': 
                            vm.vals.length = 0;
                            break;
                    }
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

                    var state = vm.root.mode == 'rules'
                        ? 'home.ratingsengine.rulesprospects.segment.attributes.rules.picker'
                        : (vm.root.mode == 'dashboardrules'
                            ? 'home.ratingsengine.dashboard.segment.attributes.rules.picker'
                            : 'home.segment.explorer.enumpicker');
                    
                    $state.go(state, { entity: vm.item.Entity, fieldname: vm.item.ColumnId });
                }

                vm.isValid = function () {
                    if($scope.form.$valid === true){
                        QueryStore.setPublicProperty('enableSaveSegmentButton', true);
                    }else {
                        QueryStore.setPublicProperty('enableSaveSegmentButton', false);
                    }
                    return $scope.form.$valid;
                }

                vm.showUnsetButton = function(){
                    if(vm.root.mode === 'rules' || vm.root.mode === 'dashboardrules'){
                        return true;
                    } else {
                        return false;
                    }
                }

                vm.getEntity = function(){
                    return vm.item.Entity;
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