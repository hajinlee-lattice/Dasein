import { format } from "path";

angular.module('lp.import.wizard.customfields', [])
.controller('ImportWizardCustomFields', function(
    $state, $stateParams, $scope, ResourceUtility, 
    ImportWizardStore, FieldDocument, mergedFieldDocument,
    ImportUtils, $transition$
) {
    var vm = this;
    var alreadySaved = ImportWizardStore.getSavedDocumentFields($state.current.name),
        extraFieldMappingInfo = FieldDocument.extraFieldMappingInfo;

    if(alreadySaved){
        FieldDocument.fieldMappings = alreadySaved;
    }else{
        var from = $transition$._targetState._definition.parent.name;
        FieldDocument.fieldMappings = ImportWizardStore.getSavedDocumentFields(from);
    }
    angular.extend(vm, {
        AvailableFields: [],
        ignoredFields: FieldDocument.ignoredFields || [],
        fieldMappings: FieldDocument.fieldMappings,
        mergedFields: mergedFieldDocument.main || mergedFieldDocument,
        fieldMappingIgnore: {},
        defaultsIgnored: [],
        fieldDateTooltip: ImportWizardStore.tooltipDateTxt,
        extraFieldMappingInfo: extraFieldMappingInfo
    });
    // console.log('STORE TOOLTIP ', vm.fieldDateTooltip)
    vm.getTooltip = () => {
        // console.log(vm.fieldDateTooltip);
        return vm.fieldDateTooltip;
    }
    vm.getToolTipDate = () => {
        // console.log(vm.fieldDateTooltip);
        return vm.fieldDateTooltip;
    }
    vm.init = function() {
        vm.size = vm.AvailableFields.length;
        if(vm.fieldMappings) { //vm.mergedFields This was the original code
            vm.fieldMappings.forEach(function(item) {
                var appended = null;
                if(!item.mappedToLatticeField) {
                    if(mergedFieldDocument.appended) {
                        appended = mergedFieldDocument.appended.find(function(dup) {
                            return (item.userField === dup.userField);
                        });
                    }
                    if(appended) {
                        vm.AvailableFields.push(appended);
                    } else {
                        vm.AvailableFields.push(item);
                    }
                }
            });
            setTimeout(function(){
                setDefaultIgnore();
                vm.validate();
            },250);
        }
    };

    function setDefaultIgnore(){
        vm.AvailableFields.forEach(function(element){
            
            var ignore = ImportUtils.isFieldInSchema(ImportWizardStore.getEntityType(), element.userField, vm.fieldMappings);
            if(ignore == false){
                ignore = ImportUtils.isFieldInSchema(ImportWizardStore.getEntityType(), element.userField, ImportWizardStore.fieldDocument.fieldMappings);
            }
            var name = element.userField;
            if(ignore === true && $scope.fieldMapping[name]){
                // console.log(name);
                $scope.fieldMapping[name].ignore = true;
                element.defaultIgnored = true;
                vm.defaultsIgnored.push(element);
                vm.changeIgnore($scope.fieldMapping);
            }
        });
        setTimeout(function(){
            $scope.$apply();
        },100);

    }

    vm.toggleIgnores = function(checked, fieldMapping) {
        // angular.element('.ignoreCheckbox').prop('checked', checked);
        for(var i in fieldMapping) {
            var blocked = ImportUtils.isFieldInSchema(ImportWizardStore.getEntityType(), i, vm.fieldMappings);
            if(!blocked){
                fieldMapping[i].ignore = checked;
            }
        }
        vm.changeIgnore(fieldMapping);
    }

    vm.changeIgnore = function(fieldMapping) {
        var ignoredFields = [];
        for(var i in fieldMapping) {
            var userField = i,
                item = fieldMapping[userField],
                ignore = item.ignore;
            if(ignore) {
                ignoredFields.push(userField);
            }
        }
        ImportWizardStore.setIgnore(ignoredFields);
        vm.validate();
    }

    vm.changeType = function(fieldMappings, field) {
        for(var i in fieldMappings) {
            var userField = i,
                item = fieldMappings[userField];
        }
        field.fieldType = fieldMappings[field.userField].fieldType;
        vm.changeSingleType(field);
    }
    vm.changeSingleType = function(fieldMapping){
        let userField = fieldMapping.userField;
        ImportWizardStore.remapType(userField, {type: fieldMapping.fieldType, dateFormatString: fieldMapping.dateFormatString, timeFormatString: fieldMapping.timeFormatString, timezone: fieldMapping.timezone}, ImportWizardStore.getEntityType());
        ImportWizardStore.userFieldsType[userField] = {type: fieldMapping.fieldType, dateFormatString: fieldMapping.dateFormatString, timeFormatString: fieldMapping.timeFormatString, timezone: fieldMapping.timezone};
        vm.validate();
    }
    
    vm.updateFormats = (formats) => {
        let field = formats.field;
        ImportWizardStore.userFieldsType[field.userField] = {type: field.fieldType, dateFormatString: formats.dateformat, timeFormatString: formats.timeformat, timezone: formats.timezone};
        vm.validate();
    }

    vm.getNumberDroppedFields = function(){
        if(vm.defaultsIgnored && vm.defaultsIgnored != null){
            return vm.defaultsIgnored.length;
        } else{
            return 0;
        }
        
    }

    vm.filterStandardList = function(input) {
        if(input.mappedField) {
            return false;
        }
        return true;
    };

    vm.validate = function() {
        // console.log(vm.form.$valid);
        if(vm.form){
            setTimeout(() => {
                ImportWizardStore.setValidation('customfields', vm.form.$valid);
                $scope.$apply();
            }, 100);
            
        }
    }
    vm.getDateFormatFromLatticeSchema = (mappedField) => {
        let fieldSchema = ImportUtils.getFieldFromLaticeSchema(ImportWizardStore.getEntityType(), mappedField);
        if(fieldSchema){
            return fieldSchema.dateFormatString;
        }else{
            return '';
        }
    }
    vm.getTimeFormatFromLatticeSchema = (mappedField) => {
        let fieldSchema = ImportUtils.getFieldFromLaticeSchema(ImportWizardStore.getEntityType(), mappedField);
        if(fieldSchema){
            return fieldSchema.timeFormatString;
        }else{
            return '';
        }
    }
    vm.getTimezoneFromLatticeSchema = (mappedField) => {
        let fieldSchema = ImportUtils.getFieldFromLaticeSchema(ImportWizardStore.getEntityType(), mappedField);
        if(fieldSchema){
            return fieldSchema.timezone;
        }else{
            return '';
        }
    }
    vm.init();
});