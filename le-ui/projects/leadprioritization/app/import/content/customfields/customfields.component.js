angular.module('lp.import.wizard.customfields', [])
.controller('ImportWizardCustomFields', function(
    $state, $stateParams, $scope, ResourceUtility, 
    ImportWizardStore, FieldDocument, mergedFieldDocument,
    ImportUtils
) {
    var vm = this;
    angular.extend(vm, {
        AvailableFields: [],
        ignoredFields: FieldDocument.ignoredFields || [],
        fieldMappings: FieldDocument.fieldMappings,
        mergedFields: mergedFieldDocument.main || mergedFieldDocument,
        fieldMappingIgnore: {},
        defaultsIgnored: []
    });

    vm.init = function() {
        vm.size = vm.AvailableFields.length;
        if(vm.mergedFields) {
            vm.mergedFields.forEach(function(item) {
                var appended = null;
                if(item.mappedField == null) {
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
            },0);
        }
    };

    function setDefaultIgnore(){
        vm.AvailableFields.forEach(function(element){
            
            var ignore = ImportUtils.isFieldInSchema(ImportWizardStore.getEntityType(), element.userField, vm.fieldMappings);
            if(ignore === true){
                var name = element.userField;
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
    }

    vm.changeType = function(fieldMapping) {
        for(var i in fieldMapping) {
            var userField = i,
                item = fieldMapping[userField];

            ImportWizardStore.remapType(userField, item.fieldType);
        }
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
    }

    vm.init();
});