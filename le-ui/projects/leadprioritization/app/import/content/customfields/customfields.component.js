angular.module('lp.import.wizard.customfields', [])
.controller('ImportWizardCustomFields', function(
    $state, $stateParams, $scope, ResourceUtility, ImportWizardStore, FieldDocument
) {
    var vm = this;
    angular.extend(vm, {
        AvailableFields: [],
        ignoredFields: FieldDocument.ignoredFields || [],
        fieldMappings: FieldDocument.fieldMappings,
        fieldMappingIgnore: {}
    });

    vm.init = function() {
        vm.size= vm.AvailableFields.length;

        vm.fieldMappings.forEach(function(item){
            if(item.mappedField == null) {
        	    vm.AvailableFields.push(item);
            }
        });
    };

    vm.toggleIgnores = function(checked, fieldMapping) {
        angular.element(".ignoreCheckbox").prop('checked', checked);
        for(var i in fieldMapping) {
            fieldMapping[i].ignore = checked;
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

    vm.filterStandardList = function(input) {
        if(input.mappedField) {
            return false;
        }
        return true;
    }

    vm.init();
});