angular.module('lp.import.wizard.customfields', [])
.controller('ImportWizardCustomFields', function(
    $state, $stateParams, $scope, ResourceUtility, ImportWizardStore, FieldDocument
) {
    var vm = this;
    angular.extend(vm, {
        parent_name: $state.current.name.split('.')[3],
        full_name: $state.current.name,
        customFieldsIgnore: [],
        AvailableFields: [],
        ignoredFields: FieldDocument.ignoredFields = [],
        fieldMappings: FieldDocument.fieldMappings,
        fieldMappingIgnore: {}
    });

    vm.init = function() {
        vm.size= vm.AvailableFields.lengh;
        vm.customFields = ImportWizardStore.getCustomFields(vm.parent_name);

        vm.fieldMappings.forEach(function(item){
            if(item.mappedField == null) {
        	    vm.AvailableFields.push(item.userField);
            }
        });

        for( var i=0 ; i< vm.fieldMappings.length; i++) {
            if (vm.fieldMappings[i].mappedField == null) {
       	        vm.fieldMappings[i].mappedField = vm.fieldMappings[i].userField;
            }
        }
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
        for (var i =0 ; i < vm.AvailableFields.length; i++) {
            if (vm.AvailableFields[i] === input.userField) {
                return true;
            }
        }
        return false;
    };
    vm.init();
});