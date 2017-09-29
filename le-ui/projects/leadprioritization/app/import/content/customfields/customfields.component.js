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
    });

    vm.init = function() {
        vm.size= vm.AvailableFields.lengh;
        vm.customFields = ImportWizardStore.getCustomFields(vm.parent_name);

        vm.fieldMappings.forEach(function(item){
            if(item.mappedField == null) {
        	    vm.AvailableFields.push(item.userField);
            }
        });
        vm.customFields.forEach(function(item){
            vm.customFieldsIgnore[item.CustomField] = false;
        });

        vm.toggleIgnores = function($event) {
            var target = $event.target;
            vm.customFields.forEach(function(item){
                vm.customFieldsIgnore[item.CustomField] = target.checked;
            });
        }

        for( var i=0 ; i< vm.fieldMappings.length; i++) {
            if (vm.fieldMappings[i].mappedField == null) {
       	        vm.fieldMappings[i].mappedField = vm.fieldMappings[i].userField;
            }
        }
    };

    vm.changeIgnore = function(fieldMapping) {
         vm.fieldMappings.forEach(function(fieldMapping) {
             if (fieldMapping.ignored) {
                 vm.ignoredFields.push(fieldMapping.userField);
                 delete fieldMapping.ignored;
             }
         });
        ImportWizardStore.setFieldDocument(FieldDocument);
    }
    vm.changeType = function(fieldMapping) {
        ImportWizardStore.setFieldDocument(FieldDocument);
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