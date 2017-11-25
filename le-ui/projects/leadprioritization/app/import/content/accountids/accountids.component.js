angular.module('lp.import.wizard.accountids', [])
.controller('ImportWizardAccountIDs', function(
    $state, $stateParams, $scope, ResourceUtility, ImportWizardStore, FieldDocument, UnmappedFields
) {
    var vm = this;

    angular.extend(vm, {
        state: ImportWizardStore.getAccountIdState(),
        fieldMappings: FieldDocument.fieldMappings,
        fieldMappingsMap: {},
        AvailableFields: [],
        idFieldMapping: {"userField":"Id","mappedField":"Id","fieldType":"TEXT","mappedToLatticeField":true},
        Id: 'Id',
        UnmappedFieldsMappingsMap: {}
    });

    vm.init = function() {
        vm.UnmappedFields = UnmappedFields;

        ImportWizardStore.setUnmappedFields(UnmappedFields);

        vm.UnmappedFields.forEach(function(field) {
            vm.UnmappedFieldsMappingsMap[field.name] = field;
        });

        vm.fieldMappings.forEach(function(fieldMapping) {
            vm.fieldMappingsMap[fieldMapping.mappedField] = fieldMapping;
        });

        vm.fieldMappings.forEach(function(fieldMapping, index) {
            var userField = fieldMapping.userField;
            if(fieldMapping.mappedField != null) {
                vm.selectedIndex = index;
            }
            vm.AvailableFields.push(userField);
        });
    };

    vm.changeLatticeField = function(mapping) {
        ImportWizardStore.setSaveObjects([{userField: mapping.userField, mappedField: vm.Id}]);
        ImportWizardStore.setValidation('one', true);
    };

    vm.init();
});