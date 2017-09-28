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

        vm.fieldMappings.forEach(function(fieldMapping) {
            var userField = fieldMapping.userField;
            vm.AvailableFields.push(userField);
        });

    };

    vm.changeLatticeField = function(mapping) {
        if (vm.fieldMappingsMap[vm.Id]) {
	        vm.fieldMappingsMap[vm.Id].userField = mapping.userField;
	        vm.fieldMappingsMap[vm.Id].mappedToLatticeField = true;
        }
        vm.AvailableFields = vm.AvailableFields.filter(function(item){
            return item !== mapping.userField;
        });
        ImportWizardStore.setAvailableFields(vm.AvailableFields);
        ImportWizardStore.setFieldDocument(FieldDocument);
    };

    vm.init();
});