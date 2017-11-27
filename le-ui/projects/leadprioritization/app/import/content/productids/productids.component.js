angular.module('lp.import.wizard.productids', [])
.controller('ImportWizardProductIDs', function(
    $state, $stateParams, $scope, ResourceUtility, ImportWizardStore, FieldDocument, UnmappedFields
) {
    var vm = this;

    angular.extend(vm, {
        state: ImportWizardStore.getAccountIdState(),
        fieldMapping: {},
        fieldMappings: FieldDocument.fieldMappings,
        fieldMappingsMap: {},
        AvailableFields: [],
        idFieldMapping: {"userField":"Id","mappedField":"Id","fieldType":"TEXT","mappedToLatticeField":true},
        mappedFieldMap: {
            product: 'productId',
        },
        UnmappedFieldsMappingsMap: {},
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
        var mapped = [];
        for(var i in mapping) {
            var key = i,
                item = mapping[key],
                map = {userField: item, mappedField: vm.mappedFieldMap[key]};

            mapped.push(map);
        }
        ImportWizardStore.setSaveObjects(mapped);
        if(mapped.length >= Object.keys(vm.mappedFieldMap).length) {
            ImportWizardStore.setValidation('one', true);
        }
    };

    vm.init();
});