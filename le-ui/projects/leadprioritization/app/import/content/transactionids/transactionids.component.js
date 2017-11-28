angular.module('lp.import.wizard.transactionids', [])
.controller('ImportWizardTransactionIDs', function(
    $state, $stateParams, $scope, ResourceUtility, ImportWizardStore, FieldDocument, UnmappedFields
) {
    var vm = this;

    angular.extend(vm, {
        state: ImportWizardStore.getAccountIdState(),
        fieldMapping: {},
        fieldMappings: FieldDocument.fieldMappings,
        fieldMappingsMap: {},
        AvailableFields: [],
        unavailableFields: [],
        idFieldMapping: {"userField":"Id","mappedField":"Id","fieldType":"TEXT","mappedToLatticeField":true},
        mappedFieldMap: {
            product: 'ProductId',
            contact: 'ContactId',
            account: 'AccountId',
        },
        UnmappedFieldsMappingsMap: {},
    });

    vm.init = function() {
        vm.UnmappedFields = UnmappedFields;

        ImportWizardStore.setUnmappedFields(UnmappedFields);

        vm.fieldMappings.forEach(function(fieldMapping, index) {
            vm.fieldMappingsMap[fieldMapping.mappedField] = fieldMapping;
            vm.AvailableFields.push(fieldMapping);
            for(var i in vm.mappedFieldMap) {
                if(fieldMapping.mappedField == vm.mappedFieldMap[i]) {
                    vm.fieldMapping[i] = fieldMapping.userField
                }
            }
        });
        checkValidation();
    };

    vm.changeLatticeField = function(mapping) {
        var mapped = [];
        vm.unavailableFields = [];
        for(var i in mapping) {
            var key = i,
                item = mapping[key],
                map = {userField: item, mappedField: vm.mappedFieldMap[key]};

            mapped.push(map);
            vm.unavailableFields.push(item)
        }
        ImportWizardStore.setSaveObjects(mapped);
        checkValidation();
    };

    var checkValidation = function() {
        if(Object.keys(vm.fieldMapping).length >= Object.keys(vm.mappedFieldMap).length) {
             ImportWizardStore.setValidation('one', true);
        }
    }

    vm.init();
});