angular.module('lp.import.wizard.contactids', [])
.controller('ImportWizardContactIDs', function(
    $state, $stateParams, $scope, $timeout, ResourceUtility, ImportWizardStore, FieldDocument, UnmappedFields, FeatureFlagService
) {
    var vm = this;

    var entityMatchEnabled = ImportWizardStore.entityMatchEnabled;

    angular.extend(vm, {
        state: ImportWizardStore.getAccountIdState(),
        fieldMapping: {},
        fieldMappings: FieldDocument.fieldMappings,
        fieldMappingsMap: {},
        AvailableFields: [],
        unavailableFields: [],
        idFieldMapping: {"userField":"Id","mappedField":"Id","fieldType":"TEXT","mappedToLatticeField":true},
        mappedFieldMap: {
            contact: (entityMatchEnabled ? 'CustomerContactId' : 'ContactId'),
            account: (entityMatchEnabled ? 'CustomerAccountId' : 'AccountId')
        },
        UnmappedFieldsMappingsMap: {},
        savedFields: ImportWizardStore.getSaveObjects($state.current.name),
        initialMapping: {},
        keyMap: {},
        saveMap: {},
        entityMatchEnabled: entityMatchEnabled
    });

    vm.init = function() {
        vm.UnmappedFields = UnmappedFields;

        ImportWizardStore.setUnmappedFields(UnmappedFields);
        ImportWizardStore.setValidation('ids', false);

        var userFields = [];
        vm.fieldMappings.forEach(function(fieldMapping, index) {
            vm.fieldMappingsMap[fieldMapping.mappedField] = fieldMapping;
            if(userFields.indexOf(fieldMapping.userField) === -1) {
                userFields.push(fieldMapping.userField);
                vm.AvailableFields.push(fieldMapping);
            }
            for(var i in vm.mappedFieldMap) {
                if(fieldMapping.mappedField == vm.mappedFieldMap[i]) {
                    vm.fieldMapping[i] = fieldMapping.userField;
                }
            }
        });
        if(vm.savedFields) {
            vm.savedFields.forEach(function(fieldMapping, index) {
                vm.saveMap[fieldMapping.originalMappedField] = fieldMapping;

                vm.fieldMappingsMap[fieldMapping.mappedField] = fieldMapping;
                if(userFields.indexOf(fieldMapping.userField) === -1) {
                    userFields.push(fieldMapping.userField);
                    vm.AvailableFields.push(fieldMapping);
                }
                for(var i in vm.mappedFieldMap) {
                    if(fieldMapping.mappedField == vm.mappedFieldMap[i]) {
                        vm.fieldMapping[i] = fieldMapping.userField
                    }
                }
            });
        }
        vm.AvailableFields = vm.AvailableFields.filter(function(item) {
            return (item.userField);
        });
    };

    vm.changeLatticeField = function(mapping, form) {
        var mapped = [];
        vm.unavailableFields = [];
        for(var i in mapping) {
            if(mapping[i] || mapping[i] === "") {
                var key = i,
                    userField = mapping[key],
                    map = {
                        userField: userField, 
                        mappedField: vm.mappedFieldMap[key],
                        // removing the following 3 lines makes it update instead of append
                        originalUserField: (vm.saveMap[vm.mappedFieldMap[key]] ? vm.saveMap[vm.mappedFieldMap[key]].originalUserField : vm.keyMap[vm.mappedFieldMap[key]]),
                        originalMappedField: (vm.saveMap[vm.mappedFieldMap[key]] ? vm.saveMap[vm.mappedFieldMap[key]].originalMappedField : vm.mappedFieldMap[key]),
                        append: false
                    };
                mapped.push(map);
                if(userField) {
                    vm.unavailableFields.push(userField);
                }
            }
        }
        ImportWizardStore.setSaveObjects(mapped, $state.current.name);
        vm.checkValid(form);
    };

    vm.checkFieldsDelay = function(form) {
        var mapped = [];
        $timeout(function() {
            for(var i in vm.fieldMapping) {
                var key = i,
                    userField = vm.fieldMapping[key];

                vm.keyMap[vm.mappedFieldMap[key]] = userField;
                vm.initialMapping[key] = userField;
                if(userField) {
                    vm.unavailableFields.push(userField);
                }
            }
        }, 1);
    }

    vm.checkValidDelay = function(form) {
        $timeout(function() {
            vm.checkValid(form);
        }, 1);
    };

    vm.checkValid = function(form) {
        ImportWizardStore.setValidation('ids', form.$valid);
    }


    //

    vm.isMultipleTemplates = () => {
        var flags = FeatureFlagService.Flags();
        var multipleTemplates = FeatureFlagService.FlagIsEnabled(flags.ENABLE_MULTI_TEMPLATE_IMPORT);
        return multipleTemplates;
    }

    vm.init();
});