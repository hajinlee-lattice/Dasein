angular.module('lp.import.wizard.producthierarchy', [])
.controller('ImportWizardProductHierarchy', function(
    $state, $stateParams, $scope, $timeout, ResourceUtility, ImportWizardStore, FieldDocument, UnmappedFields
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
            product_line: 'ProductLine',
            product_family: 'ProductFamily',
            product_category: 'ProductCategory',
        },
        UnmappedFieldsMappingsMap: {},
        savedFields: ImportWizardStore.getSaveObjects($state.current.name),
        initialMapping: {},
        keyMap: {},
        saveMap: {}
    });

    vm.init = function() {
        vm.UnmappedFields = UnmappedFields;

        ImportWizardStore.setUnmappedFields(UnmappedFields);
        ImportWizardStore.setValidation('ids', false);
        
        vm.fieldMappings.forEach(function(fieldMapping, index) {
            vm.fieldMappingsMap[fieldMapping.mappedField] = fieldMapping;
            vm.AvailableFields.push(fieldMapping);
            for(var i in vm.mappedFieldMap) {
                if(fieldMapping.mappedField == vm.mappedFieldMap[i]) {
                    vm.fieldMapping[i] = fieldMapping.userField
                }
            }
        });
        if(vm.savedFields) {
            vm.savedFields.forEach(function(fieldMapping, index) {
                vm.saveMap[fieldMapping.originalMappedField] = fieldMapping;

                vm.fieldMappingsMap[fieldMapping.mappedField] = fieldMapping;
                vm.AvailableFields.push(fieldMapping);
                for(var i in vm.mappedFieldMap) {
                    if(fieldMapping.mappedField == vm.mappedFieldMap[i]) {
                        vm.fieldMapping[i] = fieldMapping.userField
                    }
                }
            });
        }
    };

    vm.changeLatticeField = function(mapping, form) {
        var mapped = [];
        for(var i in mapping) {
            var key = i,
                userField = mapping[key],
                map = {
                    userField: userField, 
                    mappedField: vm.mappedFieldMap[key],
                    originalUserField: (vm.saveMap[vm.mappedFieldMap[key]] ? vm.saveMap[vm.mappedFieldMap[key]].originalUserField : vm.keyMap[vm.mappedFieldMap[key]]),
                    originalMappedField: (vm.saveMap[vm.mappedFieldMap[key]] ? vm.saveMap[vm.mappedFieldMap[key]].originalMappedField : vm.mappedFieldMap[key]),
                    append: true
                };
            mapped.push(map);
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
            }
        }, 1);
    }

    vm.checkValidDelay = function(form) {
        $timeout(function() {
            vm.checkValid(form);
        }, 1);
    };

    vm.checkValid = function(form) {
        ImportWizardStore.setValidation('product_hierarchy', form.$valid);
    }

    vm.init();
});