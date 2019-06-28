angular.module('lp.import.wizard.accountids', [])
.controller('ImportWizardAccountIDs', function(
    $state, $stateParams, $scope, $timeout, 
    ResourceUtility, FeatureFlagService, ImportWizardStore, FieldDocument, UnmappedFields, Banner
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
        idFieldMapping: {
            userField: "Id",
            mappedField: "Id",
            fieldType: "TEXT",
            mappedToLatticeField: true
        },
        mappedFieldMap: {
            account: (entityMatchEnabled ? 'CustomerAccountId' : 'AccountId')
        },
        entityMatchEnabled: entityMatchEnabled,
        UnmappedFieldsMappingsMap: {},
        savedFields: ImportWizardStore.getSaveObjects($state.current.name),
        initialMapping: {},
        keyMap: {},
        saveMap: {},
        matchIdItems: [],
        match: false
    });

    vm.init = function() {
        var flags = FeatureFlagService.Flags();

        let validationStatus = ImportWizardStore.getValidationStatus();
        let banners = Banner.get();
        if (validationStatus && banners.length == 0) {
            let messageArr = validationStatus.map(function(error) { return error['message']; });
            Banner.error({ message: messageArr });
        }

        vm.UnmappedFields = UnmappedFields;
        ImportWizardStore.setUnmappedFields(UnmappedFields);
        ImportWizardStore.setValidation('ids', false);

        vm.UnmappedFields.forEach(function(field) {
            vm.UnmappedFieldsMappingsMap[field.name] = field;
        });

        var userFields = [];
        vm.fieldMappings.forEach(function(fieldMapping, index) {
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

        if(vm.isMultipleTemplates()){
            vm.setMapToContactId();
        }
    };

    vm.setMapToContactId = () => {
        // vm.fieldMappings[0].mapToLatticeId = true;
        // console.log(vm.fieldMappings);

        for(var i = 0; i < vm.fieldMappings.length; i++) {
            if(vm.fieldMappings[i].mapToLatticeId){
                vm.match = vm.fieldMappings[i].mapToLatticeId;
                break;
            }
        }
    }

    vm.changeLatticeField = function(mapping, form) {
        var mapped = [];
        vm.unavailableFields = [];
        for(var i in mapping) {
            if(mapping[i] || mapping[i] === "") { // yes yes, don't worry about it
                var key = i,
                    userField = mapping[key],
                    map = {
                        userField: userField, //(userField === 'unmapToNull' ? null : userField), 
                        mappedField: vm.mappedFieldMap[key],
                        // removing the following 3 lines makes it update instead of append
                        originalUserField: (vm.saveMap[vm.mappedFieldMap[key]] ? vm.saveMap[vm.mappedFieldMap[key]].originalUserField : vm.keyMap[vm.mappedFieldMap[key]]),
                        originalMappedField: (vm.saveMap[vm.mappedFieldMap[key]] ? vm.saveMap[vm.mappedFieldMap[key]].originalMappedField : vm.mappedFieldMap[key]),
                        append: false
                    };
                // leaving this here because maybe a hack for PLS-13927, I never tested this though
                // if you see this after july 2019, please remove
                // console.log(map);
                // if(vm.entityMatchEnabled) {
                //     map = Object.assign({
                //         idType: 'Account',
                //         systemName: 'DefaultSystem',
                //         mapToLatticeId: true 
                //     }, map);
                // }
                // console.log(map);
                if(vm.isMultipleTemplates() && map.mappedField == "CustomerAccountId"){
                    map.mapToLatticeId = vm.match;
                    map.IdType = map.mapToLatticeId == true ?'Account' : null;
                }
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
    vm.isMultipleTemplates = () => {
        var flags = FeatureFlagService.Flags();
        var multipleTemplates = FeatureFlagService.FlagIsEnabled(flags.ENABLE_MULTI_TEMPLATE_IMPORT);
        return multipleTemplates;
    }

    vm.addMatchId = () => {
        vm.matchIdItems.push({
            userField: '-- Select Field --',
            system: '-- Select System --'
        });
    }
    vm.removeMatchId = (index) => {
        vm.matchIdItems.splice(index, 1);
    }

    vm.init();

});