angular.module('lp.import.wizard.matchtoaccounts', [])
.controller('ImportWizardMatchToAccounts', function(
    $state, $stateParams, $scope, $timeout, 
    ResourceUtility, ImportWizardStore, FieldDocument, UnmappedFields, MatchingFields, Banner
) {
    var vm = this;
    var alreadySaved = ImportWizardStore.getSavedDocumentFields($state.current.name);
    // console.log(alreadySaved)
    if(alreadySaved){
        FieldDocument.fieldMappings = alreadySaved;
        // vm.updateAnalysisFields();
    }
    var makeList = function(object) {
        var list = [];
        for(var i in object) {
            list.push(object[i]);
        }
        return list;
    }

    var matchingFieldsList = makeList(MatchingFields),
        ignoredFieldLabel = '-- Unmapped Field --',
        noFieldLabel = '-- No Fields Available --',
        entityMatchEnabled = ImportWizardStore.entityMatchEnabled;

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
        entityMatchEnabled: ImportWizardStore.entityMatchEnabled,
        ignoredFields: FieldDocument.ignoredFields || [],
        ignoredFieldLabel: ignoredFieldLabel,
        matchingFieldsList: angular.copy(matchingFieldsList),
        matchingFields: MatchingFields,
    });

    vm.init = function() {

        let validationStatus = ImportWizardStore.getValidationStatus();
        if (validationStatus) {
            let messageArr = validationStatus.map(function(error) { return error['message']; });
            Banner.error({ message: messageArr });
        }

        console.log('AvailableFields 1', angular.copy(vm.AvailableFields));
        vm.UnmappedFields = UnmappedFields;

        ImportWizardStore.setUnmappedFields(UnmappedFields);
        ImportWizardStore.setValidation('matchtoaccounts', vm.entityMatchEnabled);

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
            return (item.userField && typeof item.userField !== 'object');
        });
        console.log('AvailableFields 2', angular.copy(vm.AvailableFields));
    };

    vm.changeLatticeField = function(mapping, form) {
        var mapped = [];
        vm.unavailableFields = [];
        for(var i in mapping) {
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
        ImportWizardStore.setSaveObjects(mapped, $state.current.name);
        vm.checkValid(form);
    };


    /**
     * NOTE: The delimiter could cause a problem if the column name has : as separator 
     * @param {*} string 
     * @param {*} delimiter 
     */
    var makeObject = function(string, delimiter) {
        var delimiter = delimiter || '^/',
            string = string || '',
            pieces = string.split(delimiter);

        return {
            mappedField: pieces[0],
            userField: (pieces[1] === "" ? fallbackUserField : pieces[1]) // allows unmapping
        }
    }

    vm.changeMatchingFields = function(mapping, form) {
        var _mapping = [];
        vm.unavailableFields = [];
        vm.ignoredFieldLabel = ignoredFieldLabel;

        for(var i in mapping) {
            var item = mapping[i],
                map = makeObject(item.userField);

            if(!map.userField) {
                /**
                 * to unmap find the userField using the original fieldMappings object
                 */
                var fieldItem = vm.fieldMappings.find(function(item) {
                    return item.mappedField === i;
                });
                if(fieldItem && fieldItem.userField) {
                    map.userField = fieldItem.userField;
                    map.mappedField = null;
                    map.unmap = true;
                }
            }

            if(item.userField) {
                vm.unavailableFields.push(map.userField);
            }

            if(map.userField) {
                _mapping.push(map);
            }
        }

        if(vm.unavailableFields.length >= vm.AvailableFields.length) {
            vm.ignoredFieldLabel = noFieldLabel;
        }
        ImportWizardStore.setSaveObjects(_mapping);
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

    vm.checkMatchingFieldsDelay = function(form) {
        $timeout(function() {
            for(var i in vm.fieldMapping) {
                var key = i,
                userField = vm.fieldMapping[key];

                vm.keyMap[key] = userField;
                vm.initialMapping[key] = userField;

                var fieldMapping = vm.fieldMapping[i],
                fieldObj = makeObject(fieldMapping.userField);

                vm.unavailableFields.push(fieldObj.userField);
            }
        }, 1);
    }

    vm.checkValidDelay = function(form) {
        $timeout(function() {
            vm.checkValid(form);
        }, 1);
    };

    vm.checkValid = function(form) {
        ImportWizardStore.setValidation('matchtoaccounts', form.$valid);
    }


    vm.init();
});