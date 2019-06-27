import {actions, reducer} from '../../templates/multiple/multipletemplates.redux';
import { store, injectAsyncReducer } from 'store';
angular.module('lp.import.wizard.contactids', [])
.controller('ImportWizardContactIDs', function(
    $state, $stateParams, $scope, $timeout, 
    ResourceUtility, ImportWizardStore, FieldDocument, 
    UnmappedFields, Banner, FeatureFlagService
) {
    var vm = this;
    vm.ignoredFieldLabel = '-- Unmapped Field --';
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
        entityMatchEnabled: entityMatchEnabled,
        matchIdItems: [],
        systems: [],
        match: false
    });

    vm.init = function() {
        vm.UnmappedFields = UnmappedFields;
        injectAsyncReducer(store, 'multitemplates.contactids', reducer);
        this.unsubscribe = store.subscribe(() => {
            const data = store.getState()['multitemplates.contactids'];
            // vm.systems = [{name: 't1', displayName: 'Test 1'}, {name: 't2', displayName: 'Test 2'}]; //data;
        });
       //[{ displayName: '-- Select System --', name: 'select'},{name: 't1', displayName: 'Test 1'}, {name: 't2', displayName: 'Test 2'}],
        actions.fetchSystems({Contact: true});
        let validationStatus = ImportWizardStore.getValidationStatus();
        if (validationStatus) {
            let messageArr = validationStatus.map(function(error) { return error['message']; });
            Banner.error({ message: messageArr });
        }

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
        console.log('MMM ==> ',mapping);
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
                // console.log(' <===> ',map, vm.fieldMapping.contact);
                if(vm.isMultipleTemplates() && map.mappedField == "CustomerContactId"){
                    map.mapToLatticeId = vm.match;
                    map.IdType = map.mapToLatticeId == true ?'Contact' : null;
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


    //

    vm.isMultipleTemplates = () => {
        var flags = FeatureFlagService.Flags();
        var multipleTemplates = FeatureFlagService.FlagIsEnabled(flags.ENABLE_MULTI_TEMPLATE_IMPORT);
        return multipleTemplates;
    }

    vm.addMatchId = () => {
        vm.matchIdItems.push({
            userField: '-- Select Field --',
            system: { displayName: '-- Select System --', name: 'select'}
        });
    }
    vm.removeMatchId = (index) => {
        vm.matchIdItems.splice(index, 1);
    }

    vm.updateSystem = ($index) => {
        let item = vm.matchIdItems[$index];
        console.log('ITEM ', item);

    }
    vm.changeMatchingFields = (index, newVal, oldVal) => {
        console.log(index, ' NEW => ', newVal, " == OLD => ",  oldVal);
        console.log('MAPPING ==> ', vm.fieldMapping);
        
    }

    vm.init();
});