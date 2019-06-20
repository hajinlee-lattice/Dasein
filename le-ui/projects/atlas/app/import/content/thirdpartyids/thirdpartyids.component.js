angular.module('lp.import.wizard.thirdpartyids', [])
.controller('ImportWizardThirdPartyIDs', function(
    $state, $stateParams, $scope, $timeout, 
    ResourceUtility, ImportWizardStore, Identifiers, FieldDocument
) {
    var vm = this;
    var alreadySaved = ImportWizardStore.getSavedDocumentFields($state.current.name);
    if(alreadySaved){
        FieldDocument.fieldMappings = alreadySaved;
    }

    angular.extend(vm, {
        identifliers: Identifiers,
        fieldMappings: FieldDocument.fieldMappings,
        fieldMapping: ImportWizardStore.getThirdpartyidFields().map,
        fields: ImportWizardStore.getThirdpartyidFields().fields,
        savedFields: ImportWizardStore.getSaveObjects($state.current.name),
        entityType: ImportWizardStore.entityType,
        availableFields: [],
        unavailableFields: [],
        unavailableTypes: [],
        hiddenFields: [],
        field: {name: '', types: [
            'MAP',
            'CRM',
            //'ERP',
            'OTHER'
        ], field: ''}
    });

    vm.init = function() {

        let validationStatus = ImportWizardStore.getValidationStatus();
        if (validationStatus) {
            let messageArr = validationStatus.map(function(error) { return error['message']; });
            Banner.error({ message: messageArr });
        }
        
        vm.fieldMappings.forEach(function(fieldMapping) {
            vm.availableFields.push(fieldMapping.userField);
        });
    };

    vm.hiddenFilter = function(item) {
        if(vm.hiddenFields.indexOf(item) !== -1) {
            return false;
        }
        return true;
    }

    vm.changeLatticeField = function(mapping, form) {
        vm.unavailableFields = [];
        vm.unavailableTypes = [];
        mapping.forEach(function(item){
            //vm.unavailableFields.push(item.userField);
            vm.unavailableTypes.push(item.userField);
            item.append = true;
        });

        ImportWizardStore.setSaveObjects(mapping);
        ImportWizardStore.setThirdpartyidFields(vm.fields, mapping);
        vm.checkValid(form);
    };

    vm.addIdentifier = function(){
        vm.fields.push(vm.field);
    };

    vm.removeIdentifier = function(index, form){
        vm.fieldMapping.splice(index, 1);
        vm.fields.splice(index, 1);
        vm.checkValid(form);
    };

    var validateMapping = function(mapping) {
        var keys = [],
            valid = true;
        mapping.forEach(function(item) {
            var type = item.cdlExternalSystemType,
                name = (item.mappedField ? item.mappedField.toLowerCase() : ''),
                key = name + type;
            valid = (keys.indexOf(key) === -1);
            keys.push(key);
        });
        return valid;
    }

    vm.checkValidDelay = function(form) {
        $timeout(function() {
            vm.checkValid(form);
        }, 1);
    };

    vm.checkValid = function(form) {
        $timeout(function() {
            if(!validateMapping(vm.fieldMapping)) {
                ImportWizardStore.setValidation('thirdpartyids', false);
            } else if (!vm.fieldMapping.length) {
                ImportWizardStore.setValidation('thirdpartyids', true);
            } else {
                ImportWizardStore.setValidation('thirdpartyids', form.$valid);
            }
        }, 0);
    }

    vm.init();
});