angular.module('lp.import.wizard.thirdpartyids', [])
.controller('ImportWizardThirdPartyIDs', function(
    $state, $stateParams, $scope, $timeout, 
    ResourceUtility, ImportWizardStore, Identifiers, FieldDocument
) {
    var vm = this;

    angular.extend(vm, {
        identifiers: Identifiers,
        fieldMappings: FieldDocument.fieldMappings,
        fieldMapping: ImportWizardStore.getThirdpartyidFields().map,
        fields: ImportWizardStore.getThirdpartyidFields().fields,
        availableFields: [],
        unavailableFields: [],
        unavailableTypes: [],
        hiddenFields: [],
        field: {name: '', types: [
            'MAP',
            'CRM',
            //'ERP',
            'Other'
        ], field: ''}
    });

    vm.init = function() {
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
            vm.unavailableTypes.push(item.mappedField);
        });

        ImportWizardStore.setSaveObjects(mapping);
        ImportWizardStore.setThirdpartyidFields(vm.fields, mapping);
        vm.checkValid(form);
    };

    vm.addIdentifier = function(){
        vm.fields.push(vm.field);
    };

    vm.removeIdentifier = function(index){
    };

    var validateMapping = function(mapping) {
        var keys = [],
            valid = true;
        mapping.forEach(function(item) {
            var key = item.userName + item.mappedField;
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
        if(!validateMapping(vm.fieldMapping)) {
            ImportWizardStore.setValidation('thirdpartyids', false);
        } else {
            ImportWizardStore.setValidation('thirdpartyids', form.$valid);
        }
    }

    vm.init();
});