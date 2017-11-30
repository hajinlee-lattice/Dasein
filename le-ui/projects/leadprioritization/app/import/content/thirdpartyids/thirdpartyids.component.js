angular.module('lp.import.wizard.thirdpartyids', [])
.controller('ImportWizardThirdPartyIDs', function(
    $state, $stateParams, $scope, ResourceUtility, ImportWizardStore, Identifiers, FieldDocument
) {
    var vm = this;

    angular.extend(vm, {
        identifiers: Identifiers,
        fieldMappings: FieldDocument.fieldMappings,
        fieldMapping: ImportWizardStore.getThirdpartyidFields().map,
        fields: ImportWizardStore.getThirdpartyidFields().fields,
        availableFields: [],
        unavailableFields: [],
        hiddenFields: [],
        field: {name: '', types: ['MAP','CRM','ERP','Other'], field: ''}
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

    vm.changeLatticeField = function(mapping) {
        //  var mapped = [];
        //  for(var i in mapping) {
        //     var item = mapping[i];
        //         map = {}; //userField: item.userField.value, mappedField: item.mappedField};
        //     if(item.mappedField) {
        //         map.mappedField = item.mappedField;
        //     }
        //     if(item.userField && item.userField.value) {
        //         map.userField = item.userField.value;
        //     }
        //     mapped.push(map);
        // }
        ImportWizardStore.setSaveObjects(mapping);
        ImportWizardStore.setThirdpartyidFields(vm.fields, mapping);
    };

    vm.addIdentifier = function(){
        vm.fields.push(vm.field);
    };

    vm.init();
});