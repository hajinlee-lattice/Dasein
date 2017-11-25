angular.module('lp.import.wizard.thirdpartyids', [])
.controller('ImportWizardThirdPartyIDs', function(
    $state, $stateParams, $scope, ResourceUtility, ImportWizardStore, Identifiers, FieldDocument
) {
    var vm = this;

    angular.extend(vm, {
        identifiers: Identifiers,
        fieldMappings: FieldDocument.fieldMappings,
        availableFields: [],
        fields: [],
        field: {name: '', types: ['MAP','CRM','ERP','Other'], field: ''}
    });

    vm.init = function() {
        vm.fieldMappings.forEach(function(fieldMapping) {
            var userField = fieldMapping.userField;
            vm.availableFields.push(userField);
        });
    };

    vm.changeLatticeField = function(mapping) {
        ImportWizardStore.setSaveObjects(mapping);
    };

    vm.addIdentifier = function(){
        vm.fields.push(vm.field);
    };

    vm.init();
});