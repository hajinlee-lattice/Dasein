angular.module('lp.import.wizard.thirdpartyids', [])
.controller('ImportWizardThirdPartyIDs', function(
    $state, $stateParams, $scope, ResourceUtility, ImportWizardStore, Identifiers, FieldDocument
) {
    var vm = this;

    angular.extend(vm, {
        identifiers: Identifiers,
        fieldMappings: FieldDocument.fieldMappings,
        fieldMappingsMap: {},
        AvailableFields: [],
        idFieldMapping: {"userField":"CRMId","mappedField":"CRMId","fieldType":"TEXT","mappedToLatticeField":true},
        Id: 'CRMId',
     });

     vm.init = function() {
         vm.fieldMappings.forEach(function(fieldMapping) {
             vm.fieldMappingsMap[fieldMapping.mappedField] = fieldMapping;
         });

         vm.fieldMappings.forEach(function(fieldMapping) {
             var userField = fieldMapping.userField;
             if (fieldMapping.mappedField != 'Id') {
                 vm.AvailableFields.push(userField);
             }
         });

     };

     vm.changeLatticeField = function(mapping) {
         if(vm.fieldMappingsMap[vm.Id]) {
	         vm.fieldMappingsMap[vm.Id].userField = mapping.userField;
	         vm.fieldMappingsMap[vm.Id].mappedToLatticeField = true;
    	 }
         ImportWizardStore.setFieldDocument(FieldDocument);
     };


    vm.addIdentifier = function(){
    	console.log("Add Identifier");
    };

    vm.init();
});