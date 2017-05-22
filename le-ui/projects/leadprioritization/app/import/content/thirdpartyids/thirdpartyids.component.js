angular.module('lp.import.wizard.thirdpartyids', [])
.controller('ImportWizardThirdPartyIDs', function(
    $state, $stateParams, $scope, ResourceUtility, ImportStore, Identifiers
) {
    var vm = this;

    angular.extend(vm, {
    	identifiers: Identifiers
    });

    vm.init = function() {

    }

    vm.addIdentifier = function(){
    	console.log("Add Identifier");
    };

    vm.init();
});