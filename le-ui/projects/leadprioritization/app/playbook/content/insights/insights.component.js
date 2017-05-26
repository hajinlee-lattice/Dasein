angular.module('lp.playbook.wizard.insights', [])
.controller('PlaybookWizardInsights', function(
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