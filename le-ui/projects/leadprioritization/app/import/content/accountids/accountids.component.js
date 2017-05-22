angular.module('lp.import.wizard.accountids', [])
.controller('ImportWizardAccountIDs', function(
    $state, $stateParams, $scope, ResourceUtility, ImportWizardStore
) {
    var vm = this;

    angular.extend(vm, {
        state: ImportWizardStore.getAccountIdState()
    });

    vm.init = function() {

    };

    vm.init();
});