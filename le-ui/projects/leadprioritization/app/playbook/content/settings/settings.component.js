angular.module('lp.playbook.wizard.settings', [])
.controller('PlaybookWizardSettings', function(
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