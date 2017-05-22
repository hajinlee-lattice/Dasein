angular.module('common.wizard.progress', [])
.controller('ImportWizardProgress', function(
    $state, $stateParams, $scope, ResourceUtility, ImportWizardStore, WizardProgressItems
) {
    var vm = this;

    angular.extend(vm, {
        items: WizardProgressItems
    });

    vm.init = function() {

    }

    vm.init();
});