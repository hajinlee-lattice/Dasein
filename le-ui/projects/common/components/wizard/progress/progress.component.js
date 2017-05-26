angular.module('common.wizard.progress', [])
.controller('ImportWizardProgress', function(
    $state, $stateParams, $scope, ResourceUtility, WizardProgressContext, WizardProgressItems
) {
    var vm = this;

    angular.extend(vm, {
        items: WizardProgressItems,
        context: WizardProgressContext
    });

    vm.init = function() {

    }

    vm.init();
});