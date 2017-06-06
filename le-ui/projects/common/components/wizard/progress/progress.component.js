angular.module('common.wizard.progress', [
    'mainApp.core.modules.ServiceErrorModule'
])
.controller('ImportWizardProgress', function(
    $state, $stateParams, $scope, ResourceUtility, WizardProgressContext, 
    WizardProgressItems, WizardValidationStore, ServiceErrorUtility
) {
    var vm = this;

    angular.extend(vm, {
        items: WizardProgressItems,
        context: WizardProgressContext
    });

    vm.init = function() {

    }

    vm.click = function(state, $event) {
        var split = state.split('.');
        var selected = split.pop();
        var validation = WizardValidationStore.validation;
        var not_validated = [];

        for (var i=0; i<split.length; i++) {
            var section = split[i];
            var vsection = validation[section];
            
            if (!vsection) {
                not_validated.push(section);
            }
        }
        
        if (not_validated.length > 0) {
            $event.preventDefault();
        } else {
            $state.go('home.' + vm.context + '.wizard.' + state);
        }
    }

    vm.init();
});