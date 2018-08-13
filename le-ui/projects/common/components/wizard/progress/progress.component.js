angular.module('common.wizard.progress', [
    'common.exceptions'
])
.controller('ImportWizardProgress', function(
    $state, $stateParams, $scope, $timeout, ResourceUtility, WizardProgressContext, 
    WizardProgressItems, WizardValidationStore, ServiceErrorUtility
) {
    var vm = this;

    angular.extend(vm, {
        items: WizardProgressItems,
        context: WizardProgressContext,
        wizard: '.',
        rootState: function() {
            return 'home.' + WizardProgressContext + '.';
        }(),
        itemMap: {}
    });

    vm.init = function() {
        vm.items.forEach(function(item) {
            vm.itemMap[vm.rootState + item.state.split('.').pop()] = item;
        });

        // vm.current = vm.itemMap[vm.rootState + $state.current.name.split('.').pop()];
        // vm.currentState = vm.current.state;
        // vm.previousState = '';

    }
    
    vm.isDisable = function(item){
        if(item.progressDisabled === true){
            return true;
        }
        return false;
    }

    vm.click = function($event) {
        // This is a hack. This needs to be replaced by conditional CSS classes on the li's
        $event.preventDefault();
    }

    vm.init();
});