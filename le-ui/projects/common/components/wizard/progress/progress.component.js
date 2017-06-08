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
        context: WizardProgressContext,
        rootState: 'home.' + WizardProgressContext + '.wizard.',
        itemMap: {}
    });

    vm.init = function() {
        vm.items.forEach(function(item) {
            vm.itemMap[vm.rootState + item.state] = item;
        });
    }

    vm.click = function(state, $event) {
        var split = state.split('.'),
            selected = split.pop(),
            validation = WizardValidationStore.validation,
            not_validated = [];

        for (var i=0; i<split.length; i++) {
            var section = split[i],
                vsection = validation[section];
            
            if (!vsection) {
                not_validated.push(section);
            }
        }
        
        if (not_validated.length > 0) {
            $event.preventDefault();
        } else {
            var nextState = vm.rootState + state,
                current = vm.itemMap[$state.current.name];

            if (current.nextFn) {
                current.nextFn(nextState);
            } else {
                $state.go(nextState);
            }
        }
    }

    vm.init();
});