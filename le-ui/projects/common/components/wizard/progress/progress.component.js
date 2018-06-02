angular.module('common.wizard.progress', [
    'mainApp.core.modules.ServiceErrorModule'
])
.controller('ImportWizardProgress', function(
    $state, $stateParams, $scope, $timeout, ResourceUtility, WizardProgressContext, 
    WizardProgressItems, WizardValidationStore, ServiceErrorUtility, DisableWizardNavOnLastStep
) {
    var vm = this;

    angular.extend(vm, {
        items: WizardProgressItems,
        context: WizardProgressContext,
        disabledNav: DisableWizardNavOnLastStep || false,
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
    }
    
    vm.isDisable = function(item){
        if(item.progressDisabled === true){
            return true;
        }
        return false;
    }

    vm.click = function(item, state, $event) {
        if(item.progressDisabled === true){
            $event.preventDefault();
            return;
        }
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

            not_validated.forEach(function(key, index) {
                vm.itemMap[vm.rootState + key].invalid = true;
                
                $timeout(function() {
                    vm.itemMap[vm.rootState + key].invalid = false;
                }, 3000)
            })
        } else {
            var nextState = vm.rootState + state,
                nextStateUrl = 'home.' + vm.context + vm.wizard + nextState,
                current = vm.itemMap[vm.rootState + $state.current.name.split('.').pop()];
            if (current.nextFn) {
                current.nextFn(nextStateUrl);
            } else {
                $state.go(nextStateUrl);
            }
        }
    }

    vm.init();
});