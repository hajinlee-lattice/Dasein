angular.module('common.wizard.progress', [
    'mainApp.core.modules.ServiceErrorModule'
])
.controller('ImportWizardProgress', function(
    $state, $stateParams, $scope, $timeout, ResourceUtility, WizardProgressContext, 
    WizardProgressItems, WizardValidationStore, ServiceErrorUtility
) {
    var vm = this;

    angular.extend(vm, {
        items: WizardProgressItems,
        context: WizardProgressContext,
        wizard: '.wizard.',
        rootState: function() {
            var rootValue = 'home.' + WizardProgressContext + '.';
            if(!WizardProgressContext.includes("ratingsengine.ai")){
                rootValue += 'wizard.';
            }
            return rootValue;
        }(),//'home.' + WizardProgressContext + '.wizard.',
        itemMap: {}
    });

    vm.init = function() {
        if(WizardProgressContext.includes("ratingsengine.ai")){
            vm.wizard = '.';
        }
        vm.items.forEach(function(item) {
            vm.itemMap[vm.rootState + item.state.split('.').pop()] = item;
        });

        console.log('Progress component', vm.itemMap);
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

            not_validated.forEach(function(key, index) {
                vm.itemMap[vm.rootState + key].invalid = true;
                
                $timeout(function() {
                    vm.itemMap[vm.rootState + key].invalid = false;
                }, 3000)
            })
        } else {
            var nextState = vm.rootState + state,
                current = vm.itemMap[vm.rootState + $state.current.name.split('.').pop()];

            if (current.nextFn) {
                current.nextFn(nextState);
            } else {
                $state.go(nextState);
            }
        }
    }

    vm.init();
});