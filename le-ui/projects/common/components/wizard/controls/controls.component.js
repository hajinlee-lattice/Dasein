angular.module('common.wizard.controls', [])
.controller('ImportWizardControls', function(
    $state, $stateParams, $scope, $timeout, ResourceUtility, WizardProgressItems,
    WizardProgressContext, WizardControlsOptions, WizardValidationStore
) {
    var vm = this;

    angular.extend(vm, {
        itemMap: {},
        items: WizardProgressItems,
        state: $state.current.name,
        prev: WizardControlsOptions.backState,
        next: 'home.' + WizardProgressContext + '.wizard',
        valid: false
    });

    vm.init = function() {
        vm.rootState = vm.next + '.';

        vm.setButtons();

        vm.items.forEach(function(item) {
            vm.itemMap[vm.rootState + item.state] = item;
        });
    }

    vm.click = function(isPrev) {
        vm.setButtons();

        if (vm.next && !isPrev) {
            vm.go(vm.next);
        } else if (isPrev && vm.prev) {
            vm.go(vm.prev);
        } else if (!isPrev && !vm.next) {
            vm.go(WizardControlsOptions.nextState);
        }
    }

    vm.go = function(state) {
        var current = vm.itemMap[$state.current.name]
            console.log(current, vm.rootState, state);
        if (current.nextFn) {
            current.nextFn(state);
        } else {
            $state.go(state);
        }
    }

    vm.setButtons = function() {
        var current = $state.current.name,
            item, state, split, last, prev, next, nsplit, psplit;

        for (var i=0; i<vm.items.length; i++) {
            item = vm.items[i];
            state = item.state;

            if (vm.rootState + state == current) {
                split = state.split('.');
                last = split[split.length-1];

                vm.prev = WizardControlsOptions.backState;
                vm.next = '';
                
                if (i+1 < vm.items.length) {
                    next = vm.items[i+1].state;
                    nsplit = next.split('.');

                    vm.next = vm.rootState + nsplit.join('.');
                }

                if (i-1 >= 0) {
                    prev = vm.items[i-1].state;
                    psplit = prev.split('.');
                    
                    vm.prev = vm.rootState + psplit.join('.');
                }
            }

        }

        vm.isValid();
    }

    vm.isValid = function() {
        var current = $state.current.name;
        var currentStep = current.split('.').pop();

        vm.valid = WizardValidationStore.getValidation(currentStep);

        return vm.valid;
    }

    vm.init();
});