angular.module('common.wizard.controls', [])
.controller('ImportWizardControls', function(
    $state, $stateParams, $scope, $timeout, $rootScope, ResourceUtility, WizardProgressItems,
    WizardProgressContext, WizardControlsOptions, WizardValidationStore/*, ImportWizardService, ImportWizardStore*/
) {
    var vm = this;

    angular.extend(vm, {
        itemMap: {},
        items: WizardProgressItems,
        state: $state.current.name,
        prev: WizardControlsOptions.backState,
        next: function() {
                var nextValue = 'home.' + WizardProgressContext;
                if(WizardControlsOptions.suffix === undefined && WizardProgressContext.includes("playbook")){
                    nextValue += '.wizard';
                }
                return nextValue;
        }() ,
        
        valid: false,
        toState: $state.current,
        nextDisabled: false
    });

    vm.init = function() {
        vm.rootState = vm.next + '.';
        // console.log($state);
        vm.setButtons();

        vm.items.forEach(function(item) {
            var key = vm.rootState + item.state;
            vm.itemMap[key] = item;
        });
        vm.item = vm.itemMap[vm.toState.name];
    }

    vm.click = function(isPrev) {
        vm.setButtons();

        if (vm.next && !isPrev) {
            vm.go(vm.next, isPrev);
        } else if (isPrev && vm.prev) {
            vm.go(vm.prev, isPrev);
        } else if (!isPrev && !vm.next) {
            if (WizardControlsOptions.nextState) {
                var params = WizardControlsOptions.nextStateParams
                    ? typeof WizardControlsOptions.nextStateParams == 'function'
                        ? WizardControlsOptions.nextStateParams()
                        : WizardControlsOptions.nextStateParams
                    : {};

                vm.go(WizardControlsOptions.nextState, isPrev, params);
            }
        }

    }

    vm.go = function(state, isPrev, params) {
        var current = vm.itemMap[$state.current.name];

        vm.nextDisabled = true;

        if (current.nextFn && !isPrev) {
            current.nextFn(state, params);
        } else {
            $state.go(state, params);
        }
    }

    $rootScope.$on('$stateChangeSuccess', function(event, toState, toParams, fromState, fromParams) { 
        angular.element(window).scrollTop(0,0);
        vm.toState = toState;
        vm.item = vm.itemMap[vm.toState.name];
        vm.nextDisabled = false;
    })

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
                    next = vm.getNext(i).state;
                    nsplit = next.split('.');

                    vm.next = vm.rootState + nsplit.join('.');
                }

                if (i-1 >= 0) {
                    prev = vm.getPrev(i-1) ? vm.getPrev(i-1).state : vm.prev;
                    psplit = prev.split('.');
                    
                    vm.prev = vm.rootState + psplit.join('.');
                }
            }

        }

        vm.isValid();
    }

    vm.getNext = function(index) {
        var item,
            i = 1;

        while (item = vm.items[index + i++]) {
            if (item.hide && index + i < vm.items.length) {
                continue;
            }

            break;
        }
        return item;
    }

    vm.getPrev = function(index) {
        var item,
            i = 0;

        while (item = vm.items[index + i]) {
            i = i - 1;

            if (item.hide && index + i >= 0) {
                continue;
            }

            break;
        }
        return item;
    }

    vm.isValid = function() {
        var current = $state.current.name,
            currentStep = current.split('.').pop();

        if (WizardValidationStore.getValidation) {
            vm.valid = WizardValidationStore.getValidation(currentStep);
        } else {
            vm.valid = true;
        }

        return vm.valid;
    }

    vm.init();
});