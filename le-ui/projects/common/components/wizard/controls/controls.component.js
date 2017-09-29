angular.module('common.wizard.controls', [])
.controller('ImportWizardControls', function(
    $state, $stateParams, $scope, $timeout, ResourceUtility, WizardProgressItems,
    WizardProgressContext, WizardControlsOptions, WizardValidationStore, ImportWizardService, ImportWizardStore
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
            if (vm.next == 'home.import.wizard.accounts.one.two.three.four.five') {
                ImportWizardService.SaveFieldDocuments(ImportWizardStore.getCsvFileName(), ImportWizardStore.getFieldDocument());
        	} 
            vm.go(vm.next, isPrev);
        } else if (isPrev && vm.prev) {
            vm.go(vm.prev, isPrev);
        } else if (!isPrev && !vm.next) {
            // FIXME:  there should be no ImportWizard specific code in this generic component
            //ImportWizardService.startImportCsv(ImportWizardStore.getCsvFileName());
            
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

        if (current.nextFn && !isPrev) {
            current.nextFn(state, params);
        } else {
            $state.go(state, params);
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