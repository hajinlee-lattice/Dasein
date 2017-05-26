angular.module('common.wizard.controls', [])
.controller('ImportWizardControls', function(
    $state, $stateParams, $scope, $timeout, ResourceUtility, WizardProgressContext, WizardProgressItems
) {
    var vm = this;

    angular.extend(vm, {
        items: WizardProgressItems,
        state: $state.current.name,
        prev: 'home.' + WizardProgressContext + '.entry',
        next: 'home.' + WizardProgressContext + '.wizard'
    });

    vm.init = function() {
        vm.setButtons();
    }

    vm.click = function(isPrev) {
        vm.setButtons();
        
        if (vm.next && !isPrev) {
            $state.go(vm.next);
        } else if (isPrev && vm.prev) {
            $state.go(vm.prev);
        } else if (!isPrev && !vm.next) {
            $state.go('home.segment')
        }
    }

    vm.setButtons = function() {
        var current = $state.current.name;

        for (var i=0; i<vm.items.length; i++) {
            var item = vm.items[i];
            var state = item.state;

            if ('home.' + WizardProgressContext + '.wizard.' + state == current) {
                var split = state.split('.');
                var last = split[split.length-1];

                if (i+1 < vm.items.length) {
                    var next = vm.items[i+1].state;
                    var nsplit = next.split('.');

                    vm.next = 'home.' + WizardProgressContext + '.wizard.' + nsplit.join('.');
                } else {
                    vm.next = '';
                }
                if (i-1 >= 0) {
                    var prev = vm.items[i-1].state;
                    var psplit = prev.split('.');
                    
                    vm.prev = 'home.' + WizardProgressContext + '.wizard.' + psplit.join('.');
                } else {
                    vm.prev = 'home.' + WizardProgressContext + '.entry';
                }
            }

        }
    }

    vm.init();
});