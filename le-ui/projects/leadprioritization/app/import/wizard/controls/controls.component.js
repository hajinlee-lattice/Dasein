angular.module('lp.import.wizard.controls', [])
.controller('ImportWizardControls', function(
    $state, $stateParams, $scope, $timeout, ResourceUtility, ImportWizardStore, WizardProgressItems
) {
    var vm = this;

    angular.extend(vm, {
        items: WizardProgressItems,
        state: $state.current.name,
        prev: 'home.import.entry',
        next: 'home.import.wizard'
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
        }
    }

    vm.setButtons = function() {
        var current = $state.current.name;

        for (var i=0; i<vm.items.length; i++) {
            var item = vm.items[i];
            var state = item.state;

            if ('home.import.wizard.' + state == current) {
                var split = state.split('.');
                var last = split[split.length-1];

                if (i+1 < vm.items.length) {
                    var next = vm.items[i+1].state;
                    var nsplit = next.split('.');

                    vm.next = 'home.import.wizard.' + nsplit.join('.');
                } else {
                    vm.next = '';
                }
                if (i-1 >= 0) {
                    var prev = vm.items[i-1].state;
                    var psplit = prev.split('.');
                    
                    vm.prev = 'home.import.wizard.' + psplit.join('.');
                } else {
                    vm.prev = 'home.import.entry';
                }
            }

        }
    }

    vm.init();
});