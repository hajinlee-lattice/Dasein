angular.module('lp.configureattributes.configure', [])
.component('configureAttributesConfigure', {
    controllerAs: 'vm',
    templateUrl: 'app/configureattributes/content/configure/configure.component.html',
    controller: function(
        $state, $stateParams, $scope, $timeout, 
        ResourceUtility
    ) {
        var vm = this;

        angular.extend(vm, {
            stateParams: $stateParams,
            steps: {
                spend_change: {
                    label: '% Spend Change'
                },
                spend_over_time: {
                    label: 'Spend Over Time'
                },
                share_of_wallet: {
                    label: 'Share of Wallet'
                },
                margin: {
                    label: '% Margin'
                }
            },
            step: $state.current.name.split('.').pop(-1)
        });

        vm.steps_count = Object.keys(vm.steps).length;

        vm.init = function() {
        };

        vm.goto = function(name) {
            $state.go('home.configureattributes.' + name);
        }

        vm.init();
    }
});