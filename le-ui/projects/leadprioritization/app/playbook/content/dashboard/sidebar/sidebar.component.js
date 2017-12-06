angular
.module('lp.playbook.dashboard.sidebar', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('SidebarPlaybookController', function(
    $rootScope, $state, $stateParams, StateHistory, ResourceUtility, PlaybookWizardStore, Play
) {
    var vm = this;

    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        state: $state,
        stateParams: $stateParams,
        StateHistory: StateHistory,
        play: Play
    });

    vm.init = function() {
        vm.play_name = $stateParams.play_name || '';

        var launchedStatus = PlaybookWizardStore.getLaunchedStatus(vm.play);
        
        console.log(vm.play);

        vm.segment = vm.play.segment;
        vm.targetsDisabled = (vm.play.ratingEngine ? false : true);

        if ($state.current.name === 'home.playbook.dashboard.launch_job') {
            vm.menuDisabled = true;
        }

        $rootScope.$broadcast('header-back', { 
            path: '^home.playbook.dashboard',
            displayName: vm.play.displayName,
            sref: 'home.playbook'
        });
    }

    vm.init();
});