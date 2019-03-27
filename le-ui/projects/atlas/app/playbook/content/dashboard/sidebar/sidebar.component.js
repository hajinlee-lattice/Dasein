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
        play: Play,
        dashboardType: ''
    });

    vm.init = function() {
        vm.play_name = $stateParams.play_name || '';

        var launchedStatus = PlaybookWizardStore.getLaunchedStatus(vm.play);

        vm.launchHistoryDisabled = !launchedStatus.hasLaunchHistory;

        vm.segment = vm.play.segment;
        vm.targetsDisabled = false; //(vm.play.ratingEngine ? false : true);

        if ($state.current.name === 'home.playbook.dashboard.launch_job') {
            vm.menuDisabled = true;
        }

    }

    vm.init();
});