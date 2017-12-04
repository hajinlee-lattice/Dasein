angular
.module('lp.playbook.dashboard.sidebar', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('SidebarPlaybookController', function(
    $rootScope, $state, $stateParams, StateHistory, ResourceUtility, PlaybookWizardStore
) {
    var vm = this;

    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        state: $state,
        stateParams: $stateParams,
        StateHistory: StateHistory
    });

    vm.init = function() {
        vm.play_name = $stateParams.play_name || '';

        var play = PlaybookWizardStore.getPlay(vm.play_name),
            launchedStatus = PlaybookWizardStore.getLaunchedStatus(play);
        
        vm.segment = play.segment;
        vm.targetsDisabled = (play.ratingEngine ? false : true);

        if ($state.current.name === 'home.playbook.dashboard.launch_job') {
            vm.menuDisabled = true;
        }

        $rootScope.$broadcast('header-back', { 
            path: '^home.playbook.dashboard',
            displayName: play.displayName,
            sref: 'home.playbook'
        });
    }

    vm.init();
});