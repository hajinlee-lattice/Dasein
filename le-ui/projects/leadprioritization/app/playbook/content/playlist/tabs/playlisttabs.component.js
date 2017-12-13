angular.module('lp.playbook.playlisttabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('PlayListTabsController', function ($state, $filter, ResourceUtility, PlaybookWizardStore) {
    var vm = this;

    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        current: PlaybookWizardStore.current
    });

    vm.count = function(type, current) {
        var filter = current
            ? { launchHistory: { playLaunch: { launchState: type } } }
            : { launchHistory: { mostRecentLaunch: { launchState: type } } };
        
        return ($filter('filter')(vm.current.plays, filter, true) || []).length;
    }

    vm.historyTabIsDisabled = ((vm.count('Launching') + vm.count('Launched') + vm.count('Failed')) === 0);

    // console.log(vm.historyTabIsDisabled);

    vm.clickLaunchHistoryTab = function($event) {
        $state.go('home.playbook.plays.launchhistory', {reload: true});
    };

});