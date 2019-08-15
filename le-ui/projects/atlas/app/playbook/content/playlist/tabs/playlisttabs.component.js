angular.module('lp.playbook.playlisttabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('PlayListTabsController', function ($state, $filter, ResourceUtility, PlaybookWizardStore) {
    var vm = this;

    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        current: PlaybookWizardStore.current,
        counts: {},
        cumulativeCount: 0
    });

    var ALL_LAUNCH_STATES = ['Launched', 'Launching', 'Completed' ,'Failed', 'Syncing', 'Synced', 'PartialSync', 'SyncFailed'];

    vm.count = function(type, current) {
        var filter = current
            ? { launchHistory: { playLaunch: { launchState: type } } }
            : { launchHistory: { mostRecentLaunch: { launchState: type } } };

        var launched = $filter('filter')(vm.current.plays, filter, true);
        if(launched && launched.length) {
            vm.counts[type] = launched.length;
        }
        return vm.counts[type] || 0;
    }

    vm.countAll = function() {
        var count = 0;
        ALL_LAUNCH_STATES.forEach(function(state) {
            count += vm.count(state);
        });
        vm.cumulativeCount = count;
        return count;
    }

    vm.historyTabIsDisabled = ((vm.count('Launching') + vm.count('Launched') + vm.count('Failed')) === 0);

    // console.log(vm.historyTabIsDisabled);

    vm.clickLaunchHistoryTab = function($event) {
        $state.go('home.playbook.plays.launchhistory', {reload: true});
    };

});