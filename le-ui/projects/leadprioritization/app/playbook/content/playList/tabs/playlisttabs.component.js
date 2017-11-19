angular.module('lp.playbook.playlisttabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('PlayListTabsController', function ($filter, ResourceUtility, PlaybookWizardStore) {
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
});