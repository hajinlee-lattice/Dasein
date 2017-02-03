angular.module('common.datacloud.explorertabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
    ])
.controller('ExplorerTabsController', function ($state, $stateParams, $scope,
    FeatureFlagService, BrowserStorageUtility, ResourceUtility, DataCloudStore) {

    var vm = this,
        flags = FeatureFlagService.Flags();

    angular.extend(vm, {
        DataCloudStore: DataCloudStore,
        stateParams: $stateParams,
        section: $stateParams.section,
        show_lattice_insights: FeatureFlagService.FlagIsEnabled(flags.LATTICE_INSIGHTS)
    });

    vm.setStateParams = function(section) {
        vm.section = section;
        $state.go('home.datacloud.explorer', { section: vm.section }, { notify: true });
    }

    vm.init = function() {

    }

    vm.init();
});