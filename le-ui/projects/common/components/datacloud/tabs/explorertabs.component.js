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
        var goHome = false;
        if(section && section == vm.section && section) {
            goHome = true;
        }
        vm.section = section;
        var params = {
            section: vm.section
        }
        if(goHome) {
            params.category = '';
            params.subcategory = '';
        }
        $state.go('home.datacloud.explorer', params, { notify: true });
    }

    vm.init = function() {

    }

    vm.init();
});