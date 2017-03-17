angular.module('common.datacloud.analysistabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
    ])
.controller('AnalysisTabsController', function ($state, $stateParams, $scope,
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
        $state.go('home.model.analysis', params, { notify: true });
    }

    vm.init = function() {

    }

    vm.init();
});