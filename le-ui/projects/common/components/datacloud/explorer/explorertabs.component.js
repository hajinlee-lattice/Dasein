angular.module('common.datacloud.explorertabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
    ])
.controller('ExplorerTabsController', function ($state, $stateParams, $scope,
    BrowserStorageUtility, ResourceUtility, DataCloudStore) {

    var vm = this;

    angular.extend(vm, {
        DataCloudStore: DataCloudStore,
        stateParams: $stateParams,
        section: $stateParams.section
    });

    vm.setStateParams = function(section) {
        vm.section = section;
        $state.go('home.datacloud.explorer', { section: vm.section }, { notify: true });
    }

    vm.init = function() {

    }

    vm.init();
});