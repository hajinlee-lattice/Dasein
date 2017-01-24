angular.module('common.datacloud.explorertabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
    ])
.controller('ExplorerTabsController', function ($state, $scope,
    BrowserStorageUtility, ResourceUtility, DataCloudStore) {

    var vm = this;

    angular.extend(vm, {
        section: getSection($state.current.name),
        DataCloudStore: DataCloudStore
    });

    function getSection(string, fromEnd) {
        var arr = string.split('.'),
            fromEnd = fromEnd || 1,
            section = arr.slice(Math.max(arr.length - fromEnd, 1)).join('.');

        return section;
    }


    vm.init = function() {

    }

    vm.init();
});