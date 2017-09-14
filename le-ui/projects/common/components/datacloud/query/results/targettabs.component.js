angular.module('common.datacloud.targettabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
    ])
.controller('TargetTabsController', function (
    $state, $stateParams, ResourceUtility, Config, PlaybookWizardStore
) {

    var vm = this;
    angular.extend(vm, {
        stateParams: $stateParams,
        config: Config,
        showTabs: false
    });
    vm.init = function() {

    }
    vm.init();

});