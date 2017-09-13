angular.module('common.datacloud.targettabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
    ])
.controller('TargetTabsController', function (
    $state, $stateParams, ResourceUtility, Config
) {

    var vm = this;
    angular.extend(vm, {
        stateParams: $stateParams,
        config: Config
    });
    vm.init = function() {
        
    }
    vm.init();

});