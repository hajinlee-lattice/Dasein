angular.module('common.datacloud.targettabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
    ])
.controller('TargetTabsController', function (
    $state, $stateParams, ResourceUtility
) {

    var vm = this;
    angular.extend(vm, {
        stateParams: $stateParams
    });
    vm.init = function() {
        
    }
    vm.init();

});