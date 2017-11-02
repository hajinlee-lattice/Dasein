angular.module('lp.playbook.playlisttabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('PlayListTabsController', function ($filter, ResourceUtility) {
    var vm = this;

    angular.extend(vm, {
        ResourceUtility: ResourceUtility
    });

});