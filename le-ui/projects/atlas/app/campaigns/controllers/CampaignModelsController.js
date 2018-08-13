angular.module('lp.campaigns.models', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('CampaignModelsController', function ($stateParams,
    BrowserStorageUtility, ResourceUtility, CampaignStore) {

    var vm = this;
    angular.extend(vm, {
        campaignId: $stateParams.campaignId
    });

    vm.init = function() {

    }

    vm.init();
});