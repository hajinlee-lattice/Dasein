angular.module('lp.campaigns.list', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.widgets.CampaignListTileWidget',
    'mainApp.campaigns.modals.CreateCampaignModal'
])
.controller('CampaignListController', function (
    ResourceUtility, CampaignList, CampaignStore, CreateCampaignModal
) {
    var vm = this;

    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        campaigns: CampaignList || []
    },{  
        init: function() {
            CampaignStore.getCampaigns();
        }

    });

    vm.showCreateCampaignModal = function ($event) {
        
        console.log("create campaign");
        if ($event != null) {
            $event.stopPropagation();
        }
        CreateCampaignModal.show();
    };

    vm.init();
});