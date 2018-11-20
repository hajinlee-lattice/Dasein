angular.module('lp.campaigns', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.campaigns.modals.DeleteCampaignModal'
])
.controller('CreateCampaignController', ['$scope', '$state', '$stateParams', 'BrowserStorageUtility', 'ResourceUtility', 'CampaignService',
    function($scope, $state, $stateParams, BrowserStorageUtility, ResourceUtility, CampaignService){

    var vm = this;

    angular.extend(vm, {
        campaignName: '',
        campaignDescription: '',
        saveInProgress: false,
        addCampaignErrorMessage: "",
        showAddCampaignError: false
    });
    
}])
.controller('CampaignListController', ['Campaigns', 'CampaignService', 'DeleteCampaignModal', 'ResourceUtility', function(Campaigns, CampaignService, DeleteCampaignModal, ResourceUtility) {
    var vm = this;

    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        campaigns: Campaigns
    });


    vm.showCustomMenu = false;
    vm.customMenuClick = function ($event) {
        if ($event != null) {
            $event.stopPropagation();
        }
        
        $scope.showCustomMenu = !$scope.showCustomMenu;

        if ($scope.showCustomMenu) {
            $(document).bind('click', function(event){
                var isClickedElementChildOfPopup = $element
                    .find(event.target)
                    .length > 0;

                if (isClickedElementChildOfPopup)
                    return;

                $scope.$apply(function(){
                    $scope.showCustomMenu = false;
                    $(document).unbind(event);
                });
            });
        }
    };

    /* Navigate to Campaign Detail page */
    vm.tileClick = function ($event) {
        $event.preventDefault();
    };


    vm.showCreateCampaignClicked = function($event) {
        vm.showCreateCampaignForm = true;
    };
    vm.createCampaignClicked = function() {
        
        vm.saveInProgress = true;

        var campaign = {
            campaignName: vm.campaignName,
            campaignDescription: vm.campaignDescription
        };

        CampaignService.CreateCampaign(campaign).then(function(result){

            if (result != null && result.success === true) {
                $state.go('home.campaigns', {}, { reload: true });
            } else {
                vm.saveInProgress = false;
                vm.addCampaignErrorMessage = result;
                vm.showAddCampaignError = true;
            }

        });

    };

    vm.cancelCreateCampaignClicked = function(){
        vm.showCreateCampaignForm = false;     
    };

    vm.deleteCampaignClicked = function(campaignId) {
        DeleteCampaignModal.show(campaignId);
    }

}]);