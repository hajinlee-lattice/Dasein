angular.module('lp.campaigns.list', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.widgets.CampaignListTileWidget',
    'mainApp.campaigns.modals.CreateCampaignModal'
])
.controller('CampaignListController', function (
    ResourceUtility, CampaignList, CampaignStore, CreateCampaignModal
) {
    var vm = this;

    vm.createClicked = false;

    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        campaigns: CampaignList || []
    },{  
        init: function() {
            CampaignStore.getCampaigns();
        }

    });

    vm.createCampaignClick = function ($event) {
        if ($event != null) {
            $event.stopPropagation();
        }
        vm.createClicked = true;



    };
    vm.createCampaignCancelClick = function ($event) {
        if ($event != null) {
            $event.stopPropagation();
        }
        vm.createClicked = false;
    };



    // This needs to be moved over to CampaignTileListWidget.js When ready
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
    vm.tileClick = function ($event) {
        $event.preventDefault();
    };




    vm.init();
});