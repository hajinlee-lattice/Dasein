angular
    .module('pd.navigation.sidebar', [
        'mainApp.appCommon.utilities.ResourceUtility',
        'mainApp.appCommon.utilities.StringUtility',
        'mainApp.core.services.FeatureFlagService'
    ])
    .controller('SidebarRootController', function($scope, $state, FeatureFlagService, ResourceUtility) {
        $scope.$state = $state;
        $scope.ResourceUtility = ResourceUtility;

        $scope.handleSidebarToggle = function ($event) {
            $("body").toggleClass("open-nav");
            $("body").addClass("controlled-nav");  // indicate the user toggled the nav
        }
        
        FeatureFlagService.GetAllFlags().then(function() {
            var flags = FeatureFlagService.Flags();
            $scope.showUserManagement = FeatureFlagService.FlagIsEnabled(flags.USER_MGMT_PAGE);
            $scope.showModelCreationHistory = FeatureFlagService.FlagIsEnabled(flags.MODEL_HISTORY_PAGE);
            $scope.showApiConsole = FeatureFlagService.FlagIsEnabled(flags.API_CONSOLE_PAGE);
        });
    });
