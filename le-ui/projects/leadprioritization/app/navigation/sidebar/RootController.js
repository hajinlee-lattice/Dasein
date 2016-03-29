angular
    .module('pd.navigation.sidebar', [
        'mainApp.appCommon.utilities.ResourceUtility',
        'mainApp.appCommon.utilities.StringUtility',
        'mainApp.core.services.FeatureFlagService'
    ])
    .controller('SidebarRootController', function($scope, FeatureFlagService, ResourceUtility) {
        $scope.ResourceUtility = ResourceUtility;
        console.log(ResourceUtility);
        FeatureFlagService.GetAllFlags().then(function() {
            var flags = FeatureFlagService.Flags();
            $scope.showUserManagement = FeatureFlagService.FlagIsEnabled(flags.USER_MGMT_PAGE);
            $scope.showModelCreationHistory = FeatureFlagService.FlagIsEnabled(flags.MODEL_HISTORY_PAGE);
        });
    });