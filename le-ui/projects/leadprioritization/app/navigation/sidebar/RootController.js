angular
    .module('pd.navigation.sidebar', ['mainApp.core.services.FeatureFlagService'])
    .controller('SidebarRootController', function($scope, FeatureFlagService) {
        FeatureFlagService.GetAllFlags().then(function() {
            var flags = FeatureFlagService.Flags();
            $scope.showUserManagement = FeatureFlagService.FlagIsEnabled(flags.USER_MGMT_PAGE);
            $scope.showModelCreationHistory = FeatureFlagService.FlagIsEnabled(flags.MODEL_HISTORY_PAGE);
        });
    });