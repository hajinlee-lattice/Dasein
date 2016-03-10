angular
    .module('pd.navigation.sidebar', ['mainApp.core.services.FeatureFlagService'])
    .controller('SidebarRootController', function($scope, FeatureFlagService) {
        FeatureFlagService.GetAllFlags().then(function() {
            var flags = FeatureFlagService.Flags();
            $scope.showCreateModel = FeatureFlagService.FlagIsEnabled(flags.UPLOAD_JSON);
            $scope.showUserManagement = FeatureFlagService.FlagIsEnabled(flags.USER_MGMT_PAGE);
            $scope.showModelCreationHistory = FeatureFlagService.FlagIsEnabled(flags.MODEL_HISTORY_PAGE);
            $scope.showJobs = FeatureFlagService.FlagIsEnabled(flags.JOBS_PAGE);
            $scope.showMarketoSettings = FeatureFlagService.FlagIsEnabled(flags.MARKETO_SETTINGS_PAGE);
        });
    });