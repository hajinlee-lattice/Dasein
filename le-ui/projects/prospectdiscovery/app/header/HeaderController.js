angular.module('mainApp.core.controllers.MainHeaderController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.login.services.LoginService',
    'mainApp.core.services.FeatureFlagService'
])

.controller('MainHeaderController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, NavUtility, LoginService, FeatureFlagService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.showUserManagement = false;
console.log('ResourceUtility', $scope.ResourceUtility);
    var clientSession = BrowserStorageUtility.getClientSession();

    if (clientSession != null) {
        FeatureFlagService.GetAllFlags().then(function() {
            var flags = FeatureFlagService.Flags();
            $scope.userDisplayName = clientSession.DisplayName;
            $scope.showUserManagement = FeatureFlagService.FlagIsEnabled(flags.USER_MGMT_PAGE);
            $scope.showSystemSetup = FeatureFlagService.FlagIsEnabled(flags.SYSTEM_SETUP_PAGE);
            $scope.showModelCreationHistoryDropdown = FeatureFlagService.FlagIsEnabled(flags.MODEL_HISTORY_PAGE);
            $scope.showActivateModel = FeatureFlagService.FlagIsEnabled(flags.ACTIVATE_MODEL_PAGE);
            $scope.showSetup = FeatureFlagService.FlagIsEnabled(flags.SETUP_PAGE);
        });
    }

    $scope.handleClick = function ($event, name) {
        $event ? $event.preventDefault() : null;

        switch(name) {
            case 'dropdown': 
                break;
            case 'logout': 
                LoginService.Logout(); 
                return;
            default:
                break;
        }

        name ? $rootScope.$broadcast(NavUtility[name]) : null;
    };
});