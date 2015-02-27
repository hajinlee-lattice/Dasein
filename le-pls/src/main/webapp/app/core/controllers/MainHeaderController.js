angular.module('mainApp.core.controllers.MainHeaderController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.login.services.LoginService'
])

.controller('MainHeaderController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, RightsUtility, GriotNavUtility, LoginService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.showUserManagement = false;

    var clientSession = BrowserStorageUtility.getClientSession();
    if (clientSession == null) {
        return;
    }

    $scope.userDisplayName = clientSession.DisplayName;
    $scope.showUserManagement = RightsUtility.canSeeUserManagement(clientSession.availableRights);

    $scope.dropdownClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
    };
    
    $scope.modelListClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        $rootScope.$broadcast(GriotNavUtility.MODEL_LIST_NAV_EVENT);
    };
    
    $scope.userManagementClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        $rootScope.$broadcast(GriotNavUtility.USER_MANAGEMENT_NAV_EVENT);
    };
    
    $scope.updatePasswordClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        $rootScope.$broadcast(GriotNavUtility.UPDATE_PASSWORD_NAV_EVENT);
    };
    
    $scope.manageCredentialsClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        $rootScope.$broadcast(GriotNavUtility.MANAGE_CREDENTIALS_NAV_EVENT);
    };
    
    $scope.logoutClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        LoginService.Logout(); 
    };
});