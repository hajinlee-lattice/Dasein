angular.module('mainApp.core.controllers.MainHeaderController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.login.services.LoginService'
])

.controller('MainHeaderController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, GriotNavUtility, LoginService) {
    $scope.ResourceUtility = ResourceUtility;
    
    var clientSession = BrowserStorageUtility.getClientSession();
    if (clientSession == null) {
        return;
    }
    
    $scope.userDisplayName = clientSession.DisplayName;
    
    // Only show the Models tab if the credentials have been filled out
    var configDoc = BrowserStorageUtility.getConfigDocument();
    $scope.shouldShowModels = configDoc != null && configDoc.CrmApiCredentials != null && configDoc.MapApiCredentials != null;
    
    $scope.$on(GriotNavUtility.SYSTEM_CONFIGURED_COMPLETE_EVENT, function (event, data) {
        $scope.shouldShowModels = true;
    });
    
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