angular.module('mainApp.core.controllers.MainHeaderController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.login.services.LoginService'
])

.controller('MainHeaderController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, RightsUtility, NavUtility, LoginService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.showUserManagement = false;

    var clientSession = BrowserStorageUtility.getClientSession();
    if (clientSession != null) {
        $scope.userDisplayName = clientSession.DisplayName;
        $scope.showUserManagement = RightsUtility.maySeeUserManagement();
        $scope.showSystemSetup =  RightsUtility.maySeeSystemSetup();
        $scope.showModelCreationHistoryDropdown = RightsUtility.maySeeModelCreationHistory();
        $scope.showActivateModel = RightsUtility.mayEditActivateModel();
    }

    var featureFlags = BrowserStorageUtility.getFeatureFlagsDocument();
    if (featureFlags) {
        if (featureFlags.hasOwnProperty("ActivateModel")) {
            $scope.showActivateModel = $scope.showActivateModel && featureFlags['ActivateModel'];
        }
        if (featureFlags.hasOwnProperty("SystemSetup")) {
            $scope.showSystemSetup = $scope.showSystemSetup && featureFlags['SystemSetup'];
        }
    }

    $scope.dropdownClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
    };
    
    $scope.modelListClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        $rootScope.$broadcast(NavUtility.MODEL_LIST_NAV_EVENT);
    };
    
    $scope.userManagementClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        $rootScope.$broadcast(NavUtility.USER_MANAGEMENT_NAV_EVENT);
    };

    $scope.systemSetupClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $rootScope.$broadcast(NavUtility.MANAGE_CREDENTIALS_NAV_EVENT);
    };

    $scope.showModelCreationHistory = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        $rootScope.$broadcast(NavUtility.MODEL_CREATION_HISTORY_NAV_EVENT);
    };
    
    $scope.updatePasswordClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        $rootScope.$broadcast(NavUtility.UPDATE_PASSWORD_NAV_EVENT);
    };
    
    $scope.manageCredentialsClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        $rootScope.$broadcast(NavUtility.MANAGE_CREDENTIALS_NAV_EVENT);
    };
    
    $scope.activateModelClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        $rootScope.$broadcast(NavUtility.ACTIVATE_MODEL);
    };
    
    $scope.logoutClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        LoginService.Logout(); 
    };
});