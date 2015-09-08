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

    $scope.setupClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $rootScope.$broadcast(NavUtility.SETUP_NAV_EVENT);
    };

    $scope.logoutClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        LoginService.Logout();
    };
});