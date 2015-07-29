angular.module('mainApp.core.controllers.MainViewController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.config.services.ConfigService',
    'mainApp.core.controllers.MainHeaderController',
    'mainApp.config.controllers.ManageCredentialsController',
    'mainApp.login.controllers.UpdatePasswordController',
    'mainApp.userManagement.controllers.UserManagementController',
    'mainApp.models.controllers.AdminInfoController',
    'mainApp.models.controllers.ModelListController',
    'mainApp.models.controllers.ModelDetailController',
    'mainApp.models.controllers.ModelCreationHistoryController',
    'mainApp.models.controllers.MultipleModelSetupController'
])

.controller('MainViewController', function ($scope, $http, $rootScope, $compile, ResourceUtility, BrowserStorageUtility, TimestampIntervalUtility, NavUtility, ConfigService) {
    $scope.ResourceUtility = ResourceUtility;

    if ($scope.isLoggedInWithTempPassword || $scope.isPasswordOlderThanNinetyDays) {
        createUpdatePasswordView();
    } else {
        createModelListView();
    }

    // Handle Initial View
    $http.get('./app/core/views/MainHeaderView.html').success(function (html) {
        var scope = $rootScope.$new();
        scope.mandatePasswordChange = $scope.isLoggedInWithTempPassword || $scope.isPasswordOlderThanNinetyDays;
        $compile($("#mainHeaderView").html(html))(scope);
    });

    // Handle when the Manage Credentials link is clicked
    $scope.$on(NavUtility.MANAGE_CREDENTIALS_NAV_EVENT, function (event, data) {
        createManageCredentialsView();
    });
    
    function createManageCredentialsView() {
        // Set the hash
        window.location.hash = NavUtility.MANAGE_CREDENTIALS_HASH;
        
        // Fetch the view and make it Angular aware
        $http.get('./app/config/views/ManageCredentialsView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }
    
    // Handle when the Update Password link is clicked
    $scope.$on(NavUtility.UPDATE_PASSWORD_NAV_EVENT, function (event, data) {
        if (data != null && data.Success) {
            createUpdatePasswordSuccessView();
        } else {
            createUpdatePasswordView();
        }
    });
    
    function createUpdatePasswordView() {
        // Set the hash
        window.location.hash = NavUtility.UPDATE_PASSWORD_HASH;

        // Fetch the view and make it Angular aware
        $http.get('./app/login/views/UpdatePasswordView.html').success(function (html) {
            var scope = $rootScope.$new();
            scope.isLoggedInWithTempPassword = $scope.isLoggedInWithTempPassword;
            scope.isPasswordOlderThanNinetyDays = $scope.isPasswordOlderThanNinetyDays;
            $compile($("#mainContentView").html(html))(scope);
        });
    }

    function createUpdatePasswordSuccessView() {
        // Set the hash
        window.location.hash = NavUtility.UPDATE_PASSWORD_HASH;
        $http.get('./app/login/views/UpdatePasswordSuccessView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }

    // Handle when the User Management link is clicked
    $scope.$on(NavUtility.USER_MANAGEMENT_NAV_EVENT, function (event, data) {
        createUserManagementView();
    });
    
    function createUserManagementView() {
        // Set the hash
        window.location.hash = NavUtility.USER_MANAGEMENT_HASH;
        
        // Fetch the view and make it Angular aware
        $http.get('./app/userManagement/views/UserManagementView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }

    // Handle when the User Management link is clicked
    $scope.$on(NavUtility.ADMIN_INFO_NAV_EVENT, function (event, data) {
        createAdminInfoView(data);
    });

    function createAdminInfoView(data) {
        // Set the hash
        window.location.hash = NavUtility.ADMIN_INFO_HASH;

        // Fetch the view and make it Angular aware
        $http.get('./app/models/views/AdminInfoView.html').success(function (html) {
            var scope = $rootScope.$new();
            scope.data = data;
            $compile($("#mainContentView").html(html))(scope);
        });
    }
    
    $scope.$on(NavUtility.MODEL_CREATION_HISTORY_NAV_EVENT, function (event, data) {
        modelCreationHistoryView();
    });
    
    function modelCreationHistoryView() {
        window.location.hash = NavUtility.MODEL_CREATION_HISTORY_HASH;
        
        $http.get('./app/models/views/ModelCreationHistoryView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }
    
    // Handle when the Model List link is clicked
    $scope.$on(NavUtility.MODEL_LIST_NAV_EVENT, function (event, data) {
        createModelListView();
    });
    
    function createModelListView() {
        // Set the hash
        window.location.hash = NavUtility.MODEL_LIST_HASH;
        
        // Fetch the view and make it Angular aware
        $http.get('./app/models/views/ModelListView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }
    
    // Handle when the Model List link is clicked
    $scope.$on(NavUtility.MODEL_DETAIL_NAV_EVENT, function (event, data) {
        createModelDetailView(data);
    });
    
    function createModelDetailView(data) {
        // Set the hash
        window.location.hash = NavUtility.MODEL_DETAIL_HASH;
        
        // Fetch the view and make it Angular aware
        $http.get('./app/models/views/ModelDetailView.html').success(function (html) {
            var scope = $rootScope.$new();
            scope.data = data;
            $compile($("#mainContentView").html(html))(scope);
        });
    }
    
    // Handle when the Update Password link is clicked
    $scope.$on(NavUtility.MULTIPLE_MODEL_SETUP, function (event, data) {
        createMultipleModelSetupView();
    });
    
    function createMultipleModelSetupView() {
        // Set the hash
        window.location.hash = NavUtility.MULTIPLE_MODEL_SETUP;
        
        // Fetch the view and make it Angular aware
        $http.get('./app/models/views/MultipleModelSetupView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }
});