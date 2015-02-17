angular.module('mainApp.core.controllers.MainViewController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.core.services.HelpService',
    'mainApp.config.services.GriotConfigService',
    'mainApp.core.controllers.MainHeaderController',
    'mainApp.config.controllers.ManageCredentialsController',
    'mainApp.login.controllers.UpdatePasswordController',
    'mainApp.userManagement.controllers.UserManagementController',
    'mainApp.models.controllers.ModelListController',
    'mainApp.models.controllers.ModelDetailController'
])

.controller('MainViewController', function ($scope, $http, $rootScope, $compile, ResourceUtility, BrowserStorageUtility, GriotNavUtility, HelpService, GriotConfigService) {
    $scope.copyrightString = ResourceUtility.getString('FOOTER_COPYRIGHT', [(new Date()).getFullYear()]);
    $scope.ResourceUtility = ResourceUtility;
    var directToPassword = $scope.directToPassword;
    if (directToPassword) {
        createUpdatePasswordView();
    } else {
        createModelListView();
    }
    
    // Handle Initial View
    $http.get('./app/core/views/MainHeaderView.html').success(function (html) {
        var scope = $rootScope.$new();
        $compile($("#mainHeaderView").html(html))(scope);
    });
    
    $scope.privacyPolicyClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        HelpService.OpenPrivacyPolicy();
    };
    
    // Handle when the Manage Credentials link is clicked
    $scope.$on(GriotNavUtility.MANAGE_CREDENTIALS_NAV_EVENT, function (event, data) {
        createManageCredentialsView();
    });
    
    function createManageCredentialsView() {
        // Set the hash
        window.location.hash = GriotNavUtility.MANAGE_CREDENTIALS_HASH;
        
        // Fetch the view and make it Angular aware
        $http.get('./app/config/views/ManageCredentialsView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }
    
    // Handle when the Update Password link is clicked
    $scope.$on(GriotNavUtility.UPDATE_PASSWORD_NAV_EVENT, function (event, data) {
        createUpdatePasswordView();
    });
    
    function createUpdatePasswordView() {
        // Set the hash
        window.location.hash = GriotNavUtility.UPDATE_PASSWORD_HASH;
        
        // Fetch the view and make it Angular aware
        $http.get('./app/login/views/UpdatePasswordView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }
    
    // Handle when the User Management link is clicked
    $scope.$on(GriotNavUtility.USER_MANAGEMENT_NAV_EVENT, function (event, data) {
        createUserManagementView();
    });
    
    function createUserManagementView() {
        // Set the hash
        window.location.hash = GriotNavUtility.USER_MANAGEMENT_HASH;
        
        // Fetch the view and make it Angular aware
        $http.get('./app/userManagement/views/UserManagementView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }
    
    // Handle when the Model List link is clicked
    $scope.$on(GriotNavUtility.MODEL_LIST_NAV_EVENT, function (event, data) {
        createModelListView();
    });
    
    function createModelListView() {
        // Set the hash
        window.location.hash = GriotNavUtility.MODEL_LIST_HASH;
        
        // Fetch the view and make it Angular aware
        $http.get('./app/models/views/ModelListView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }
    
    // Handle when the Model List link is clicked
    $scope.$on(GriotNavUtility.MODEL_DETAIL_NAV_EVENT, function (event, data) {
        createModelDetailView(data);
    });
    
    function createModelDetailView(data) {
        // Set the hash
        window.location.hash = GriotNavUtility.MODEL_DETAIL_HASH;
        
        // Fetch the view and make it Angular aware
        $http.get('./app/models/views/ModelDetailView.html').success(function (html) {
            var scope = $rootScope.$new();
            scope.data = data;
            $compile($("#mainContentView").html(html))(scope);
        });
    }
});