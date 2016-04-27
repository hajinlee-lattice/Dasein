angular.module('mainApp.core.controllers.MainHeaderController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.login.services.LoginService',
    'mainApp.core.services.FeatureFlagService'
])
.controller('MainHeaderController', function ($scope, $rootScope, $location, $routeParams, $document, ResourceUtility, BrowserStorageUtility, NavUtility, LoginService, FeatureFlagService) {
    $scope.ResourceUtility = ResourceUtility;

    var clientSession = BrowserStorageUtility.getClientSession();
    if (clientSession != null) {
        $scope.userDisplayName = clientSession.displayName;
    }
    $scope.showProfileNav = false;

    
    // $rootScope.$on('$routeChangeSuccess', function(e, current, pre) {
    //   console.log('Current route name: ' + $location.path());
    //   // Get all URL parameter
    //   console.log($routeParams);
    // });
    // $scope.isModelDetailsPage = true;

    $scope.handleSidebarToggle = function ($event) {
        $("body").toggleClass("open-nav");
    }

    $(document.body).click(function() {
        if ($scope.showProfileNav) {
            $scope.showProfileNav = false;
            $scope.$apply();
        }
    });

    $scope.headerClicked = function($event) {
        $scope.showProfileNav = !$scope.showProfileNav;
        $event.stopPropagation();
    };

});