angular.module('mainApp.core.controllers.MainHeaderController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.login.services.LoginService',
    'mainApp.core.services.FeatureFlagService'
])

.controller('MainHeaderController', function ($scope, $rootScope, $document, ResourceUtility, BrowserStorageUtility, NavUtility, LoginService, FeatureFlagService) {
    $scope.ResourceUtility = ResourceUtility;
    var clientSession = BrowserStorageUtility.getClientSession();
    if (clientSession != null) {
        $scope.userDisplayName = clientSession.DisplayName;
    }

    checkBrowserWidth();
    $(window).resize(checkBrowserWidth);
    $scope.showProfileNav = false;

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

    function checkBrowserWidth(){
        if (window.matchMedia("(min-width: 1200px)").matches) {
            $("body").addClass("open-nav");
        } else {
            $("body").removeClass("open-nav");
        }
    }
});