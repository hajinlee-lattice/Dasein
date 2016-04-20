angular.module('mainApp.core.controllers.MainHeaderController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.login.services.LoginService',
    'mainApp.core.services.FeatureFlagService'
])

.controller('MainHeaderController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, NavUtility, LoginService, FeatureFlagService) {
    $scope.ResourceUtility = ResourceUtility;
    var clientSession = BrowserStorageUtility.getClientSession();
    if (clientSession != null) {
        $scope.userDisplayName = clientSession.DisplayName;
    }

    checkBrowserWidth();
    $(window).resize(checkBrowserWidth);

    $scope.handleSidebarToggle = function ($event) {
        $("body").toggleClass("open-nav");
    }

    $('html').click(function() {
        if ($scope.showProfileNav) {
            $scope.showProfileNav = false;
        }
    });

    function checkBrowserWidth(){
        if (window.matchMedia("(min-width: 1200px)").matches) {
            $("body").addClass("open-nav");
        } else {
            $("body").removeClass("open-nav");
        }
    }
});