angular.module('mainApp.core.controllers.MainHeaderController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.login.services.LoginService',
    'mainApp.core.services.FeatureFlagService'
])
.controller('MainHeaderController', function ($scope, $rootScope, $routeParams, $document, ResourceUtility, BrowserStorageUtility, NavUtility, LoginService, FeatureFlagService) {
    
    $scope.ResourceUtility = ResourceUtility;

    var clientSession = BrowserStorageUtility.getClientSession();
    if (clientSession != null) {
        $scope.userDisplayName = clientSession.DisplayName;
    }
    $scope.showProfileNav = false;

    
    $rootScope.$on('$stateChangeSuccess', function(e, toState, toParams, fromState, fromParams) {
        if (isModelDetailState(fromState.name) && ! isModelDetailState(toState.name)) {
            $scope.isModelDetailsPage = false;
        }
    });

    function isModelDetailState(stateName) {
        var stateNameArr = stateName.split('.');
        if (stateNameArr[0] == 'home' && stateNameArr[1] == 'model') {
            return true;
        }
        return false;
    }

    $scope.$on('model-details', function(event, args) {
        $scope.isModelDetailsPage = true;
        $scope.displayName = args.displayName;
    });

    
    checkBrowserWidth();
    $(window).resize(checkBrowserWidth);
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