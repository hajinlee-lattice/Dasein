angular.module('mainApp.core.controllers.MainHeaderController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.services.FeatureFlagService'
])
.controller('MainHeaderController', function ($scope, $rootScope, $state, ResourceUtility, BrowserStorageUtility, FeatureFlagService) {
    $scope.ResourceUtility = ResourceUtility;

    var clientSession = BrowserStorageUtility.getClientSession();
    
    if (clientSession != null) {
        var Tenant = clientSession ? clientSession.Tenant : null;

        $scope.userDisplayName = clientSession.DisplayName;
        $scope.tenantName = window.escape(Tenant.DisplayName);
    }

    $scope.showProfileNav = false;

    $rootScope.$on('$stateChangeSuccess', function(e, toState, toParams, fromState, fromParams) {
        if (toState.params) {
            $scope.pageDisplayIcon = toState.params.pageIcon ? toState.params.pageIcon : null;
            $scope.pageDisplayName = toState.params.pageTitle ? toState.params.pageTitle : null;
        }
        
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
        $scope.modelDisplayName = args.displayName;
    });
    
    checkBrowserWidth();

    $(window).resize(checkBrowserWidth);

    $scope.handleSidebarToggle = function ($event) {
        $("body").toggleClass("open-nav");
        $("body").addClass("controlled-nav");  // indicate the user toggled the nav
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
      // if the user has closed the nav, leave it closed when increasing size
      if (window.matchMedia("(min-width: 1200px)").matches && !$("body").hasClass("controlled-nav")) {
        if (typeof(sessionStorage) !== 'undefined') {
            if(sessionStorage.getItem('open-nav') === 'true') {
                $("body").addClass('open-nav');
            } else {
                $("body").removeClass('open-nav');
            }
        }
      } else {
        if($("body").hasClass("open-nav")) {
          // if the nav is open when scaling down close it but allow it to re-open by removing our user controlled class indicator
          $("body").removeClass("controlled-nav");
        }
        $("body").removeClass("open-nav");
      }
    }
});
