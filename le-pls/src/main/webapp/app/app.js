//Initial load of the application    
var mainApp = angular.module('mainApp', [
    'mainApp.appCommon.utilities.EvergageUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.services.ResourceStringsService',
    'mainApp.login.services.LoginService',
    'mainApp.core.services.SessionService',
    'mainApp.config.services.GriotConfigService',
    'mainApp.login.controllers.LoginController',
    'mainApp.core.controllers.MainViewController',
    'mainApp.login.controllers.LoginController'
])

.controller('MainController', function ($scope, $http, $rootScope, $compile, BrowserStorageUtility, 
    EvergageUtility, ResourceStringsService, LoginService, SessionService, GriotConfigService) {
    
    ResourceStringsService.GetResourceStrings().then(function(result) {
        
        var previousSession = BrowserStorageUtility.getClientSession();
        if (previousSession != null) {
            $scope.refreshPreviousSession(previousSession.Tenant);
        } else {
            
            // Create the Login View
            $http.get('./app/login/views/LoginView.html').success(function (html) {
                var scope = $rootScope.$new();
                $compile($("#mainView").html(html))(scope);
            });
        }
        
    });
    
    $scope.refreshPreviousSession = function (tenant) {
        //Refresh session and go somewhere
        LoginService.GetSessionDocument(tenant).then(
            // Success
            function (data) {
                if (data && data.Success === true) {
                    //Initialize Evergage
                    EvergageUtility.Initialize({
                        userID: data.Result.User.Identifier, 
                        title: data.Result.User.Title,
                        datasetPrefix: "pls"
                    });
                    
                    $scope.getLocaleSpecificResourceStrings(data.Result.User.Locale);
                } else {
                   SessionService.ClearSession();
                }
            },
            
            // Fail
            function (data) {
                SessionService.ClearSession();
            }
        );
    };
    
    $scope.getLocaleSpecificResourceStrings = function (locale) {
        ResourceStringsService.GetResourceStrings(locale).then(function(result) {
            $scope.getWidgetConfigDoc();
        });
    };
    
    //TODO:pierce Add this back when we can configure credentials in PLS
    /*$scope.getConfigDoc = function () {
        GriotConfigService.GetConfigDocument().then(function(result) {
            $scope.getWidgetConfigDoc();
        });
    };*/
    
    $scope.getWidgetConfigDoc = function () {
        GriotConfigService.GetWidgetConfigDocument().then(function(result) {
            $http.get('./app/core/views/MainView.html').success(function (html) {
                var scope = $rootScope.$new();
                $compile($("#mainView").html(html))(scope);
            });
        });
    };
});

mainApp.factory('authInterceptor', function ($rootScope, $q, $window, BrowserStorageUtility) {
  return {
    request: function (config) {
      config.headers = config.headers || {};
      if (BrowserStorageUtility.getTokenDocument()) {
        config.headers.Authorization = BrowserStorageUtility.getTokenDocument();
      }
      return config;
    },
    response: function (response) {
      if (response.status === 401) {
        // handle the case where the user is not authenticated
      }
      return response || $q.when(response);
    }
  };
});

mainApp.config(function ($httpProvider) {
  $httpProvider.interceptors.push('authInterceptor');
});