//Initial load of the application    
var mainApp = angular.module('mainApp', [
    'mainApp.appCommon.utilities.EvergageUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.services.ResourceStringsService',
    'mainApp.login.services.LoginService',
    'mainApp.core.services.SessionService',
    'mainApp.config.services.ConfigService',
    'mainApp.login.controllers.LoginController',
    'mainApp.core.controllers.MainViewController',
    'mainApp.login.controllers.LoginController'
])

.controller('MainController', function ($scope, $http, $rootScope, $compile, BrowserStorageUtility, ResourceUtility,
    EvergageUtility, ResourceStringsService, LoginService, SessionService, ConfigService) {
    
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
            function (data, status) {
                if (data && data.Success === true) {
                    //Initialize Evergage
                    EvergageUtility.Initialize({
                        userID: data.Result.User.Identifier, 
                        title: data.Result.User.Title,
                        datasetPrefix: "pls",
                        company: data.Ticket.Tenants[0].DisplayName
                    });
                    
                    $scope.getLocaleSpecificResourceStrings(data.Result.User.Locale);
                }
            },
            
            // Fail
            function (data, status) {
                
            }
        );
    };
    
    $scope.getLocaleSpecificResourceStrings = function (locale) {
        ResourceStringsService.GetResourceStrings(locale).then(function(result) {
            $scope.copyrightString = ResourceUtility.getString('FOOTER_COPYRIGHT', [(new Date()).getFullYear()]);
            $scope.privacyPolicyString = ResourceUtility.getString('HEADER_PRIVACY_POLICY');
            $scope.getWidgetConfigDoc();
        });
    };
    
    $scope.getWidgetConfigDoc = function () {
        ConfigService.GetWidgetConfigDocument().then(function(result) {
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