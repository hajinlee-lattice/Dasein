//Initial load of the application    
var mainApp = angular.module('mainApp', [
    'templates-main',
    'ui.router',
    'ui.bootstrap',
    /*
    'pd.navigation'
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.core.services.ResourceStringsService',
    'mainApp.core.services.HelpService',
    'mainApp.login.services.LoginService',
    'mainApp.config.services.ConfigService',
    'mainApp.core.controllers.MainViewController',
    */
    'mainApp.login.controllers.LoginController'
])
.config(function($stateProvider, $urlRouterProvider, $locationProvider) {
    //$locationProvider.html5Mode(true);
    $urlRouterProvider.otherwise('/');

    $stateProvider
        .state('login', {
            url: '/',
            resolve: {
                /*
                WidgetConfig: function($q, ConfigService) {
                    var deferred = $q.defer();

                    ConfigService.GetWidgetConfigDocument().then(function(result) {
                        deferred.resolve();
                    });

                    return deferred.promise;
                },
                FeatureFlags: function($q, FeatureFlagService) {
                    var deferred = $q.defer();
                    
                    FeatureFlagService.GetAllFlags().then(function() {
                        deferred.resolve();
                    });
                    
                    return deferred.promise;
                },
                */
                ResourceStrings: function($q, BrowserStorageUtility, ResourceStringsService) {
                    var deferred = $q.defer(),
                        session = BrowserStorageUtility.getClientSession();

                    ResourceStringsService.GetExternalResourceStringsForLocale().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                "header": {
                    template: ''
                },
                "main": {
                    controller: 'LoginController',
                    templateUrl: 'app/views/LoginView.html'
                }
            }
        })
        .state('login.tenants', {
            url: '/tenants',
            views: {
                "header": {
                    controller: 'MainHeaderController',
                    templateUrl: 'app/views/MainHeaderView.html'
                },
                "main": {
                    template: 'tenants'
                }
            }
        })
        .state('login.password.update', {
            url: '/password/update',
            views: {
                "main": {
                    template: 'password update'
                }
            }
        })
        .state('login.password.forgot', {
            url: '/password/forgot',
            views: {
                "main": {
                    template: 'password forgot'
                }
            }
        });
})
.config(function($httpProvider) {
    if (!$httpProvider.defaults.headers.get) {
        $httpProvider.defaults.headers.get = {};    
    }

    $httpProvider.defaults.headers.get['If-Modified-Since'] = 'Mon, 26 Jul 1997 05:00:00 GMT';
    $httpProvider.defaults.headers.get['Cache-Control'] = 'no-cache';
    $httpProvider.defaults.headers.get['Pragma'] = 'no-cache';
})
.config(function ($httpProvider) {
    $httpProvider.interceptors.push('authInterceptor');
})
.factory('authInterceptor', function ($rootScope, $q, $window, BrowserStorageUtility) {
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
})
.controller('MainController', function() {
});