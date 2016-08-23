//Initial load of the application    
var mainApp = angular.module('mainApp', [
    'templates-main',
    'ui.router',
    'ui.bootstrap',

    'login.frame',
    'login.form',
    'login.update',
    'login.forgot',
    'login.tenants'
])
.config(function($stateProvider, $urlRouterProvider, $locationProvider) {
    //$locationProvider.html5Mode(true);
    $urlRouterProvider.otherwise('/form');

    $stateProvider
        .state('login', {
            url: '/',
            resolve: {
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
                    resolve: {
                        LoginDocument: function(BrowserStorageUtility) {
                            return BrowserStorageUtility.getLoginDocument() || {};
                        },
                        ClientSession: function(BrowserStorageUtility) {
                            return BrowserStorageUtility.getClientSession() || {};
                        }
                    },
                    controller: 'LatticeFrameController',
                    templateUrl: 'app/login/frame/LatticeFrameView.html'
                }
            }
        })
        .state('login.form', {
            url: 'form',
            views: {
                "FrameContent": {
                    controller: 'LoginViewController',
                    templateUrl: 'app/login/form/LoginFormView.html'
                }
            }
        })
        .state('login.forgot', {
            url: 'forgot',
            views: {
                "FrameContent": {
                    controller: 'PasswordForgotController',
                    templateUrl: 'app/login/forgot/PasswordForgotView.html'
                }
            }
        })
        .state('login.update', {
            url: 'update',
            views: {
                "FrameContent": {
                    resolve: {
                        LoginDocument: function(BrowserStorageUtility) {
                            return BrowserStorageUtility.getLoginDocument() || {};
                        }
                    },
                    controller: 'PasswordUpdateController',
                    templateUrl: 'app/login/update/PasswordUpdateView.html'
                }
            }
        })
        .state('login.tenants', {
            url: 'tenants',
            views: {
                "FrameContent": {
                    resolve: {
                        LoginDocument: function(BrowserStorageUtility) {
                            return BrowserStorageUtility.getLoginDocument() || {};
                        },
                        TenantList: function(LoginDocument) {
                            return LoginDocument.Tenants || [];
                        }
                    },
                    controller: 'TenantSelectController',
                    templateUrl: 'app/login/tenants/TenantSelectView.html'
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
.controller('MainController', function($rootScope, $state) {
    $rootScope.$on('$stateChangeError', 
        function(event, toState, toParams, fromState, fromParams, error){ 
                // this is required if you want to prevent the $UrlRouter reverting the URL to the previous valid location
                event.preventDefault();
                $state.go('login.form');
        })
});