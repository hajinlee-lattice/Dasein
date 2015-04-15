var app = angular.module("TenantConsoleApp", [
    "app.core.directive.MainNavDirective",
    "app.login.controller.LoginCtrl",
    "app.tenants.controller.TenantListCtrl",
    "app.tenants.controller.TenantConfigCtrl",
    "ui.router",
    'LocalStorageModule',
    'le.common.util.BrowserStorageUtility'
]);

app.factory('authInterceptor', function ($rootScope, $q, $window, BrowserStorageUtility) {
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

app.config(function($stateProvider, $urlRouterProvider, $httpProvider, localStorageServiceProvider) {
    $httpProvider.interceptors.push('authInterceptor');

    $urlRouterProvider.when("", "/login");
    $urlRouterProvider.when("/tenants", "/tenants/");

    // For any unmatched url, redirect to
    $urlRouterProvider.otherwise("/");

    // define states of the app
    $stateProvider
        .state('LOGIN', {
            url: "/login",
            templateUrl: 'app/login/view/LoginView.html'
        })
        .state('TENANT', {
            url: "/tenants",
            templateUrl: "app/core/view/MainBaseView.html"
        })
        .state('TENANT.LIST', {
            url: "/",
            templateUrl: "app/tenants/view/TenantListView.html"
        })
        .state('TENANT.CONFIG', {
            url: "/{tenantId}?readonly&listenState&product&contractId",
            templateUrl: "app/tenants/view/TenantConfigView.html"
        })
        .state('NOWHERE', {
            url: "/",
            templateUrl: 'app/core/view/Http404View.html'
        });

    localStorageServiceProvider
        .setPrefix('lattice.engines')
        .setStorageType('sessionStorage')
        .setNotify(true, true);
});