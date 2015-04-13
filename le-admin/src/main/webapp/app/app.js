var app = angular.module("TenantConsoleApp", [
    "app.core.directive.MainNavDirective",
    "app.login.controller.LoginCtrl",
    "app.tenants.controller.TenantListCtrl",
    "app.tenants.controller.TenantConfigCtrl",
    "ui.router"
]);

app.config(function($stateProvider, $urlRouterProvider) {
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
            url: "/{tenantId}?mode",
            templateUrl: "app/tenants/view/TenantConfigView.html"
        })
        .state('NOWHERE', {
            url: "/",
            templateUrl: 'app/core/view/Http404View.html'
        });
});