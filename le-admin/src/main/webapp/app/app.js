var app = angular.module("TenantConsoleApp", [
    "app.core.directive.MainNavDirective",
    "app.tenants.controller.TenantsCtrl",
    "app.tenants.controller.TenantInfoCtrl",
    "ui.router"
]);

app.config(function($stateProvider, $urlRouterProvider) {
    // For any unmatched url, redirect to
    $urlRouterProvider.otherwise("/tenants");

    // define states of the app
    $stateProvider
        .state('TENANTS', {
            url: "/tenants",
            templateUrl: "app/tenants/view/TenantsView.html"
        })
        .state('TENANT_INFO', {
            url: "/tenants/{tenantId}",
            templateUrl: "app/tenants/view/TenantInfoView.html"
        });
});