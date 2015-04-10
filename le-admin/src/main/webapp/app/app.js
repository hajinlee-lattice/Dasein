var app = angular.module("TenantConsoleApp", [
    "app.core.directive.MainNavDirective",
    "app.tenants.controller.TenantListCtrl",
    "app.tenants.controller.TenantInfoCtrl",
    "ui.router"
]);

app.config(function($stateProvider, $urlRouterProvider) {
    // For any unmatched url, redirect to
    $urlRouterProvider.otherwise("/");

    // define states of the app
    $stateProvider
        .state('TENANTS', {
            url: "/tenants",
            templateUrl: "app/tenants/view/TenantListView.html"
        })
        .state('TENANT_INFO', {
            url: "/tenants/{tenantId}",
            templateUrl: "app/tenants/view/TenantInfoView.html"
        })
        .state('NOWHERE', {
            url: "/",
            template: '<div class="text-center">' +
            '<img class="error-image" src="/assets/img/404.jpg" />' +
            '<h3>404 Page Not Found</h3>' +
            '</div>'
        });
});