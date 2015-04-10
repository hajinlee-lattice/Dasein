var app = angular.module("TenantConsoleApp", [
    "app.core.directive.MainNavDirective",
    "app.tenants.controller.TenantListCtrl",
    "app.tenants.controller.TenantInfoCtrl",
    "ui.router"
]);

app.config(function($stateProvider, $urlRouterProvider) {
    $urlRouterProvider.when("", "/tenants/");
    $urlRouterProvider.when("/tenants", "/tenants/");

    // For any unmatched url, redirect to
    $urlRouterProvider.otherwise("/");

    // define states of the app
    $stateProvider
        .state('TENANTS', {
            url: "/tenants",
            templateUrl: "app/core/view/MainBaseView.html"
        })
        .state('TENANTS.LIST', {
            url: "/",
            templateUrl: "app/tenants/view/TenantListView.html"
        })
        .state('TENANTS.INFO', {
            url: "/{tenantId}",
            templateUrl: "app/tenants/view/TenantInfoView.html"
        })
        .state('NOWHERE', {
            url: "/",
            template: '<main-nav></main-nav>' +
            '<section class="container"><div class="text-center">' +
            '<img class="error-image" src="/assets/img/404.jpg" />' +
            '<h3>404 Page Not Found</h3>' +
            '</div></section>'
        });
});