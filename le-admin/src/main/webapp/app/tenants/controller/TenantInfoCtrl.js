var app = angular.module("app.tenants.controller.TenantInfoCtrl", [
    "kendo.directives",
    'ui.router'
]);

app.controller('TenantInfoCtrl', function($scope, $stateParams) {
    $scope.tenantId = $stateParams.tenantId;

    $scope.panelBarOptions = {};
});
