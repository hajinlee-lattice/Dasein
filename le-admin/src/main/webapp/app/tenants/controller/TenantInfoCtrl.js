var app = angular.module("app.tenants.controller.TenantInfoCtrl", ['ui.router']);

app.controller('TenantInfoCtrl', function($scope, $stateParams) {
    $scope.tenantId = $stateParams.tenantId;
});
