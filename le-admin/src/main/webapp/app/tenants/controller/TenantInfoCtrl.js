var app = angular.module("app.tenants.controller.TenantInfoCtrl", [
    'app.tenants.service.TenantService',
    "kendo.directives",
    'ui.router'
]);

app.controller('TenantInfoCtrl', function($scope, $stateParams, TenantService) {
    $scope.tenantId = $stateParams.tenantId;

    TenantService.GetTenantById($scope.tenantId).then(function(result){
        $scope.data = result;
    });

    $scope.panelBarOptions = {};
});
