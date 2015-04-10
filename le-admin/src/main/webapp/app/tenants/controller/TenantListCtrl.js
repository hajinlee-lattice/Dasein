var app = angular.module("app.tenants.controller.TenantListCtrl", [
    'le.common.util.UnderscoreUtility',
    'app.tenants.service.TenantService'
]);

app.controller('TenantListCtrl', function($scope, _, TenantService) {
    TenantService.GetAllTenants().then(function(result){
        $scope.data = _.sortBy(result);
    });

});
