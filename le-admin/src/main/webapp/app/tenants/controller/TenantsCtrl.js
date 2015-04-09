var app = angular.module("app.tenants.controller.TenantsCtrl", ['le.common.util.UnderscoreUtility']);

app.controller('TenantsCtrl', function($scope, _) {
    var l = [2, 1, 4, 3];
    $scope.data = _.sortBy(l);
});
