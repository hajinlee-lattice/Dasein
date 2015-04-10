var app = angular.module("app.tenants.service.TenantService", [
    'le.common.util.UnderscoreUtility'
]);

app.service('TenantService', function($q){

    function asycMockAllTenants() {
        var mockData = [2, 1, 4 ,3];

        return $q(function(resolve, reject) {
            setTimeout(function() {
                resolve(mockData);
            }, 1000);
        });
    }

    function asycMockTenantInfo(tenantId) {
        var mockData = [2, 1, 4 ,3];

        return $q(function(resolve, reject) {
            setTimeout(function() {
                resolve(mockData);
            }, 1000);
        });
    }

    this.GetAllTenants = function() {
        var defer = $q.defer();

        asycMockAllTenants().then(function(data){
            defer.resolve(data);
        });

        return defer.promise;
    };

    this.GetTenantById = function(tenantId) {
        var defer = $q.defer();

        asycMockTenantInfo(tenantId).then(function(data){
            defer.resolve(data);
        });

        return defer.promise;
    };

});