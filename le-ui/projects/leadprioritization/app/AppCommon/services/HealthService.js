angular.module('mainApp.appCommon.services.HealthService', [
    'mainApp.core.modules.ServiceErrorModule'
])
.service('HealthService', function($q, $http, $timeout, ServiceErrorUtility, ResourceUtility) {
    var CHECK_SYSTEM_STATUS_TIMEOUT = 2000;

    this.checkSystemStatus = function() {
        var deferred = $q.defer();
        deferred.resolve();
        return deferred.promise;
    };
});
