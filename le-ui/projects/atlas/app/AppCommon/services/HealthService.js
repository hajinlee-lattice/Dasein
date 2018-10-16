angular.module('mainApp.appCommon.services.HealthService', [
    'common.exceptions'
])
.service('HealthService', function($q, $http, $timeout, Banner, ResourceUtility) {
    var CHECK_SYSTEM_STATUS_TIMEOUT = 2000;

    this.checkSystemStatus = function() {
        var deferred = $q.defer();
        var cancelled = false;

        var http = $http({
            method: 'GET',
            url: '/pls/health/systemstatus',
            timeout: $timeout(function() {
                deferred.resolve();
                cancelled = true;
            }, CHECK_SYSTEM_STATUS_TIMEOUT)
        }).then(function(response) {
            if (response.data.status === 'OK') {
                deferred.resolve();
            } else {
                Banner.error({
                    message: response.data.message || ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                });

                deferred.reject();
            }
        }).catch(function() {
            if (cancelled) {
                return;
            }

            Banner.error({
                message: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            });
            
            deferred.reject();
        });

        return deferred.promise;
    };
});
