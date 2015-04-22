var app = angular.module("app.services.service.ServiceService", [
    'le.common.util.UnderscoreUtility',
    'app.core.util.SessionUtility'
]);

app.service('ServiceService', function($q, $http, $interval, _, SessionUtility){

    this.registeredServices = null;

    function cacheServiceList(services) {
        this.registeredServices = services;
        $interval(function(){
            this.registeredServices = null;
        }, 60000);
    }

    this.GetRegisteredServices = function() {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        if (this.registeredServices == null) {
            $http({
                method: 'GET',
                url: '/admin/services'
            }).success(function (data) {
                cacheServiceList(data);
                result.resultObj = data;
                defer.resolve(result);
            }).error(function (err, status) {
                SessionUtility.handleAJAXError(err, status);
            });
        } else {
            defer.resolve(this.registeredServices);
        }

        return defer.promise;
    };

});
