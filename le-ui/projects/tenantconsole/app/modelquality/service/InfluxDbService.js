angular.module("app.modelquality.service.InfluxDbService", [
])
.service('InfluxDbService', function($q, $http, SessionUtility){

    this.Query = function (query) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/influxdb/',
            params: query
        }).success(function(data){
            result.resultObj = data;
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
            result.success = false;
            result.errMsg = err;
            defer.reject(result);
        });

        return defer.promise;
    };

});

