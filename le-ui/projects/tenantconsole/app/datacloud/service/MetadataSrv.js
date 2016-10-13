var app = angular.module("app.datacloud.service.MetadataService", [
]);

app.service('MetadataService', function($q, $http, $timeout, SessionUtility){

    this.GetDataCloudVersions = function() {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/match/metadata/versions'
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

    this.GetAccountMasterColumns = function(dataCloudVersion) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/match/metadata/?datacloudversion=' + dataCloudVersion
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

