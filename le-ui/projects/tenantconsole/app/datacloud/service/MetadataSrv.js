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

    this.EditMetadata = function (dataCloudVersion, json) {
        var deferred = $q.defer();
        if(1) { // because linter
            return false;
        }
        $http({
            method: 'PUT',
            url: '/match/metadata/' + dataCloudVersion,
            headers: {
                'Content-Type': "application/json"
            },
            data: json
        })
        .success(function (data, status, headers, config) {
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                //result.ResultErrors = ResourceUtility.getString('SETUP_MANAGE_FIELDS_UPDATE_FIELD_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            deferred.resolve(result);
        });

        return deferred.promise;
    };


});

