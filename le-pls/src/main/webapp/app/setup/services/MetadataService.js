angular.module('mainApp.setup.services.MetadataService', [
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.services.SessionService'
])

.service('MetadataService', function ($http, $q, _, BrowserStorageUtility, RightsUtility, ResourceUtility, SessionService) {

    this.GetOptions = function () {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/vdbmetadata/options?' + new Date().getTime(),
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function (data) {
            var result = {
                Success: true,
                ResultObj: data,
                ResultErrors: null
            };
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GET_OPTIONS_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.GetFields = function () {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/vdbmetadata/fields?' + new Date().getTime(),
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function (data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                result.ResultErrors = ResourceUtility.getString('SETUP_MANAGE_FIELDS_GET_FIELDS_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GET_FIELDS_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.UpdateField = function (field) {
        var deferred = $q.defer();

        $http({
            method: 'PUT',
            url: '/pls/vdbmetadata/fields/' + field.ColumnName,
            headers: {
                'Content-Type': "application/json"
            },
            data: field
        })
        .success(function (data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                result.ResultErrors = ResourceUtility.getString('SETUP_MANAGE_FIELDS_UPDATE_FIELD_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_MANAGE_FIELDS_UPDATE_FIELD_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.UpdateFields = function (fields) {
        var deferred = $q.defer();

        $http({
            method: 'PUT',
            url: '/pls/vdbmetadata/fields',
            headers: {
                'Content-Type': "application/json"
            },
            data: fields
        })
        .success(function (data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                result.ResultErrors = ResourceUtility.getString('SETUP_MANAGE_FIELDS_UPDATE_FIELDS_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_MANAGE_FIELDS_UPDATE_FIELD_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };
});