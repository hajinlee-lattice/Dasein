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

    this.GetFieldsForModelSummaryId = function (modelSummaryId) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/modelsummaries/metadata/' + modelSummaryId,
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
            if (data !== null) {
                result.Success = true;
                result.ResultObj = data;
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

    this.UpdateAndCloneFields = function (modelName, originalModelSummaryId, fields) {
        var deferred = $q.defer();

        var cloneParams = {
            name : modelName,
            description: 'cloned from model: ' + originalModelSummaryId,
            attributes: fields,
            sourceModelSummaryId: originalModelSummaryId
        };

        $http({
            method: 'POST',
            url: '/pls/models/' + originalModelSummaryId + '/clone',
            headers: {
                'Content-Type': "application/json"
            },
            data: cloneParams
        })
        .success(function (data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data !== null) {
                result.Success = true;
                result.ResultObj = data;
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
