angular.module('mainApp.setup.services.LeadEnrichmentService', [
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.services.SessionService'
])

.service('LeadEnrichmentService', function ($http, $q, BrowserStorageUtility, RightsUtility, ResourceUtility, SessionService) {

    this.GetAvariableAttributes = function () {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/leadenrichment/avariableattributes?' + new Date().getTime(),
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                result.ResultErrors = ResourceUtility.getString('LEAD_ENRICHMENT_GET_AVAILABLE_ATTRIBUTES_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('LEAD_ENRICHMENT_GET_AVAILABLE_ATTRIBUTES_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.GetSavedAttributes = function () {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/leadenrichment/savedattributes?' + new Date().getTime(),
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                result.ResultErrors = ResourceUtility.getString('LEAD_ENRICHMENT_GET_SELECTED_ATTRIBUTES_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('LEAD_ENRICHMENT_GET_SELECTED_ATTRIBUTES_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };
});