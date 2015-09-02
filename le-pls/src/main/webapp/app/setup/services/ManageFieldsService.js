angular.module('mainApp.setup.services.ManageFieldsService', [
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.services.SessionService'
])

.service('ManageFieldsService', function ($http, $q, _, BrowserStorageUtility, RightsUtility, ResourceUtility, SessionService) {

    this.GetFields = function () {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/metadata/fields?' + new Date().getTime()
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
                result.ResultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ReportErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };
});