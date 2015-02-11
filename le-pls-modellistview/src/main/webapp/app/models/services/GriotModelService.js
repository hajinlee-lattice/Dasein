angular.module('mainApp.models.services.GriotModelService', [
    'mainApp.core.utilities.ServiceErrorUtility',
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('GriotModelService', function ($http, $q, ServiceErrorUtility, ResourceUtility) {
    
    this.GetAllModels = function () {
        var deferred = $q.defer();
        var result = null;
       
        $http({
            method: "GET", 
            url: "./GriotService.svc/GetModels"
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
                
                deferred.resolve(result);
            } else {
                result = {
                    success: data.Success,
                    resultObj: null,
                    resultErrors: null
                };
                if (data.Success === true) {
                    result.resultObj = data.Result;
                } else {
                    result.resultErrors = ServiceErrorUtility.HandleFriendlyServiceResponseErrors(data);
                }
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            var result = {
                success: false,
                resultObj: null,
                resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };
            deferred.resolve(result);
        });
        
        return deferred.promise;
    };
});