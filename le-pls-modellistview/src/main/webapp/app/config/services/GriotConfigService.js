angular.module('mainApp.config.services.GriotConfigService', [
    'mainApp.core.utilities.ServiceErrorUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('GriotConfigService', function ($http, $q, BrowserStorageUtility, ServiceErrorUtility, ResourceUtility) {
    
    this.GetConfigDocument = function () {
        var deferred = $q.defer();
        var result = null;
        
        // Check cache first
        var cachedConfigDoc = BrowserStorageUtility.getConfigDocument();
        if (cachedConfigDoc != null && cachedConfigDoc.Timestamp > new Date().getTime()) {
            result = {
                success: true,
                resultObj: cachedConfigDoc,
                resultErrors: null
            };
            deferred.resolve(result);
            return deferred.promise;
        }
        
        $http({
            method: "GET", 
            url: "./GriotService.svc/GetConfigDocument"
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
                    BrowserStorageUtility.setConfigDocument(data.Result);
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
    
    this.GetWidgetConfigDocument = function () {
        var deferred = $q.defer();
        var result = null;
        
        // Check cache first
        var cachedConfigDoc = BrowserStorageUtility.getWidgetConfigDocument();
        if (cachedConfigDoc != null && cachedConfigDoc.Timestamp > new Date().getTime()) {
            result = {
                success: true,
                resultObj: cachedConfigDoc,
                resultErrors: null
            };
            deferred.resolve(result);
            return deferred.promise;
        }
        
        $http({
            method: "GET", 
            url: "./GriotService.svc/GetWidgetConfigDocument"
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
                    var parsedResult = JSON.parse(data.Result);
                    result.resultObj = parsedResult;
                    BrowserStorageUtility.setWidgetConfigDocument(parsedResult);
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
    
    //If a user forgets their password, this will reset it and notify them
    this.ValidateApiCredentials = function (apiObj) {
        if (apiObj == null) {
            return null;
        }
        var deferred = $q.defer();
        
        $http({
            method: "POST", 
            url: "./GriotService.svc/ValidateApiConfiguration",
            data: JSON.stringify(apiObj)
        })
        .success(function(data, status, headers, config) {
            var result = null;
            if (data != null && data !== "") {
                result = data;
                if (data.Success !== true) {
                    if (ServiceErrorUtility.ServiceResponseContainsError(data, "VALIDATE_CREDENTIALS_FAILURE")) {
                        result.FailureReason = "VALIDATE_CREDENTIALS_FAILURE";
                    } else {
                        result.FailureReason = "SYSTEM_ERROR";
                    }
                }
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            deferred.resolve(data);
        });
        
        return deferred.promise;
    };
});