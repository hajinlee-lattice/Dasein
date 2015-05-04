angular.module('mainApp.config.services.ConfigService', [
    'mainApp.core.utilities.ServiceErrorUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.URLUtility',
    'mainApp.core.services.SessionService'
])
.service('ConfigService', function ($http, $q, BrowserStorageUtility, ServiceErrorUtility, ResourceUtility, URLUtility, SessionService) {
    
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
        
        var test = URLUtility.GetBaseUrl();
        var webServer = URLUtility.GetWebServerAddress("/") + "/assets/resources/WidgetConfigurationDocument.json";
        
        $http({
            method: 'GET', 
            url: webServer
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: null
                };
                deferred.resolve(result);
                return;
            }
            
            BrowserStorageUtility.setWidgetConfigDocument(data);
            result = {
                success: true,
                resultObj: data,
                resultErrors: null
            };
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            result = {
                success: false,
                resultObj: null,
                resultErrors: null
            };
            deferred.resolve(result);
        });
        
        return deferred.promise;
    };
    
    this.GetCurrentCredentials = function (topologyType, isProduction) {
        isProduction = typeof isProduction !== 'undefined' ? isProduction : true;
        var deferred = $q.defer();
        var test = BrowserStorageUtility.getClientSession().Tenant;
        var tenant = BrowserStorageUtility.getClientSession().Tenant.Identifier;
        
        var credentialUrl = "/pls/credentials/" + topologyType + "?tenantId=" + tenant;
        if (topologyType === "sfdc") {
            credentialUrl += "&isProduction=" + isProduction;
        }
        $http({
            method: "GET", 
            url: credentialUrl
        })
        .success(function(data, status, headers, config) {
            var result = null;
            if (data != null && data !== "") {
                result = data;
                /*if (data.Success === true) {
                    SessionService.HandleResponseErrors(data, status);
                    if (ServiceErrorUtility.ServiceResponseContainsError(data, "VALIDATE_CREDENTIALS_FAILURE")) {
                        result.FailureReason = "VALIDATE_CREDENTIALS_FAILURE";
                    } else {
                        result.FailureReason = "SYSTEM_ERROR";
                    }
                }*/
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            deferred.resolve(data);
        });
        
        return deferred.promise;
    };
    
    //If a user forgets their password, this will reset it and notify them
    this.ValidateApiCredentials = function (topologyType, apiObj, isProduction) {
        if (apiObj == null) {
            return null;
        }
        isProduction = typeof isProduction !== 'undefined' ? isProduction : true;
        var deferred = $q.defer();
        var tenant = BrowserStorageUtility.getClientSession().Tenant.Identifier;
        var credentialUrl = "/pls/credentials/" + topologyType + "/?tenantId=" + tenant;
        if (topologyType === "sfdc") {
            credentialUrl += "&isProduction=" + isProduction;
        }
        var result;
        $http({
            method: "POST", 
            url: credentialUrl,
            data: JSON.stringify(apiObj)
        })
        .success(function(data, status, headers, config) {
            var result = null;
            if (data != null && data !== "") {
                result = {
                    success: data.Success,
                    resultObj: null,
                    resultErrors: null
                };
                if (data.Success !== true) {
                    SessionService.HandleResponseErrors(data, status);
                    result.resultErrors = ResourceUtility.getString("VALIDATE_CREDENTIALS_FAILURE");
                }
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var errorMessage;
            if (data == null || data === "") {
                errorMessage = ResourceUtility.getString("SYSTEM_ERROR");
            } else {
                errorMessage = data.errorMsg;
            }
            result = {
                success: false,
                resultObj: null,
                resultErrors: errorMessage
            };
            deferred.resolve(result);
        });
        
        return deferred.promise;
    };
});