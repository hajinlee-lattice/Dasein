angular.module('mainApp.config.services.ConfigService', [
    'common.utilities.browserstorage',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.URLUtility',
    'mainApp.core.services.SessionService'
])
.service('ConfigService', function ($http, $q, BrowserStorageUtility, ResourceUtility, URLUtility, SessionService) {
    this.GetCurrentTopology = function () {
        var deferred = $q.defer();
        var tenant = BrowserStorageUtility.getClientSession().Tenant.Identifier;
        
        var credentialUrl = "/pls/config/topology?tenantId=" + tenant;
        var result;
        $http({
            method: "GET", 
            url: credentialUrl
        })
        .success(function(data, status, headers, config) {
            if (status === 200) {
                result = {
                    success: true,
                    resultObj: data.Topology,
                    resultErrors: null
                };
            } else {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: data.errorMsg
                };
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var errorMessage;
            if (data != null && data !== "") {
                errorMessage = data.errorMsg;
            } else {
                errorMessage = ResourceUtility.getString("SYSTEM_ERROR");
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
    
    this.GetCurrentCredentials = function (topologyType, isProduction) {
        isProduction = typeof isProduction !== 'undefined' ? isProduction : true;
        var deferred = $q.defer();
        var tenant = BrowserStorageUtility.getClientSession().Tenant.Identifier;
        
        var credentialUrl = "/pls/credentials/" + topologyType + "?tenantId=" + tenant;
        if (topologyType === "sfdc") {
            credentialUrl += "&isProduction=" + isProduction;
        }
        var result;
        $http({
            method: "GET", 
            url: credentialUrl
        })
        .success(function(data, status, headers, config) {
            if (status === 200) {
                result = {
                    success: true,
                    resultObj: data,
                    resultErrors: null
                };
            } else {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: data.errorMsg
                };
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString("SYSTEM_ERROR")
                };
            deferred.resolve(result);
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
            data: JSON.stringify(apiObj),
            timeout: 60000
        })
        .success(function(data, status, headers, config) {
            if (status === 200) {
                result = {
                    success: true,
                    resultObj: data,
                    resultErrors: null
                };
            } else {
                SessionService.HandleResponseErrors(data, status);
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString("VALIDATE_CREDENTIALS_FAILURE")
                };
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var errorMessage;
            if (status === 0) {
                errorMessage = ResourceUtility.getString("VALIDATE_CREDENTIALS_TIMEOUT");
            } else if ( data.errorCode === "LEDP_18030" ) {
                errorMessage = ResourceUtility.getString("VALIDATE_CREDENTIALS_FAILURE");
            } else {
                errorMessage = ResourceUtility.getString("SYSTEM_ERROR");
                console.error(data.errorMsg);
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