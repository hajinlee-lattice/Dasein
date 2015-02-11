angular.module('mainApp.login.services.LoginService', [
    'mainApp.core.utilities.ServiceErrorUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility'
])
.service('LoginService', function ($http, $q, BrowserStorageUtility, ServiceErrorUtility, ResourceUtility, StringUtility) {
    
    this.Login = function (username, password) {
        var deferred = $q.defer();
        
        var passwordHash = CryptoJS.SHA256(password);
        var httpHeaders = {
        };
        
        $http({
            method: 'POST', 
            url: '/pls/login',
            data: JSON.stringify({ Username: username, Password: passwordHash.toString() }),
 //           headers: httpHeaders
        })
        .success(function(data, status, headers, config) {
            var result = null;    
            if (data != null && data !== "") {
                result = data;
                if (data.Success === true) {                    
                    BrowserStorageUtility.setTokenDocument(data.Uniqueness + "." + data.Randomness);
                    data.Result.UserName = username;
                    BrowserStorageUtility.setLoginDocument(data.Result);
                } else {
                    if (ServiceErrorUtility.ServiceResponseContainsError(data, ServiceErrorUtility.InvalidCredentials)) {
                        result.FailureReason = ServiceErrorUtility.InvalidCredentials;
                    } else if (ServiceErrorUtility.ServiceResponseContainsError(data, ServiceErrorUtility.ExpiredPassword)) {
                        result.FailureReason = ServiceErrorUtility.ExpiredPassword;
                    } else if (ServiceErrorUtility.ServiceResponseContainsError(data, ServiceErrorUtility.LoginServiceNotRunning)) {
                        result.FailureReason = ServiceErrorUtility.LoginServiceNotRunning;
                    } else {
                        result.FailureReason = "LOGIN_UNKNOWN_ERROR";
                    }
                }
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            deferred.resolve(false);
        });
        
        return deferred.promise;
    };
    
    this.GetSessionDocument = function (tenant) {
        if (tenant == null) {
            return null;
        }
        var deferred = $q.defer();
        
        $http({
            method: 'POST', 
            url: '/pls/attach',
            data: angular.toJson(tenant)
        })
        .success(function(data, status, headers, config) {
            var result = false;
            if (data != null && data.Success === true) {
                BrowserStorageUtility.setSessionDocument(data.Result);
                data.Result.User.Tenant = tenant;
                BrowserStorageUtility.setClientSession(data.Result.User);
                result = data;
            } else {
                ServiceErrorUtility.HandleFriendlyServiceResponseErrors(data);
            }
            
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            deferred.resolve(data);
        });
        
        return deferred.promise;
    };
    
    //If a user forgets their password, this will reset it and notify them
    this.ResetPassword = function (username) {
        if (username == null) {
            return null;
        }
        var deferred = $q.defer();
        
        $http({
            method: 'GET', 
            url: "./LoginService.svc/ForgotPassword?username=" + username
        })
        .success(function(data, status, headers, config) {
            deferred.resolve(data);
        })
        .error(function(data, status, headers, config) {
            deferred.resolve(data);
        });
        
        return deferred.promise;
    };
    
    this.Logout = function () {
        var deferred = $q.defer();
        
        $http({
            method: 'GET', 
            data: '',
            url: "/pls/users/logout",
            headers: {
               "Content-Type": "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            if (data != null && data.Success === true) {
                BrowserStorageUtility.clear(false);
                ResourceUtility.clearResourceStrings();
                window.location.reload();
            } else {
                ServiceErrorUtility.HandleFriendlyServiceResponseErrors(data);
            }
            deferred.resolve(data);
        })
        .error(function(data, status, headers, config) {
            deferred.resolve(data);
        });
        
        return deferred.promise;
    };
    
    this.ChangePassword = function (oldPassword, newPassword, confirmNewPassword) {
        var deferred = $q.defer();
        
        if (StringUtility.IsEmptyString(oldPassword) || StringUtility.IsEmptyString(newPassword) || StringUtility.IsEmptyString(confirmNewPassword)) {
            deferred.resolve(null);
            return deferred.promise;
        }
        
        var loginDoc = BrowserStorageUtility.getLoginDocument();
        var result = {
            userName: loginDoc.UserName,
            logSubID: CryptoJS.SHA256(oldPassword).toString(),
            logMasterID: CryptoJS.SHA256(newPassword).toString(),
            logDeploymentID: CryptoJS.SHA256(confirmNewPassword).toString()
            
        };
        
        $http({
            method: 'POST',
            url: "./LoginService.svc/ChangePassword",
            data: JSON.stringify(result)
        })
        .success(function(data, status, headers, config) {
            if (data == null || data.Success !== true) {
                ServiceErrorUtility.HandleFriendlyServiceResponseErrors(data);
            }
            deferred.resolve(data);
        })
        .error(function(data, status, headers, config) {
            deferred.resolve(data);
        });
        
        return deferred.promise;
    };
});