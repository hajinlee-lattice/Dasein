angular.module('mainApp.login.services.LoginService', [
    'mainApp.core.utilities.ServiceErrorUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.services.SessionService'
])
.service('LoginService', function ($http, $q, BrowserStorageUtility, ServiceErrorUtility, ResourceUtility, StringUtility, SessionService) {
    
    this.Login = function (username, password) {
        var deferred = $q.defer();
        
        var passwordHash = CryptoJS.SHA256(password);
        var httpHeaders = {
        };
        
        $http({
            method: 'POST', 
            url: '/pls/login',
            data: JSON.stringify({ Username: username, Password: passwordHash.toString() })
         })
        .success(function(data, status, headers, config) {
            var result = null;    
            if (data != null && data !== "") {
                result = data;
                BrowserStorageUtility.setTokenDocument(data.Uniqueness + "." + data.Randomness);
                data.Result.UserName = username;
                BrowserStorageUtility.setLoginDocument(data.Result);
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            var result = {
                Success: false,
                errorMessage: data ? data.errorMsg : ResourceUtility.getString('LOGIN_UNKNOWN_ERROR')
            };
            deferred.resolve(result);
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
                SessionService.HandleResponseErrors(data, status);
            }
            
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            deferred.resolve(data);
        });
        
        return deferred.promise;
    };

    // If a user forgets their password, this will reset it and notify them
    this.ResetPassword = function (username) {
        if (username == null) {
            return null;
        }
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: "/pls/forgotpassword/" + username
        })
            .success(function(data, status, headers, config) {
                var result = { Success: false };

                if (data === true || data === 'true') {
                    result.Success = true;
                } else {
                    SessionService.HandleResponseErrors(data, status);
                }
                deferred.resolve(result);
            })
            .error(function(data, status, headers, config) {
                SessionService.HandleResponseErrors(data, status);
                var result = { Success: false, Error: data };
                deferred.resolve(result);
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
                SessionService.HandleResponseErrors(data, status);
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
        var username = loginDoc.UserName;
        var creds = {
            OldPassword : CryptoJS.SHA256(oldPassword).toString(),
            NewPassword : CryptoJS.SHA256(newPassword).toString()
        };

        $http({
            method: 'PUT',
            url: "/pls/users/" + username,
            data: angular.toJson(creds),
            headers: {
                "Content-Type": "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success:    true,
                Status:     status
            };

            if (data !== true && data !== 'true') {
                result.Success = false;
                SessionService.HandleResponseErrors(data, status);
            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                    Success:    false,
                    Status:     status
                };
            deferred.resolve(result);
        });

        return deferred.promise;
    };
});