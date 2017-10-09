angular.module('mainApp.login.services.LoginService', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.services.SessionService'
])
.service('LoginService', function ($http, $q, $window, $location, BrowserStorageUtility, ResourceUtility, StringUtility, SessionService) {

    this.Login = function (username, password) {
        var deferred = $q.defer();

        var passwordHash = CryptoJS.SHA256(password);
        var httpHeaders = {
        };
        var params = JSON.stringify({ Username: username, Password: passwordHash.toString() });

        $http({
            method: 'POST',
            url: '/pls/login',
            data: params
         }).then(
            function onSuccess(response) {
                var result = response.data;
                if (result != null && result !== "") {
                    BrowserStorageUtility.setTokenDocument(result.Uniqueness + "." + result.Randomness);
                    result.Result.UserName = username;
                    BrowserStorageUtility.setLoginDocument(result.Result);
                }
                deferred.resolve(result);
            }, function onError(response) {

                var result = {
                    Success: false,
                    errorMessage: ResourceUtility.getString('LOGIN_UNKNOWN_ERROR')
                };

                if (response.data && response.data.errorCode === 'LEDP_18001') {
                    result.errorMessage = ResourceUtility.getString('DEFAULT_LOGIN_ERROR_TEXT');
                }
                deferred.resolve(result.errorMessage);
            });

        return deferred.promise;
    };

    this.GetSessionDocument = function (tenant, username) {
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
                result = data;

                BrowserStorageUtility.setClientSession(data.Result.User, function(){
                    BrowserStorageUtility.setHistory(username, tenant);
                    
                    deferred.resolve(result);
                });
            }

            if (result.Result.User.AccessLevel === null) {
                status = 401;
                SessionService.HandleResponseErrors(data, status);
                deferred.resolve(result);
            }
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
            method: 'PUT',
            url: "/pls/forgotpassword/",
            data: {Username: username, Product: "Lead Prioritization", HostPort: this.getHostPort()},
            headers: {"Content-Type": "application/json"}
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
            url: "/pls/logout",
            headers: {
               "Content-Type": "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            if (data != null && data.Success === true) {
                BrowserStorageUtility.clear(false);
                ResourceUtility.clearResourceStrings();

                setTimeout(function() {
                    window.open("/login/", "_self");
                }, 300);
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

        var creds = {
            OldPassword : CryptoJS.SHA256(oldPassword).toString(),
            NewPassword : CryptoJS.SHA256(newPassword).toString()
        };

        var username = BrowserStorageUtility.getLoginDocument().UserName;
        $http({
            method: 'PUT',
            url: '/pls/password/' + username + '/',
            data: creds,
            headers: {
                "Content-Type": "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success:    true,
                Status:     status
            };

            if (!data.Success) {
                result.Success = false;
            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            var result = {
                    Success:    false,
                    Status:     status
                };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.getHostPort = function() {
        var host = $location.host();
        var protocal = $location.protocol();
        var port = $location.port();
        if (port == 80) {
            return protocal + "://" + host;
        } else {
            return protocal + "://" + host + ":" + port;
        }
    };

});
