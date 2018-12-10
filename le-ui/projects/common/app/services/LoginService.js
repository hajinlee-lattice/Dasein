angular.module('mainApp.login.services.LoginService', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.services.SessionService'
])
.service('LoginService', function ($http, $q, $location, $window, BrowserStorageUtility, ResourceUtility, StringUtility, SessionService) {

    this.Login = function (username, password) {
        var deferred = $q.defer();

        var passwordHash = CryptoJS.SHA256(password);
        var httpHeaders = {
        };
        var params = JSON.stringify({ Username: username, Password: passwordHash.toString() });

        $http({
            method: 'POST',
            url: '/pls/login',
            data: params,
            headers: {
                'ErrorDisplayMethod': 'none'
            }
         }).then(
            function onSuccess(response) {
                // console.log('BACK Again');
                var result = response.data;
                if (result != null && result !== "" && result.Success == true) {
                    BrowserStorageUtility.setTokenDocument(result.Uniqueness + "." + result.Randomness);
                    result.Result.UserName = username;
                    result.Result.FirstName = result.FirstName;
                    result.Result.LastName = result.LastName;
                    BrowserStorageUtility.setLoginDocument(result.Result);
                    deferred.resolve(result);
                } else {
                	var errors = result.Errors;
                	var result = {
                            Success: false,
                            errorMessage: errors[0]
                        };
                	deferred.resolve(result.errorMessage);
                }
                
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

    this.SamlLogin = function (sessionDocument) {
        var deferred = $q.defer();
        var result = sessionDocument;
        if (result != null && result !== "") {
            result.Ticket.AuthenticationRoute = result.AuthenticationRoute; //FIXME this is just here until backend starts passing it

            BrowserStorageUtility.setTokenDocument(result.Ticket.Uniqueness + "." + result.Ticket.Randomness);
            deferred.resolve(result);
            BrowserStorageUtility.setLoginDocument(result.Ticket); // with normal LoginService.login ^ this is result.Result, here it's result.Ticket and result.Result is what attach would use
        }
        deferred.resolve(result);

        return deferred.promise;
    };

    this.PostToJwt = function (params) {
        var deferred = $q.defer();

        console.log(params);

        $http({
            method: 'POST',
            url: '/pls/jwt/handle_request',
            data: {
                'requestParameters': params
            },
            headers: {
                'Content-Type': 'application/json'
            }
        }).then(
            function onSuccess(data, status, headers, config){
                deferred.resolve(data.data);
            },
            function onError(data, status, headers, config) {
                SessionService.HandleResponseErrors(data.data, status);
                deferred.resolve(data.data);
            }
        );
        return deferred.promise;
    };

    this.SSOLogin = function (tenantName, params) {
        var deferred = $q.defer();

        console.log(params);

        $http({
            method: 'POST',
            url: '/pls/saml/splogin',
            data: {
                'requestParameters': params,
                'tenantDeploymentId': tenantName
            },
            headers: {
                'Content-Type': 'application/json'
            }
        }).then(
            function onSuccess(data){        
                console.log(data.data);
                deferred.resolve(data.data);
            },
            function onError(data, status, headers, config) {
                SessionService.HandleResponseErrors(data.data, status);
                deferred.resolve(data.data);
            }
        );
        return deferred.promise;
    }


    this.GetSessionDocument = function (tenant, username) {
        if (tenant == null) {
            return null;
        }

        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/attach',
            data: angular.toJson(tenant)
        }).then(
            function onSuccess(d, status, headers, config){
                var data = d.data;
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
            },
            function onError(data, status, headers, config) {
                SessionService.HandleResponseErrors(data.data, status);
                deferred.resolve(data.data);
            }
        );
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
        }).then(
            function onSuccess(data, status, headers, config){
                var result = { Success: false };
                if (data.data === true || data.data === 'true') {
                    result.Success = true;
                } else {
                    SessionService.HandleResponseErrors(data.data, status);
                }
                deferred.resolve(result);
            },
            function onError(data, status, headers, config){
                var result = { Success: false, Error: data.data };
                deferred.resolve(result);
            }
        );
        return deferred.promise;
    };

    this.Logout = function (params) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            data: '',
            url: '/pls/logout',
            headers: {
               'Content-Type': 'application/json'
            }
        }).then(
            function onSuccess(data, status, headers, config){
                if (data != null && data.data.Success === true) {
                    BrowserStorageUtility.clear(false);
                    ResourceUtility.clearResourceStrings();
    
                    var loginDocument = BrowserStorageUtility.getLoginDocument(),
                        authenticationRoute = (loginDocument && loginDocument.AuthenticationRoute ? loginDocument.AuthenticationRoute : null),
                        tenantId = (BrowserStorageUtility.getClientSession() && BrowserStorageUtility.getClientSession().Tenant  && BrowserStorageUtility.getClientSession().Tenant.Identifier ? BrowserStorageUtility.getClientSession().Tenant.Identifier : null);
    
                    var paramString = params ? '?' + Object.keys(params).map(function(k) {
                        return encodeURIComponent(k) + '=' + encodeURIComponent(params[k]);
                    }).join('&') : '';

                    setTimeout(function() {
                        if(authenticationRoute === 'SSO') {
                            $window.open('/login/saml/' + tenantId + '/logout' + paramString, '_self');
                        } else {
                            paramString = params ? paramString + '&logout=true' : '?logout=true';
                            setTimeout(function() {
                                $window.open('/login' + paramString, '_self');    
                            }, 300);
                        }
                    }, 300);
                } else {
                    SessionService.HandleResponseErrors(data.data, status);
                }
                deferred.resolve(data);
            },
            function onError(data, status, headers, config){
                deferred.resolve(data.data);
            }
        );
       
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
        }).then(
            function onSuccess(data, status, headers, config){
                var result = {
                    Success:    true,
                    Status:     status
                };
    
                if (!data.data.Success) {
                    result.Success = false;
                }
    
                deferred.resolve(result);
            },
            function onError(data, status, headers, config){
                var result = {
                    Success:    false,
                    Status:     status
                };
                deferred.resolve(result);
            }
        );
       
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
