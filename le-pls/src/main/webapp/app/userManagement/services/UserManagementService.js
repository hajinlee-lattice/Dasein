angular.module('mainApp.userManagement.services.UserManagementService', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.RightsUtility'
])
.service('UserManagementService', function ($http, $q, _, BrowserStorageUtility, ResourceUtility, RightsUtility, ServiceErrorUtility) {
    
    this.GetUsers = function (tenantId) {
        var deferred = $q.defer();

        $http({
            method: 'GET', 
            url: '/pls/users/all/' + tenantId
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data != null && data !== "") {
                result.Success = true;
                result.ResultObj = data;
            } else {
                result.ResultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            var result = {
                Success: false,
                ReportErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };
            deferred.resolve(result);
        });
        
        return deferred.promise;
    };
    
    this.AddUser = function (newUser) {
        var deferred = $q.defer();

        var clientSession = BrowserStorageUtility.getClientSession();
        var tenant = {
            DisplayName: clientSession.Tenant.DisplayName,
            Identifier:  clientSession.Tenant.Identifier
        };

        var user = {
            Username:  newUser.Email,
            FirstName: newUser.FirstName,
            LastName:  newUser.LastName,
            Email:     newUser.Email
        };

        var creds = {
            Username: user.Username,
            Password: "WillBeResetImmediately"
        };

        var registration = {
            User:        user,
            Credentials: creds,
            Tenant:      tenant
        };

        $http({
            method: 'POST', 
            url: '/pls/users/add',
            data: JSON.stringify(registration)
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: {},
                ResultErrors: null
            };
            if (data != null) {
                $http({
                    method: 'PUT',
                    url: '/pls/users/resetpassword/' + JSON.stringify(user.Username)
                }).success(function(pwd){
                    result.Success = true;
                    result.ResultObj = { Username: user.Username, Password: pwd };
                    deferred.resolve(result);
                }).error(function(error){
                    console.log(error);
                    result.ResultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
                    deferred.resolve(result);
                });
            } else {
                result.ResultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
                deferred.resolve(result);
            }
        })
        .error(function(data, status, headers, config) {
            var result = {
                Success: false,
                ReportErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.DeleteUser = function(user) {
        var deferred = $q.defer();
        var result = {
            Success: false,
            User: user
        };
        $http({
            method: 'DELETE',
            url: '/pls/users/' + JSON.stringify(user.Username)
        })
        .success(function(data, status, headers, config) {
            if(data != null && (data === "true" || data === true)) {
                result.Success = true;
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            deferred.resolve(result);
        });

        return deferred.promise;
    };

});