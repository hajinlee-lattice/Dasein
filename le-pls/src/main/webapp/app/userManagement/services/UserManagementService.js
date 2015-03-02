angular.module('mainApp.userManagement.services.UserManagementService', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.RightsUtility'
])
.service('UserManagementService', function ($http, $q, _, BrowserStorageUtility, ResourceUtility, RightsUtility, ServiceErrorUtility) {

    this.GetUserByEmail = function(email) {
        var deferred = $q.defer();
        var result = {
            Success: false,
            ResultObj: null,
            ResultErrors: null
        };

        $http({
            method: 'GET',
            url: '/pls/users?email=' + email
        }).success(function(data){
            if (data != null) {
                result.Success = true;
                result.ResultObj = data;
            }
            deferred.resolve(result);
        }).error(function(){
            result.ResultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.GrantDefaultRights = function(username){
        var deferred = $q.defer();
        var result = {
            Success: false,
            ResultObj: null,
            ResultErrors: null
        };
        var clientSession = BrowserStorageUtility.getClientSession();
        var data = {
            Username: username,
            TenantId: clientSession.Tenant.Identifier
        };

        $http({
            method: 'PUT',
            url: '/pls/users/grant',
            data: JSON.stringify(data)
        }).success(function(data){
            if (data != null) {
                result.Success = true;
                result.ResultObj = data;
            }
            deferred.resolve(result);
        }).error(function(){
            result.ResultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.GetUsers = function () {
        var deferred = $q.defer();

        $http({
            method: 'GET', 
            url: '/pls/users/all/'
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
                    url: '/pls/users/resetpassword/' + user.Username
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

    this.DeleteSingleUser = function(user) {
        var deferred = $q.defer();
        var result = {
            Success: false,
            ReportErrors: null
        };

        $http({
            method: 'DELETE',
            url: '/pls/users/' + user.Username,
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function(data) {
            if(data != null && (data.Success === "true" || data.Success === true)) {
                result.Success = true;
            } else {
                result.ReportErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function() {
            result.ReportErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
            deferred.resolve(result);
        });

        return deferred.promise;
    };


    this.DeleteUsers = function(users) {
        var deferred = $q.defer();
        var result = {
            Success: false,
            SuccessUsers: [],
            FailUsers: []
        };
        var tenantId = BrowserStorageUtility.getClientSession().Tenant.Identifier;

        $http({
            method: 'POST',
            url: '/pls/users/bulkdelete',
            data: users,
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function(data) {
            if(data != null) {
                result.Success = true;
                result.SuccessUsers = data.SuccessUsers;
                result.FailUsers = data.FailUsers;
            }
            deferred.resolve(result);
        })
        .error(function() {
            deferred.resolve(result);
        });
        return deferred.promise;
    };

});