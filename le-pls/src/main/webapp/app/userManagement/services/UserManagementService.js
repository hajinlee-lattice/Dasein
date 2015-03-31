angular.module('mainApp.userManagement.services.UserManagementService', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.services.SessionService'
])
.service('UserManagementService', function ($http, $q, _, BrowserStorageUtility, ResourceUtility, RightsUtility, SessionService) {

    this.GrantDefaultRights = function(username){
        var deferred = $q.defer();
        var result = {
            Success: false,
            ResultObj: null,
            ResultErrors: null
        };
        $http({
            method: 'PUT',
            url: '/pls/users/' + username,
            data: {
                AccessLevel: RightsUtility.AccessLevel.EXTERNAL_USER
            },
            headers: {"Content-Type": "application/json"}
        }).success(function(data){
            result.Success = data.Success;
            deferred.resolve(result);
        }).error(function(data, status){
            SessionService.HandleResponseErrors(data, status);
            result.ResultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
            deferred.resolve(result);
        });
        return deferred.promise;
    };

    this.GetUsers = function () {
        var deferred = $q.defer();

        $http({
            method: 'GET', 
            url: '/pls/users?' + new Date().getTime()
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = _.sortBy(data.Result, 'Username');
            } else {
                result.ResultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
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
            Credentials: creds
        };

        $http({
            method: 'POST', 
            url: '/pls/users',
            headers: {
                'Content-Type': "application/json"
            },
            data: registration
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: {},
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = { Username: user.Username, Password: data.Result.Password };
                deferred.resolve(result);
            } else {
                result.ResultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
                if (_.some(data.Errors, function(err){ return err.indexOf("email conflicts") > -1; })) {
                    result.ResultObj.ConflictingUser = data.Result.ConflictingUser;
                    result.ResultErrors = ResourceUtility.getString('ADD_USER_CONFLICT_EMAIL');
                }
                deferred.resolve(result);
            }
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ReportErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };
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

        var usernames = _.map(users, function(user){ return user.Username; });

        $http({
            method: 'DELETE',
            url: '/pls/users?usernames=' + JSON.stringify(usernames)
        })
        .success(function(data) {
            if(data.Success) {
                result.Success = true;
                _.each(data.Result.SuccessUsers, function(name){
                    var user = _.find(users, {Username: name});
                    if (user) {
                        result.SuccessUsers.push(user);
                    }
                });
                _.each(data.Result.FailUsers, function(name){
                    var user = _.find(users, {Username: name});
                    if (user) {
                        result.FailUsers.push(user);
                    }
                });
            }
            deferred.resolve(result);
        })
        .error(function(data, status) {
            SessionService.HandleResponseErrors(data, status);
            deferred.resolve(result);
        });
        return deferred.promise;
    };

});