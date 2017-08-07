var app = angular.module('mainApp.userManagement.services.UserManagementService', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.services.SessionService'
]);

app.service('UserManagementService', function ($http, $q, _, BrowserStorageUtility, ResourceUtility, RightsUtility, SessionService) {

    this.AssignAccessLevel = function (username, accessLevel) {
        var deferred = $q.defer();
        var result = {
            Success: false,
            ResultObj: null,
            ResultErrors: null
        };
        $http({
            method: 'PUT',
            url: '/pls/users/' + JSON.stringify(username),
            data: {
                AccessLevel: accessLevel
            },
            headers: {"Content-Type": "application/json"}
        }).success(function (data) {
            result.Success = data.Success;
            result.ResultObj = {Username: username, AccessLevel: accessLevel};
            deferred.resolve(result);
        }).error(function (data, status) {
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
            .success(function (data, status, headers, config) {
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
            .error(function (data, status, headers, config) {
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
            Username: newUser.Email,
            FirstName: newUser.FirstName,
            LastName: newUser.LastName,
            Email: newUser.Email,
            AccessLevel: newUser.AccessLevel
        };

        var creds = {
            Username: user.Username,
            Password: "WillBeResetImmediately"
        };

        var registration = {
            User: user,
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
        .success(function (data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: {},
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = {Username: user.Username, AccessLevel: user.AccessLevel, Password: data.Result.Password};
                deferred.resolve(result);
            } else {
                result.ResultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
                if (data.Result.hasOwnProperty("ConflictingUser")) {
                    result.ResultObj.ConflictingUser = data.Result.ConflictingUser;
                    result.ResultErrors = ResourceUtility.getString('ADD_USER_CONFLICT_EMAIL');
                }
                deferred.resolve(result);
            }
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ReportErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.DeleteUser = function (user) {
        var deferred = $q.defer();

        var result = {
            Success: false,
            ResultObj: {},
            ResultErrors: null
        };

        $http({
            method: 'DELETE',
            url: '/pls/users/' + JSON.stringify(user.Username),
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function (data, status, headers, config) {
            if (data.Success) {
                result.Success = true;
                deferred.resolve(result);
            } else {
                result.ResultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
                //TODO:song handle different error messages
                if (_.some(data.Errors, function (err) {
                        return err.indexOf("email conflicts") > -1;
                    })) {
                    result.ResultObj.ConflictingUser = data.Result.ConflictingUser;
                    result.ResultErrors = ResourceUtility.getString('ADD_USER_CONFLICT_EMAIL');
                }
                deferred.resolve(result);
            }
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ReportErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };
});