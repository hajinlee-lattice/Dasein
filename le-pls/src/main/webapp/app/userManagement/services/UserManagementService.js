angular.module('mainApp.userManagement.services.UserManagementService', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('UserManagementService', function ($http, $q, BrowserStorageUtility, ResourceUtility, ServiceErrorUtility) {
    
    this.GetUsers = function (tenantId) {
        var deferred = $q.defer();

        $http({
            method: 'GET', 
            url: './LoginService.svc/GetUsers'
        })
        .success(function(data, status, headers, config) {
            var result = {
                success: false,
                resultObj: null,
                resultErrors: null
            };
            if (data != null && data !== "") {
                result.success = data.Success;
                if (data.Success === true) {
                    result.resultObj = data.Result;
                } else {
                    result.resultErrors = ServiceErrorUtility.HandleFriendlyServiceResponseErrors(data);
                }
            } else {
                result.resultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            var result = {
                success: false,
                reportErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };
            deferred.resolve(result);
        });
        
        return deferred.promise;
    };
    
    this.AddUser = function (emailAddress, firstName, lastName) {
        var deferred = $q.defer();
        
        var params = {
            userName: emailAddress,
            firstName: firstName,
            lastName: lastName
        };
        $http({
            method: 'POST', 
            url: './LoginService.svc/AddUser',
            data: JSON.stringify(params)
        })
        .success(function(data, status, headers, config) {
            var result = {
                success: false,
                resultObj: null,
                resultErrors: null
            };
            if (data != null && data !== "") {
                result.success = data.Success;
                if (data.Success === true) {
                    result.resultObj = data.Result;
                } else {
                    result.resultErrors = ServiceErrorUtility.HandleFriendlyServiceResponseErrors(data);
                }
            } else {
                result.resultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            var result = {
                success: false,
                reportErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };
            deferred.resolve(result);
        });
        
        return deferred.promise;
    };
});