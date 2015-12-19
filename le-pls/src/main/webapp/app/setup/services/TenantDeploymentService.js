angular.module('mainApp.setup.services.TenantDeploymentService', [
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.services.SessionService'
])

.service('TenantDeploymentService', function ($http, $q, BrowserStorageUtility, RightsUtility, ResourceUtility, SessionService) {

    this.GetTenantDeployment = function () {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/tenantdeployments/deployment?' + new Date().getTime(),
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                result.ResultErrors = ResourceUtility.getString('SETUP_GET_TENANT_DEPLOYMENT_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_GET_TENANT_DEPLOYMENT_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.DeleteTenantDeployment = function () {
        var deferred = $q.defer();

        $http({
            method: 'DELETE',
            url: '/pls/tenantdeployments/deployment?' + new Date().getTime(),
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                result.ResultErrors = ResourceUtility.getString('SETUP_CLEAR_TENANT_DEPLOYMENT_CONFIRM_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_CLEAR_TENANT_DEPLOYMENT_CONFIRM_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.ImportSfdcData = function () {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/tenantdeployments/importsfdcdata',
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                result.ResultErrors = ResourceUtility.getString('SETUP_IMPORT_DATA_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_IMPORT_DATA_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.GetImportSfdcDataCompleteTime = function () {
        return getStepSuccessTime('IMPORT_SFDC_DATA');
    };

    this.GetEnrichDataCompleteTime = function () {
        return getStepSuccessTime('ENRICH_DATA');
    };

    this.EnrichData = function () {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/tenantdeployments/enrichdata',
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                result.ResultErrors = ResourceUtility.getString('SETUP_ENRICH_DATA_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_ENRICH_DATA_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.ValidateMetadata = function () {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/tenantdeployments/validatemetadata',
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                result.ResultErrors = ResourceUtility.getString('SETUP_VALIDATE_METADATA_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_VALIDATE_METADATA_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.CancelLaunch = function () {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/tenantdeployments/cancellaunch',
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                result.ResultErrors = ResourceUtility.getString('SETUP_CANCEL_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_CANCEL_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    function getStepSuccessTime(step) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/tenantdeployments/successtime?step=' + step,
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                result.ResultErrors = ResourceUtility.getString('SETUP_GET_COMPLETED_TIME_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_GET_COMPLETED_TIME_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    }

    this.GetObjects = function (step, status) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/tenantdeployments/objects?step=' + step + '&status=' + status,
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                result.ResultErrors = ResourceUtility.getString('SETUP_GET_OBJECTS_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_GET_OBJECTS_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.RunQuery = function (step) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/tenantdeployments/runquery?step=' + step,
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                result.ResultErrors = ResourceUtility.getString('SETUP_RUN_QUERY_START_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_RUN_QUERY_START_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.GetQueryStatus = function (queryHandle) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/tenantdeployments/querystatus/' + queryHandle,
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data.Success) {
                result.Success = true;
                result.ResultObj = data.Result;
            } else {
                result.ResultErrors = ResourceUtility.getString('SETUP_GET_QUERY_STATUS_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_GET_QUERY_STATUS_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

});