var app = angular.module("app.tenants.service.TenantService", [
    'le.common.util.UnderscoreUtility',
    'app.core.util.SessionUtility',
    "app.tenants.util.TenantUtility"
]);

app.service('TenantService', function($q, $http, _, TenantUtility, SessionUtility){

    this.CreateTenant = function(tenantId, contractId, tenantRegisration) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'POST',
            url: '/admin/tenants/' + tenantId + '?contractId=' + contractId,
            data: tenantRegisration
        }).success(function(data) {
            if (data !== "true" && data !== true) {
                result.success = false;
            }
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
        });

        return defer.promise;
    };

    this.DeleteTenant = function(tenantId, contractId) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'DELETE',
            url: '/admin/tenants/' + tenantId + '?contractId=' + contractId
        }).success(function(data) {
            if (data !== "true" && data !== true) {
                result.success = false;
            }
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
            result.success = false;
            defer.resolve(result);
        });

        return defer.promise;
    };

    this.GetAllTenants = function() {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/admin/tenants'
        }).success(function(data){
            result.resultObj = _.map(data, TenantUtility.convertTenantRecordToGridData);
            defer.resolve(result);

        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
        });

        return defer.promise;
    };

    this.GetTenantInfo = function(tenantId, contractId) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/admin/tenants/' + tenantId + '?contractId=' + contractId
        }).success(function(data){
            result.resultObj = TenantUtility.parseTenantInfo(data);
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
        });

        return defer.promise;
    };

    this.GetServiceDefaultConfig = function(service) {
        var defer = $q.defer();
        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/admin/services/' + service + '/default'
        }).success(function(data){
            if (data !== null && data.hasOwnProperty("RootPath")) {
                data.Component = service;
                result.resultObj = data;
            } else {
                result.success = false;
                result.errMsg = "Could not load default configuration.";
            }
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
        });

        return defer.promise;
    };

    this.GetTenantServiceConfig = function(tenantId, contractId, serviceName) {
        var defer = $q.defer();
        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/admin/tenants/' + tenantId + '/services/' + serviceName + '?contractId=' + contractId
        }).success(function(response){
            var data = {};
            data.Component = serviceName;
            data.RootPath = response.RootPath;
            data.Nodes = [];
            _.each(response.Nodes, function(node){
                switch (node.Node.toLowerCase()) {
                    case "state.json":
                        data.State = JSON.parse(node.Data);
                        break;
                    case "lock":
                        break;
                    default:
                        data.Nodes.push(node);
                }
            });

            result.resultObj = data;

            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
        });

        return defer.promise;
    };

    function GetTenantServiceStatus(tenantId, contractId, serviceName) {
        var defer = $q.defer();
        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/admin/tenants/' + tenantId + '/services/' + serviceName + '/state?contractId=' + contractId
        }).success(function(response){
            result.resultObj = response;
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
        });

        return defer.promise;
    }

    this.GetTenantServiceStatus = GetTenantServiceStatus;
});

