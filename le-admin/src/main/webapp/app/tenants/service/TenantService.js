var app = angular.module("app.tenants.service.TenantService", [
    'le.common.util.UnderscoreUtility',
    'app.core.util.SessionUtility'
]);

app.service('TenantUtility', function(_){

    function cleanupComponentConfigs(components, tenant, contract, space) {
        return _.map(components,
            function(component){
                var componentConfig = {
                    RootPath: "Tenants/tenant/Contracts/contract/Space/space/Services/" + component.Component
                };
                if (component.hasOwnProperty("Nodes")) {
                    componentConfig.Nodes = cleanupConfigData(component.Nodes);
                }
                return componentConfig;
            });
    }

    function cleanupConfigData(configs) {
        return _.map(configs,
            function(config){
                var cleanedConfig = {Node: config.Node};
                if (config.hasOwnProperty("Data")) {
                    cleanedConfig.Data = config.Data;
                }
                if (config.hasOwnProperty("Children")) {
                    cleanedConfig.Children = cleanupConfigData(config.Children);
                }
                return cleanedConfig;
            });
    }

    this.cleanupComponentConfigs = cleanupComponentConfigs;

    this.getStatusTemplate = function(status) {
        switch (status) {
            case this.getStatusDisplayName('OK'):
                return '<i class="fa fa-check-circle text-success component-status"></i> ' +
                    '<span class="text-success">' + this.getStatusDisplayName('OK') + '</span>';
            case this.getStatusDisplayName('INITIAL'):
                return '<i class="fa fa-minus-circle text-muted component-status"></i> ' +
                    '<span class="text-warning">' + this.getStatusDisplayName('INITIAL') + '</span>';
            case this.getStatusDisplayName('FAILED'):
                return '<i class="fa fa-times-circle text-danger component-status"></i> ' +
                    '<span class="text-danger">' + this.getStatusDisplayName('FAILED') + '</span>';
            default:
                return status;
        }
    };

    this.validateTenantId = function(tenantId) {
        var result = {
            valid: true,
            reason: null
        };
        if (tenantId.indexOf(" ") > -1) {
            result.valid = false;
            result.reason = "Tenant ID must not contain spaces";
            return result;
        }
        return result;
    };

    this.getStatusDisplayName = function(status) {
        switch (status) {
            case "OK":
                return "Active";
            case "INITIAL":
                return "New";
            case "FAILED":
                return "Installation Failed";
        }
    };
});

app.service('TenantService', function($q, $http, _, TenantUtility, SessionUtility){

    function getRandomServiceStatus() {
        var answers = ['OK', 'FAILED', 'INITIAL'];
        var randIdx = Math.floor((Math.random() * 4));
        var result = {
            "state": answers[randIdx],
            "desiredVersion": 1,
            "installedVersion": 1,
            "errorMessage": null
        };
        return result;
    }

    function asyncMockServiceStatus() {
        return $q(function(resolve) {
            setTimeout(function() {
                var answers = ['OK', 'FAILED', 'INITIAL'];
                var randIdx = Math.floor((Math.random() * 3));
                var result = {
                    "state": answers[randIdx],
                    "desiredVersion": 1,
                    "installedVersion": 1,
                    "errorMessage": null
                };
                resolve(result);
            }, Math.floor((Math.random() * 1000) + 1));
        });
    }

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
            console.log(data);
            result.resultObj = _.map(data, function(record){
                return {
                    TenantId: record.key,
                    ContractId: record.value.ContractId,
                    DisplayName: record.value.Properties.displayName,
                    Product: "LPA 2.0",
                    Status: TenantUtility.getStatusDisplayName(getRandomServiceStatus().state),
                    CreatedDate: new Date(),
                    LastModifiedDate: new Date()
                };
            });
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
            console.log(data);
            data.Component = service;
            result.resultObj = data;

            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
        });

        return defer.promise;
    };

    this.GetTenantServiceConfig = function(tenantId, service) {
        return this.GetServiceDefaultConfig(service);
    };

    this.GetTenantServiceStatus = function(tenantId, service) {
        var defer = $q.defer();
        asyncMockServiceStatus().then(function(data){
            defer.resolve(data);
        });
        return defer.promise;
    };
});

