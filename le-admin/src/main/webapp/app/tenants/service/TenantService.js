var app = angular.module("app.tenants.service.TenantService", [
    'le.common.util.UnderscoreUtility'
]);

app.service('TenantUtility', function(_){

    function applyMetadataToComponent(component, metadata) {
        _.each(component.Nodes, function(dataNode){
            var metaNode = _.findWhere(metadata.Nodes, {"Node": dataNode.Node});
            if (metaNode) {
                applyMetadataToNode(dataNode, metaNode);
            }
        });
    }

    function applyMetadataToNode(data, metadata) {
        if (data.Node === metadata.Node) {
            if (metadata.hasOwnProperty("Data") && typeof metadata.Data === "object") {
                data.Metadata = metadata.Data;
            }
            if (
                data.hasOwnProperty("Children") &&
                metadata.hasOwnProperty("Children")
            ) {
                _.each(data.Children, function(child){
                    var metaChild = _.findWhere(metadata.Children, {"Node": child.Node});
                    if (metaChild) {
                        applyMetadataToNode(child, metaChild);
                    }
                });
            }
        }
    }

    this.applyMetadataToComponent = applyMetadataToComponent;

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

app.service('TenantService', function($q, $http, _, TenantUtility){

    function asycMockAllTenants() {
        return $q(function(resolve) {
            setTimeout(function() {
                $http.get('/assets/json/tenants_mock.json').then(function(response){
                    resolve(response.data);
                });
            }, 500);
        });
    }

    function asyncMockTenantService(tenant, service) {
        var url = '/assets/json/' + service.toLowerCase() + '_default.json';
        return $q(function(resolve) {
            setTimeout(function() {
                $http.get(url).then(
                    function(response){
                        resolve(response.data);
                    }
                );
            }, 10 * Math.floor((Math.random() * 100) + 1));
        });
    }

    function asyncMockServiceMetadata(service) {
        var url = '/assets/json/' + service.toLowerCase() + '_metadata.json';
        return $q(function(resolve) {
            setTimeout(function() {
                $http.get(url).then(
                    function(response){
                        resolve(response.data);
                    }
                );
            }, 10 * Math.floor((Math.random() * 100) + 1));
        });
    }

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
                var randIdx = Math.floor((Math.random() * 4));
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

        asycMockAllTenants().then(function(data){

            var tenants = _.map(data, function(record){
               return {
                   TenantId: record.key,
                   ContractId: "CONTRACT" + Math.floor((Math.random() * 10) + 1),
                   DisplayName: record.value.Properties.displayName,
                   Product: "LPA 2.0",
                   Status: TenantUtility.getStatusDisplayName(getRandomServiceStatus().state),
                   CreatedDate: new Date()
                };
            });

            var result = {
                success: true,
                resultObj: tenants
            };

            defer.resolve(result);
        });

        return defer.promise;
    };

    this.GetTenantServiceConfig = function(tenantId, service) {
        var defer = $q.defer();

        asyncMockTenantService(tenantId, service).then(function(data){

            data.Component = service;

            var result = {
                success: true,
                resultObj: data
            };

            defer.resolve(result);
        });

        return defer.promise;
    };

    this.GetServiceMetadata = function(service) {
        var defer = $q.defer();
        asyncMockServiceMetadata(service).then(function(data){
            defer.resolve(data);
        });
        return defer.promise;
    };

    this.GetTenantServiceStatus = function(tenantId, service) {
        var defer = $q.defer();
        asyncMockServiceStatus().then(function(data){
            defer.resolve(data);
        });
        return defer.promise;
    };
});

