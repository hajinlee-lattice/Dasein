var app = angular.module("app.tenants.service.TenantService", [
    'le.common.util.UnderscoreUtility'
]);

app.service('TenantService', function($q, $http, _){

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

    function asyncMockServiceStatus() {
        return $q(function(resolve) {
            setTimeout(function() {
                var answers = ['OK', 'FAILED', 'INITIAL', 'INSTALLING'];
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
                   DisplayName: record.value.Properties.displayName,
                   VDB: "INITIAL",
                   PLS: "OK",
                   Dante: "FAILED",
                   GlobalAuth: "OK",
                   DataLoader: "INSTALLING",
                   TPL: "OK"
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

            data.rootPath = "/Pods/Default/Contracts/CONTRACT1/Tenants/Tenant1/Spaces/production/Services/" + service;

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

app.service('TenantUtility', function(_){

    function applyMetadataToComponent(component, metadata) {
        _.each(component.configuration, function(dataNode){
            var metaNode = _.findWhere(metadata, {"node": dataNode.node});
            if (metaNode) {
                applyMetadataToNode(dataNode, metaNode);
            }
        });
    }

    function applyMetadataToNode(data, metadata) {
        if (data.node === metadata.node) {
            if (metadata.hasOwnProperty("metadata")) {
                data.metadata = metadata.metadata;
            }
            if (
                data.hasOwnProperty("children") &&
                metadata.hasOwnProperty("children")
            ) {
                _.each(data.children, function(child){
                    var metaChild = _.findWhere(metadata.children, {"node": child.node});
                    if (metaChild) {
                        applyMetadataToNode(child, metaChild);
                    }
                });
            }
        }
    }

    this.applyMetadataToComponent = applyMetadataToComponent;

    function cleanupComponentConfigs(components) {
        return _.map(components,
            function(component){
                var componentConfig = {
                    component: config.component,
                    rootPath:  config.rootPath
                };
                if (config.hasOwnProperty("children")) {
                    componentConfig.children = cleanupConfigData(config.children);
                }
                return componentConfig;
            });
    }

    function cleanupConfigData(configs) {
        return _.map(configs,
            function(config){
                var cleanedConfig = {node: config.node};
                if (config.hasOwnProperty("data")) {
                    cleanedConfig.data = config.data;
                }
                if (config.hasOwnProperty("children")) {
                    cleanedConfig.children = cleanupConfigData(config.children);
                }
                return cleanedConfig;
            });
    }

    this.cleanupComponentConfigs = cleanupComponentConfigs;

    this.getStatusTemplate = function(status) {
        switch (status) {
            case 'OK':
                return '<i class="fa fa-check-circle text-success component-status"></i> ' +
                    '<span class="text-success">OK</span>';
            case 'INITIAL':
                return '<i class="fa fa-exclamation-circle text-warning component-status"></i> ' +
                    '<span class="text-warning">INITIAL</span>';
            case 'FAILED':
                return '<i class="fa fa-times-circle text-danger component-status"></i> ' +
                    '<span class="text-danger">FAILED</span>';
            case 'INSTALLING':
                return '<i class="fa fa-minus-circle text-muted component-status"></i> ' +
                    '<span class="text-muted">INSTALLING</span>';
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
});