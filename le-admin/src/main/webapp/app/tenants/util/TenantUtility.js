var app = angular.module("app.tenants.util.TenantUtility", [
    'le.common.util.UnderscoreUtility'
]);

app.service('TenantUtility', function(_){

    function constructTenantRegistration(components, tenantId, contractId, spaceId, spaceInfo) {
        var result = {};
        result.ConfigDirectories = _.map(components,
            function(component){
                var componentConfig = {
                    RootPath: "/" + component.Component
                };
                if (component.hasOwnProperty("Nodes")) {
                    componentConfig.Nodes = cleanupConfigData(component.Nodes);
                }
                return componentConfig;
            });
        if (spaceInfo == null) {
            result.CustomerSpaceInfo = {
                properties: {
                    displayName: "LPA_" + tenantId,
                    description: "A LPA solution for " + tenantId + " in " + contractId
                },
                featureFlags: ""
            };
        }
        return result;
    }

    function cleanupConfigData(configs) {
        return _.map(configs,
            function(config){
                var cleanedConfig = {Node: config.Node};
                if (config.hasOwnProperty("Data")) {
                    cleanedConfig.Data = config.Data.toString();
                }
                if (config.hasOwnProperty("Children")) {
                    cleanedConfig.Children = cleanupConfigData(config.Children);
                }
                return cleanedConfig;
            });
    }

    this.constructTenantRegistration = constructTenantRegistration;

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
            case "UNKNOWN":
            default:
                return "Unknown state";
        }
    };
});

