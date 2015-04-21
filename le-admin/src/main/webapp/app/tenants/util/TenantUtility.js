var app = angular.module("app.tenants.util.TenantUtility", [
    'le.common.util.UnderscoreUtility'
]);

app.service('TenantUtility', function(_){

    function convertTenantRecordToGridData (record) {
        var result = {
            TenantId: record.key,
            ContractId: record.value.contractId,
            DisplayName: record.value.properties.displayName,
            Status: record.value.bootstrapState.state,
            CreatedDate: new Date(record.value.properties.created),
            LastModifiedDate: new Date(record.value.properties.lastModified)
        };
        var spaceInfo = record.value.spaceInfoList[0];
        result.Product = spaceInfo.properties.product;

        if (result.Status === 'ERROR') {
            console.warn("ERROR in the tenant " + result.TenantId + " : " + record.value.bootstrapState.errorMessage);
        }
        result.Status = getStatusDisplayName(result.Status);

        return result;
    }

    this.convertTenantRecordToGridData = convertTenantRecordToGridData;

    this.parseTenantInfo = function(data) {
        return data;
    };

    function constructTenantRegistration(components, tenantId, contractId, infos) {
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

        if (typeof(infos) === "undefined" || infos === null) infos = {};

        if (infos.hasOwnProperty("CustomerSpaceInfo")) {
            result.CustomerSpaceInfo = infos.CustomerSpaceInfo;
        } else {
            result.CustomerSpaceInfo = {
                properties: {
                    displayName: "LPA_" + tenantId,
                    description: "A LPA solution for " + tenantId + " in " + contractId
                },
                featureFlags: ""
            };
        }

        if (infos.hasOwnProperty("TenantInfo")) {
            result.TenantInfo = infos.TenantInfo;
        } else {
            result.TenantInfo = {
                properties: {
                    displayName: "Test LPA tenant",
                    description: "A LPA tenant under the contract " + contractId
                },
                contractId: contractId
            };
        }

        if (infos.hasOwnProperty("ContractInfo")) {
            result.ContractInfo = infos.ContractInfo;
        } else {
            result.ContractInfo = { properties: {} };
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
                return '<i class="fa fa-minus-circle text-warning component-status"></i> ' +
                    '<span class="text-warning">' + this.getStatusDisplayName('INITIAL') + '</span>';
            case this.getStatusDisplayName('ERROR'):
                return '<i class="fa fa-times-circle text-danger component-status"></i> ' +
                    '<span class="text-danger">' + this.getStatusDisplayName('ERROR') + '</span>';
            case this.getStatusDisplayName('UNKNOWN'):
                return '<i class="fa fa-question-circle text-muted component-status"></i> ' +
                    '<span class="text-muted">' + this.getStatusDisplayName('UNKNOWN') + '</span>';
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

    function getStatusDisplayName(status) {
        switch (status) {
            case "OK":
                return "Active";
            case "INITIAL":
                return "New";
            case "ERROR":
                return "Installation Failed";
            case "UNKNOWN":
            default:
                return "Unknown state";
        }
    }
    this.getStatusDisplayName = getStatusDisplayName;
});

