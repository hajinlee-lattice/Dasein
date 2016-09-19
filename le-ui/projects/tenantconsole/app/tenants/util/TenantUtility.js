var app = angular.module("app.tenants.util.TenantUtility", [
    'le.common.util.UnderscoreUtility'
]);

app.service('TenantUtility', function(_){

    function convertTenantRecordToGridData (record) {
        var result = {
            TenantId: record.CustomerSpace.tenantId,
            ContractId: record.CustomerSpace.contractId,
            DisplayName: record.TenantInfo.properties.displayName,
            Product: record.SpaceConfiguration.Product,
            Status: record.BootstrapState.state,
            CreatedDate: new Date(record.TenantInfo.properties.created),
            LastModifiedDate: new Date(record.TenantInfo.properties.lastModified)
        };

        if (result.Status === 'ERROR') {
            console.warn("ERROR in the tenant " + result.TenantId + " : " + record.BootstrapState.errorMessage);
        }
        result.Status = getStatusDisplayName(result.Status);

        return result;
    }

    this.convertTenantRecordToGridData = convertTenantRecordToGridData;

    this.parseTenantInfo = function(data) {
        return data;
    };

    function constructTenantRegistration(components, tenantId, contractId, infos, spaceConfig, featureFlags) {
        var result = {};
        result.ConfigDirectories = _.map(components,
            function(component){
                var componentConfig = {
                    RootPath: "/" + component.Component
                };
                if (component.hasOwnProperty("Nodes")) {
                    componentConfig.Nodes = cleanupConfigData(component.Nodes);
                } else {
                    componentConfig.Nodes = [];
                }
                return componentConfig;
            });

        if (!featureFlags.hasOwnProperty("Dante") || featureFlags.Dante === false) {
            result.ConfigDirectories = _.reject(result.ConfigDirectories, {"RootPath": "/Dante"});
        }

        if (typeof(infos) === "undefined" || infos === null) infos = {};

        if (infos.hasOwnProperty("CustomerSpaceInfo")) {
            result.CustomerSpaceInfo = infos.CustomerSpaceInfo;
        } else {
            result.CustomerSpaceInfo = {
                properties: {
                    displayName: "LPA_" + tenantId,
                    description: "A LPA solution for " + tenantId + " in " + contractId,
                    product: "LPA 2.0",
                    topology: "Marketo"
                },
                featureFlags: ""
            };
        }

        if (featureFlags !== null) {
            result.CustomerSpaceInfo.featureFlags = JSON.stringify(featureFlags);
        }

        if (infos.hasOwnProperty("TenantInfo")) {
            result.TenantInfo = infos.TenantInfo;
        } else {
            result.TenantInfo = {
                properties: {
                    displayName: "Test LPA tenant",
                    description: "A LPA tenant under the contract " + contractId
                }
            };
        }

        if (infos.hasOwnProperty("ContractInfo")) {
            result.ContractInfo = infos.ContractInfo;
        } else {
            result.ContractInfo = { properties: {} };
        }

        result.SpaceConfig = spaceConfig;

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
            case this.getStatusDisplayName('MIGRATED'):
                return '<i class="fa fa-arrow-circle-up text-muted component-status"></i> ' +
                    '<span class="text-muted">' + this.getStatusDisplayName('MIGRATED') + '</span>';
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
        if (tenantId === null || tenantId === "") {
            result.valid = false;
            result.reason = "Tenant ID cannot be empty";
            return result;
        }
        if (tenantId.indexOf(" ") > -1 || tenantId.indexOf("\t") > -1) {
            result.valid = false;
            result.reason = "Tenant ID must not contain spaces";
            return result;
        }
        if (tenantId.indexOf(".") > -1) {
            result.valid = false;
            result.reason = "Tenant ID must not contain periods";
            return result;
        }
        if (tenantId.indexOf("\\") > -1 || tenantId.indexOf("/") > -1) {
            result.valid = false;
            result.reason = "Tenant ID must not contain slashes";
            return result;
        }
        if (tenantId.indexOf("-") > -1) {
            result.valid = false;
            result.reason = "Tenant ID must not contain dashes";
            return result;
        }
        var regexp = /^[a-zA-Z0-9_]+$/;
        if(!tenantId.match(regexp)) {
            result.valid = false;
            result.reason = "Tenant ID must be alpha numeric";
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
            case "MIGRATED":
                return "Migrated";
            case "UNKNOWN":
                /* falls through */
            default:
                return "Unknown state";
        }
    }
    this.getStatusDisplayName = getStatusDisplayName;


    this.parseBootstrapErrorMsg = function(message) {
        var idx = message.indexOf("::");
        if (idx != -1) {
            return message.substring(0, idx);
        }
    };

    function selectNodeByPath(nodes, path, rootPath) {
        for (var i = 0; i < nodes.length; i++) {
            var node = nodes[i];
            if ((rootPath + "/" + node.Node) === path) {
                return node;
            }
            if (node.hasOwnProperty("Children")) {
                var nodeInChildren = selectNodeByPath(node.Children, path, rootPath + "/" + node.Node);
                if (nodeInChildren !== null) return nodeInChildren;
            }
        }
        return null;
    }

    function findNodeByPath(component, path) {
        if (component.hasOwnProperty("Nodes")) {
            return selectNodeByPath(component.Nodes, path, "");
        }
        return null;
    }
    this.findNodeByPath = findNodeByPath;

    function getDerivedParameter(components, parameter) {
        var component = _.find(components, {Component: parameter.Component});
        if (component !== null) {
            var node= findNodeByPath(component, parameter.NodePath);
            if (node !== null) {
                if (node.hasOwnProperty("Metadata") &&
                    (!node.Metadata.hasOwnProperty("Type") || node.Metadata.Type === "string" || node.Metadata.Type === "options")) {
                    return node.Data.replace(/\\/g, "\\\\");
                } else {
                    return node.Data;
                }
            }
        }
        return null;
    }
    this.getDerivedParameter = getDerivedParameter;

    function evalExpressionWithValues(expression, values, scope) {
        // replace tokens by values one by one
        for (var i = 0; i < values.length; i++){
            var token = "\\\{" + i + "\\\}";
            var re = new RegExp(token, 'g');
            expression = expression.replace(re, values[i]);
        }
        /* jshint ignore:start */
        return eval(expression);
        /* jshint ignore:end */
    }
    this.evalExpressionWithValues = evalExpressionWithValues;

    function calcDerivation(components, derivation, scope) {
        var values = _.map(derivation.Parameters, function(par){
            return getDerivedParameter(components, par);
        });
        return evalExpressionWithValues(derivation.Expression, values, scope);
    }
    this.calcDerivation = calcDerivation;
});

