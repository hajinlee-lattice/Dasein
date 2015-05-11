(function(){

    var app = angular.module("app.tenants.util.CamilleConfigUtility", [
        'le.common.util.UnderscoreUtility'
    ]);

    app.service('CamilleConfigUtility', function(){
        this.getDataType = function(config) {
            var data = config.Data;
            if (config.hasOwnProperty("Metadata")) {
                return config.Metadata.Type;
            } else if (typeof data === "number") {
                return "number";
            } else if (typeof data === "boolean") {
                return "boolean";
            } else if (typeof data === "object") {
                return "object";
            } else if (Object.prototype.toString.call( data ) === '[object Array]') {
                return "array";
            } else {
                return "string";
            }
        };
        this.isInput = function(type) {
            switch (type) {
                case "boolean":
                case "options":
                case "object":
                case "array":
                    return false;
                default:
                    return true;
            }
        };

        this.isSelect = function(type) { return type === "options"; };
        this.isObject = function(type) { return type === "object"; };
        this.isList = function(type) { return type === "array"; };
        this.isPath = function(type) { return type === "path"; };

        this.isBoolean = function(type) { return type === "boolean"; };
        this.isNumber = function(type) { return type === "number"; };
        this.isPassword = function(type) { return type === "password"; };

    });

}).call();
