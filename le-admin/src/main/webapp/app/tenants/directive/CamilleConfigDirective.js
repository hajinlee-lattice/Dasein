var app = angular.module("app.tenants.directive.CamilleConfigDirective", [
    'app.tenants.service.TenantService',
    "app.tenants.util.TenantUtility",
    "app.core.util.RecursionCompiler",
    "app.core.directive.FileDownloaderDirective",
    "app.tenants.directive.ListEntryDirective",
    'ui.bootstrap',
    'ui.router',
    'ngSanitize'
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
    this.isBoolean = function(type) { return type === "boolean"; };
    this.isSelect = function(type) { return type === "options"; };
    this.isObject = function(type) { return type === "object"; };
    this.isList = function(type) { return type === "array"; };
    this.isPath = function(type) { return type === "path"; };

});

app.directive('componentsConfig', function(){
    return {
        restrict: 'AE',
        templateUrl: 'app/tenants/view/ComponentsConfigView.html',
        scope: {data: '=', isValid: '=', readonly: '='},
        controller: function($scope, TenantUtility){
            $scope.TenantUtility = TenantUtility;
            $scope.getStatusHtml = function(state) {
                return TenantUtility.getStatusTemplate(TenantUtility.getStatusDisplayName(state));
            };
        }
    };
});

app.directive('camilleConfig', function(RecursionCompiler, TenantUtility){
    return {
        restrict: 'AE',
        templateUrl: 'app/tenants/view/CamilleConfigView.html',
        scope: {config: '=', isValid: '=', readonly: '='},
        controller: function($scope){
            $scope.TenantUtility = TenantUtility;

            $scope.hasChildren =
                $scope.config.hasOwnProperty("Children") &&
                $scope.config.Children.length > 0;

            $scope.hasData = $scope.config.hasOwnProperty("Data");

        },
        compile: function(element) {
            // Use the compile function from the RecursionHelper,
            // And return the linking function(s) which it returns
            return RecursionCompiler.compile(element);
        }
    };
});

app.directive('objectEntry', function(){
    return {
        restrict: 'AE',
        templateUrl: 'app/tenants/view/ObjectEntryView.html',
        scope: {key: '=', value : '=', json: '=', isValid: '=', updater: '&'},
        controller: function($scope, CamilleConfigUtility){
            $scope.isObject = false;
            $scope.type = CamilleConfigUtility.getDataType($scope.value);
            $scope.isInput = CamilleConfigUtility.isInput($scope.type);
            if ($scope.isInput) {
                if ($scope.type === "number") {
                    $scope.inputType = "number";
                } else {
                    $scope.inputType = "text";
                }
            }
            $scope.isBoolean = CamilleConfigUtility.isBoolean($scope.type);

            $scope.isPath = CamilleConfigUtility.isPath($scope.type);
            if ($scope.isPath) {
                $scope.filename = "download.txt";
                $scope.downloadError = false;
            }

            $scope.validateInput = function() {
                if ($scope.configform.$dirty && $scope.configform.$invalid) {
                    $scope.showError = true;
                    $scope.isValid.valid = false;
                    if ($scope.configform.$error.required) {
                        $scope.errorMsg = "cannot be empty.";
                    }
                    if ($scope.configform.$error.number) {
                        $scope.errorMsg = "must be a number.";
                    }
                } else {
                    $scope.showError = false;
                    $scope.isValid.valid = true;
                    $scope.errorMsg = "no error";
                    $scope.updater();
                }
            };
        }
    };
});

app.directive('configEntry', function(){
    return {
        restrict: 'AE',
        templateUrl: 'app/tenants/view/ConfigEntryView.html',
        scope: {config: '=', isValid: '=', isOpen: '=', expandable: '=', readonly: '='},
        controller: function($scope, CamilleConfigUtility){
            if (!$scope.config.hasOwnProperty("Data") && !$scope.config.hasOwnProperty("Children")) {
                $scope.config.Data = "";
            }

            $scope.type = CamilleConfigUtility.getDataType($scope.config);

            $scope.isInput = CamilleConfigUtility.isInput($scope.type) && $scope.config.hasOwnProperty("Data");
            if ($scope.isInput) {
                if ($scope.type === "number") {
                    $scope.inputType = "number";
                    $scope.config.Data = parseFloat($scope.config.Data);
                } else {
                    $scope.inputType = "text";
                }
            }

            $scope.isBoolean = CamilleConfigUtility.isBoolean($scope.type);
            if ($scope.isBoolean) $scope.config.Data = ($scope.config.Data === "true" || $scope.config.Data === true);

            $scope.isSelect = CamilleConfigUtility.isSelect($scope.type);
            $scope.isObject = CamilleConfigUtility.isObject($scope.type);
            if ($scope.isObject) {
                $scope.jsonData = JSON.parse($scope.config.Data);
                $scope.objectUpdater = function() {
                    $scope.config.Data = JSON.stringify($scope.jsonData);
                };
            }

            $scope.isList = CamilleConfigUtility.isList($scope.type);
            if ($scope.isList) {
                $scope.listData =
                    _.reduce(
                        JSON.parse($scope.config.Data),
                        function(agg, item){ return agg + ", " + item; },
                        "");
                $scope.listData = $scope.listData.substring(1);
            }

            if ($scope.isSelect) { $scope.options = $scope.config.Metadata.Options; }

            $scope.isPath = CamilleConfigUtility.isPath($scope.type);

            $scope.validateInput = function() {
                if ($scope.configform.$dirty && $scope.configform.$invalid) {
                    $scope.showError = true;
                    $scope.isValid.valid = false;
                    if ($scope.configform.$error.required) {
                        $scope.errorMsg = "cannot be empty.";
                    }
                    if ($scope.configform.$error.number) {
                        $scope.errorMsg = "must be a number.";
                    }
                } else {
                    $scope.showError = false;
                    $scope.isValid.valid = true;
                    $scope.errorMsg = "no error";
                }
            };

        }
    };
});