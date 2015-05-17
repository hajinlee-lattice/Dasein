var app = angular.module("app.tenants.directive.CamilleConfigDirective", [
    'app.tenants.service.TenantService',
    "app.tenants.util.TenantUtility",
    "app.tenants.util.CamilleConfigUtility",
    "app.core.util.RecursionCompiler",
    "app.core.directive.FileDownloaderDirective",
    "app.tenants.directive.ObjectEntryDirective",
    "app.tenants.directive.ListEntryDirective",
    'ui.bootstrap',
    'ui.router',
    'ngSanitize'
]);

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
                if (CamilleConfigUtility.isNumber($scope.type)) {
                    $scope.inputType = "number";
                    $scope.config.Data = parseFloat($scope.config.Data);
                } else if (CamilleConfigUtility.isPassword($scope.type)) {
                    $scope.inputType = "password";
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

            if ($scope.isSelect) {
                $scope.options = $scope.config.Metadata.Options;
                if ($scope.options.length == 0) {
                    $scope.showError = true;
                    $scope.isValid.valid = false;
                    $scope.errorMsg = "no available choices.";
                } else if ($scope.options.indexOf($scope.config.Data) == -1) {
                    $scope.showError = true;
                    $scope.isValid.valid = false;
                    $scope.errorMsg = "not a valid choice.";
                }
            }

            $scope.isPath = CamilleConfigUtility.isPath($scope.type);
            if ($scope.isPath) {
                $scope.fileurl = "/admin/serverfiles?path=" + encodeURI($scope.config.Data);
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
                    if ($scope.isSelect && $scope.options.indexOf($scope.config.Data) == -1) {
                        $scope.errorMsg = "not a valid choice.";
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