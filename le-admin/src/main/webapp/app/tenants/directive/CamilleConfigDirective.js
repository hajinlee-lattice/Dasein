var app = angular.module("app.tenants.directive.CamilleConfigDirective", [
    'app.tenants.service.TenantService',
    "app.tenants.util.TenantUtility",
    'ui.bootstrap',
    'ui.router',
    'ngSanitize'
]);

app.factory('RecursionHelper', ['$compile', function($compile){
    return {
        /**
         * Manually compiles the element, fixing the recursion loop.
         * @param element
         * @param [link] A post-link function, or an object with function(s) registered via pre and post properties.
         * @returns An object containing the linking functions.
         */
        compile: function(element, link){
            // Normalize the link parameter
            if(angular.isFunction(link)){
                link = { post: link };
            }

            // Break the recursion loop by removing the contents
            var contents = element.contents().remove();
            var compiledContents;
            return {
                pre: (link && link.pre) ? link.pre : null,
                /**
                 * Compiles and re-adds the contents
                 */
                post: function(scope, element){
                    // Compile the contents
                    if(!compiledContents){
                        compiledContents = $compile(contents);
                    }
                    // Re-add the compiled contents to the element
                    compiledContents(scope, function(clone){
                        element.append(clone);
                    });

                    // Call the post-linking function, if any
                    if(link && link.post){
                        link.post.apply(null, arguments);
                    }
                }
            };
        }
    };
}]);

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
        } else {
            return "string";
        }
    };
    this.isInput = function(type) {
        switch (type) {
            case "boolean":
            case "options":
            case "object":
                return false;
            default:
                return true;
        }
    };
    this.isBoolean = function(type) { return type === "boolean"; };
    this.isSelect = function(type) { return type === "options"; };
    this.isObject = function(type) { return type === "object"; };

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

app.directive('camilleConfig', function(RecursionHelper, TenantUtility){
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
            return RecursionHelper.compile(element);
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
            }
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
                }
            }

            if ($scope.isSelect) { $scope.options = $scope.config.Metadata.Options; }

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