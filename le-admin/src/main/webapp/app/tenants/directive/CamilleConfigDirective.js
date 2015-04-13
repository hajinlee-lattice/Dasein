var app = angular.module("app.tenants.directive.CamilleConfigDirective", [
    'app.tenants.service.TenantService',
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
        var data = config.data;
        if (config.hasOwnProperty("metadata")) {
            return config.metadata.type;
        } else if (typeof data === "number") {
            return "number";
        } else if (typeof data === "boolean") {
            return "boolean";
        } else {
            return "string";
        }
    };
    this.isInput = function(type) {
        switch (type) {
            case "boolean":
            case "options":
                return false;
            default:
                return true;
        }
    };
    this.isBoolean = function(type) { return type === "boolean"; };
    this.isSelect = function(type) { return type === "options"; };
});

app.directive('componentsConfig', function(){
    return {
        restrict: 'AE',
        templateUrl: 'app/tenants/view/ComponentsConfigView.html',
        scope: {data: '=', isValid: '=', mode: '='},
        controller: function($scope, TenantUtility){
            $scope.readonly = ($scope.mode !== "NEW");
            $scope.TenantUtility = TenantUtility;
            $scope.getStatusHtml = function(state) {
                return TenantUtility.getStatusTemplate(state);
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
                $scope.config.hasOwnProperty("children") &&
                $scope.config.children.length > 0;

            $scope.hasData = $scope.config.hasOwnProperty("data");

        },
        compile: function(element) {
            // Use the compile function from the RecursionHelper,
            // And return the linking function(s) which it returns
            return RecursionHelper.compile(element);
        }
    };
});

app.directive('configEntry', function(){
    return {
        restrict: 'AE',
        templateUrl: 'app/tenants/view/ConfigEntryView.html',
        scope: {config: '=', isValid: '=', isOpen: '=', expandable: '=', readonly: '='},
        controller: function($scope, CamilleConfigUtility){
            $scope.type =
                CamilleConfigUtility.getDataType($scope.config);

            $scope.isInput = CamilleConfigUtility.isInput($scope.type);
            if ($scope.isInput) {
                if ($scope.type === "number") {
                    $scope.inputType = "number";
                } else {
                    $scope.inputType = "text";
                }
            }

            $scope.isBoolean = CamilleConfigUtility.isBoolean($scope.type);
            $scope.isSelect = CamilleConfigUtility.isSelect($scope.type);

            if ($scope.isSelect) {
                $scope.options = $scope.config.metadata.options;
                if ($scope.config.metadata.hasOwnProperty("default")) {
                    $scope.defaultOption = $scope.config.metadata.default;
                } else {
                    $scope.defaultOption = $scope.options[0];
                }
                if (!$scope.config.hasOwnProperty("data")) {
                    $scope.config.data = $scope.defaultOption;
                }
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
                }
            };

        }
    };
});