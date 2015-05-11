(function(){
    var app = angular.module("app.tenants.directive.ObjectEntryDirective", [
        "app.tenants.util.CamilleConfigUtility",
        'le.common.util.UnderscoreUtility'
    ]);

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
                };
            }
        };
    });
}).call();
