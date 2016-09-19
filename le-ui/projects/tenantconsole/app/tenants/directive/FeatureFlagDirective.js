var app = angular.module("app.tenants.directive.FeatureFlagDirective", []);

app.directive('featureFlag', function(){
    return {
        restrict: 'AE',
        template: '<form ng-if="!readonly" ng-hide={{show}} class="row form-inline form-feature-flags">' +
        '<label class="control-label">{{ name }}</label>' +
        '<input class="checkbox" type="checkbox" ng-model="checkbox.value" ng-change="checked()"/>' +
        '</form>' +
        '<div ng-if="readonly" class="row" ng-hide={{show}}>' +
        '<span class="feature-flag" ng-class="{\'text-success\': flag, \'text-danger\': !flag}">{{ name }} ' +
        '<i ng-if="flag" class="fa fa-check"></i>' +
        '<i ng-if="!flag" class="fa fa-times"></i>' +
        '</span>' +
        '</div>',
        scope: {name: '@', flag: '=', readonly:'=', show: '@'},
        controller: function($scope){
            $scope.checkbox = {
                value: $scope.flag
            };

            $scope.checked = function(){
              $scope.flag = $scope.checkbox.value;
            };
        }
    };
});