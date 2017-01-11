var app = angular.module("app.tenants.directive.FeatureFlagDirective", []);

app.directive('featureFlag', function(){
    return {
        restrict: 'AE',
        template: '<form ng-if="!readonly || editing" class="row form-inline form-feature-flags">' +
        '<label class="control-label">{{ flag.DisplayName }}</label>' +
        '<input class="checkbox" type="checkbox" ng-model="flag.Value" ng-change="checked()" ng-disabled="readonly && !flag.ModifiableAfterProvisioning"/>' +
        '</form>' +
        '<div ng-if="readonly && !editing" class="row">' +
        '<span class="feature-flag" ng-class="{\'text-success\': flag.Value, \'text-danger\': !flag.Value}">{{ flag.DisplayName }} ' +
        '<i ng-if="flag.Value" class="fa fa-check"></i>' +
        '<i ng-if="!flag.Value" class="fa fa-times"></i>' +
        '</span>' +
        '</div>',
        scope: {flag: '=', readonly:'=', editing: "="},
        controller: function($scope){

        }
    };
});