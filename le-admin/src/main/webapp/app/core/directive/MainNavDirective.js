var app = angular.module("app.core.directive.MainNavDirective", []);

app.directive('mainNav', function(){
    return {
        restrict: 'E',
        templateUrl: 'app/core/view/MainNavView.html',
        scope: true
    };
});
