var app = angular.module("app.core.directive.MainNavDirective", [
    'ui.router'
]);

app.service('MainNavService', function(){
    this.parseNavState = function (stateName) {
        if (stateName === "TENANTS" || stateName === "TENANT_INFO") {
            return "Tenants"
        }
        return "unknown";
    };
});

app.directive('mainNav', function(){
    return {
        restrict: 'E',
        templateUrl: 'app/core/view/MainNavView.html',
        scope: true,
        controller: ['$scope', '$rootScope', '$state', 'MainNavService',
            function ($scope, $rootScope, $state, MainNavService) {
                $rootScope.$on('$stateChangeSuccess', function () {
                    $scope.navState = MainNavService.parseNavState($state.current.name);
                });
            }]
    };
});
