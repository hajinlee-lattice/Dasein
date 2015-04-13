var app = angular.module("app.core.directive.MainNavDirective", [
    'ui.router'
]);

app.service('MainNavService', function(){
    this.parseNavState = function (stateName) {
        if (stateName.indexOf("TENANT") === 0) {
            return "Tenants";
        }
        return "unknown";
    };
});

app.directive('mainNav', function(){
    return {
        restrict: 'AE',
        templateUrl: 'app/core/view/MainNavView.html',
        scope: {activeNav: '='},
        controller: ['$scope', '$rootScope', '$state', 'MainNavService',
            function ($scope, $rootScope, $state, MainNavService) {
                $scope.activeState = MainNavService.parseNavState($state.current.name);

                $rootScope.$on('$stateChangeSuccess', function () {
                    $scope.activeState = MainNavService.parseNavState($state.current.name);
                });
            }]
    };
});
