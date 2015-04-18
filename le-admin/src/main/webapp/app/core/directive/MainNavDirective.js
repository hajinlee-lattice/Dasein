var app = angular.module("app.core.directive.MainNavDirective", [
    'ui.router',
    'le.common.util.BrowserStorageUtility'
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
        controller: function ($scope, $rootScope, $state,
                              MainNavService, BrowserStorageUtility) {
            routeToCorrectState();


            $rootScope.$on('$stateChangeSuccess', function () { routeToCorrectState(); });

            $scope.onSignOutClick = function() {
                BrowserStorageUtility.clear();
                $state.go('LOGIN');
            };

            function routeToCorrectState(){
                if(BrowserStorageUtility.getTokenDocument() === null){
                    BrowserStorageUtility.clear();
                    $state.go('LOGIN');
                }
                $scope.activeState = MainNavService.parseNavState($state.current.name);
            }
        }
    };
});


