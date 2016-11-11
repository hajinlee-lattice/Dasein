var app = angular.module("app.core.directive.MainNavDirective", [
    'ui.router',
    'le.common.util.BrowserStorageUtility'
]);

app.service('MainNavService', function(){
    this.parseNavState = function (stateName) {
        if (stateName.indexOf("TENANT") === 0) {
            return "Tenants";
        }
        if (stateName.indexOf("MODELQUALITY") === 0) {
            return "ModelQuality";
        }
        if (stateName.indexOf("DATACLOUD") === 0) {
            return "DataCloud";
        }
        if (stateName.indexOf("METADATA") === 0) {
            return "Metadata";
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
                var loginDoc = BrowserStorageUtility.getLoginDocument();
                if(loginDoc === null){
                    BrowserStorageUtility.clear();
                    $state.go('LOGIN');
                } else {
                    $scope.username = loginDoc.Principal;
                }
                $scope.activeState = MainNavService.parseNavState($state.current.name);
            }
        }
    };
});


