var app = angular.module("app.login.controller.LoginCtrl", [
    'le.common.directives.ngEnterDirective',
    'le.common.util.BrowserStorageUtility',
    "app.login.service.LoginService"
]);

app.controller('LoginCtrl', function($scope, $state, BrowserStorageUtility, LoginService){

    restoreSession();

    $scope.loginInProgress = false;

    $scope.onLoginClick = function(){
        if ($scope.loginInProgress) return;
        $scope.loginInProgress = true;
        $scope.showLoginError = false;
        BrowserStorageUtility.clear(false);
        login();
    };

    function restoreSession() {
        var token = BrowserStorageUtility.getTokenDocument();
        if (token !== null) {
            $state.go('TENANT.LIST');
        }
    }

    function login(){
        LoginService.Login($scope.Username, $scope.Password).then(function(result){
            if (result.success) {
                $state.go('TENANT.LIST');
            } else {
                $scope.showLoginError = true;
                $scope.loginErrorMsg = "Authentication failed.";
            }
            $scope.loginInProgress = false;
        });
    }

    $scope.showLoginForm = true;

});
