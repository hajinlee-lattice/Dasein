var app = angular.module("app.login.controller.LoginCtrl", [
    'le.common.util.BrowserStorageUtility',
    "app.login.service.LoginService"
]);

app.controller('LoginCtrl', function($scope, $state, BrowserStorageUtility, LoginService){

    restoreSession();

    $scope.onLoginClick = function(){
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
        });
    }

    $scope.showLoginForm = true;

});
