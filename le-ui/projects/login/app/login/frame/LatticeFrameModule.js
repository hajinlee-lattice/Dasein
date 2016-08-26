angular.module('login.frame', [
    'mainApp.appCommon.directives.ngEnterDirective',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.login.services.LoginService',
    'mainApp.core.services.ResourceStringsService'
])
.controller('LatticeFrameController', function(
    $scope, $state, ResourceUtility, LoginService, LoginStore, LoginDocument, ClientSession
) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.login = LoginStore.login;
    $scope.state = $state;

    LoginStore.set(LoginDocument, ClientSession);

    switch($state.current.name) {
        case 'login.form': 
            if ($scope.login.username) {
                $state.go('login.tenants');
            }
            break;
        case 'login.tenants':
            if (!$scope.login.username) {
                $state.go('login.form');
            }
            break;
    }

    $scope.clickLogout = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        LoginService.Logout();
        
        $state.go('login.form');
    }

    $scope.clickModelList = function() {
        LoginStore.redirectToLP();
    }
})