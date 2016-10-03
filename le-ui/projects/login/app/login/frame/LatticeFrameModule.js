angular.module('login.frame', [
    'mainApp.appCommon.directives.ngEnterDirective',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.login.services.LoginService',
    'mainApp.core.services.ResourceStringsService'
])
.controller('LatticeFrameController', function(
    $scope, $state, $timeout, ResourceUtility, LoginService, SessionTimeoutUtility, 
    LoginStore, LoginDocument, ClientSession
) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.login = LoginStore.login;
    $scope.state = $state;

    if (SessionTimeoutUtility.hasSessionTimedOut() && LoginDocument.UserName) {
        return LoginService.Logout();
    } else {
        switch($state.current.name) {
            case 'login.form': 
                if (LoginDocument.UserName) {
                    $state.go('login.tenants');
                }
                break;
            case 'login.tenants':
                if (!LoginDocument.UserName) {
                    $state.go('login.form');
                }
                break;
        }
    }

    LoginStore.set(LoginDocument, ClientSession);

    $timeout(function() {
        angular.element('body').addClass('initialized');
    },1)

    $scope.clickLogout = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        LoginService.Logout();
    }

    $scope.clickModelList = function() {
        LoginStore.redirectToLP();
    }
})