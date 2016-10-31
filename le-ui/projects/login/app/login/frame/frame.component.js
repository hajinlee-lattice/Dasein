angular.module('login.frame', [
    'mainApp.appCommon.directives.ngEnterDirective',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.login.services.LoginService',
    'mainApp.core.services.ResourceStringsService'
])
.component('loginFrame', {
    templateUrl: 'app/login/frame/frame.component.html',
    controller: function(
        $scope, $state, $timeout, ResourceUtility, LoginService, 
        SessionTimeoutUtility, LoginStore
    ) {
        var vm = this,
            resolve = $scope.$parent.$resolve,
            ClientSession = resolve.ClientSession,
            LoginDocument = resolve.LoginDocument;
        
        vm.ResourceUtility = ResourceUtility;
        vm.login = LoginStore.login;
        vm.state = $state;

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

        vm.clickLogout = function ($event) {
            if ($event != null) {
                $event.preventDefault();
            }
            LoginService.Logout();
        }

        vm.clickModelList = function() {
            LoginStore.redirectToLP();
        }
    }
});