angular.module('login.form', [
    'mainApp.appCommon.directives.ngEnterDirective',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.login.services.LoginService'
])
.component('loginForm', {
    templateUrl: 'app/login/form/form.component.html',
    controller: function (
        $state, ResourceUtility, LoginService, SessionTimeoutUtility
    ) {
        var vm = this;

        angular.extend(vm, {
            ResourceUtility: ResourceUtility,
            username: "",
            password: "",
            visible: false,
            loginMessage: null,
            loginErrorMessage: null,
            showLoginError: false,
            showSuccessMessage: false,
            successMessage: "",
            loginInProgress: false,
            showForgotPassword: false,
            forgotPasswordUsername: "",
            copyrightString: ResourceUtility.getString('LOGIN_COPYRIGHT', ['2010 - ' + (new Date()).getFullYear()]),
            forgotPasswordErrorMessage: ""
        })

        vm.init = function() {
            vm.visible = true;

            $('[autofocus]').focus();
        }

        vm.loginClick = function () {
            vm.showLoginError = false;
            vm.loginMessage = ResourceUtility.getString("LOGIN_LOGGING_IN_MESSAGE");
            
            if (vm.loginInProgress) {
                return;
            }

            vm.usernameInvalid = vm.username === "";
            vm.passwordInvalid = vm.password === "";

            if (vm.usernameInvalid || vm.passwordInvalid) {
                return;
            }

            vm.loginInProgress = true;

            LoginService.Login(vm.username, vm.password).then(function(result) {
                vm.loginInProgress = false;
                vm.loginMessage = null;
                if (result != null && result.Success === true) {
                    // do we need this?
                    //$rootScope.$broadcast("LoggedIn");
                    SessionTimeoutUtility.refreshSessionLastActiveTimeStamp();
                    $state.go('login.tenants');
                } else {
                    // Need to fail gracefully if we get no service response at all
                    vm.showLoginHeaderMessage(result);
                    vm.showLoginError = true;
                }
            });
        };

        vm.showLoginHeaderMessage = function (message) {
            if (message == null) {
                return;
            }

            if (message.indexOf("Global Auth") > -1) {
                message = ResourceUtility.getString("LOGIN_GLOBAL_AUTH_ERROR");
            }

            vm.loginErrorMessage = message;
            vm.showLoginError = true;
        };

        vm.forgotPasswordClick = function ($event) {
            $state.go('login.forgot');
        };

        vm.init();
    }
});