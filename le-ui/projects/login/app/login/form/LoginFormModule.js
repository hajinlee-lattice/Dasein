angular.module('login.form', [
    'mainApp.appCommon.directives.ngEnterDirective',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.login.services.LoginService',
    'mainApp.core.services.ResourceStringsService'
])
.service('LoginStore', function(TimestampIntervalUtility, BrowserStorageUtility) {
    this.login = {
        username: '',
        expires: null,
        expireDays: 0,
        tenant: ''
    };

    this.set = function(LoginDocument, ClientSession) {
        this.login.username = LoginDocument.UserName;
        this.login.expires = LoginDocument.PasswordLastModified;
        this.login.expireDays = Math.abs(TimestampIntervalUtility.getDays(LoginDocument.PasswordLastModified) - 90);

        if (ClientSession && ClientSession.Tenant) {
            this.login.tenant = ClientSession.Tenant.DisplayName;
        }
    }

    this.redirectToLP = function (Tenant) {
        if (!Tenant) {
            var ClientSession = BrowserStorageUtility.getClientSession();
            var Tenant = ClientSession.Tenant;
        }

        var UIVersion = Tenant.UIVersion || "2.0";
        var pathMap = {
            "3.0": "/lp/",
            "2.0": "/lp2/"
        };

        var previousSession = BrowserStorageUtility.getClientSession();

        return window.open(pathMap[UIVersion] || "/lp2", "_self");
    }
})
.controller('LoginViewController', function (
    $templateCache, $http, $rootScope, $compile, $state, ResourceUtility, TimestampIntervalUtility,
    BrowserStorageUtility, LoginService, ResourceStringsService, LoginStore
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
        copyrightString: ResourceUtility.getString('LOGIN_COPYRIGHT', [(new Date()).getFullYear()]),
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
                $rootScope.$broadcast("LoggedIn");

                $state.go('login.tenants');
            } else {
                // Need to fail gracefully if we get no service response at all
                vm.showLoginHeaderMessage(result);
                vm.showLoginError = true;
            }
        });
    };

    vm.showLoginHeaderMessage = function (message) {

        console.log(message);


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
});