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

        return window.open(pathMap[UIVersion] || "/lp2", "_self");
    }
})
.controller('LoginViewController', function (
    $scope, $templateCache, $http, $rootScope, $compile, $state, ResourceUtility, TimestampIntervalUtility,
    BrowserStorageUtility, LoginService, ResourceStringsService, LoginStore
) {
    $('[autofocus]').focus();

    $scope.ResourceUtility = ResourceUtility;

    $scope.username = "";
    $scope.password = "";
    $scope.loginMessage = null;
    $scope.loginErrorMessage = null;
    $scope.showLoginError = false;
    $scope.showSuccessMessage = false;
    $scope.successMessage = "";
    $scope.loginInProgress = false;
    $scope.showLoginForm = true;
    $scope.showForgotPassword = false;
    $scope.forgotPasswordUsername = "";
    $scope.forgotPasswordErrorMessage = "";

    $scope.loginClick = function () {
        $scope.showLoginError = false;
        $scope.loginMessage = ResourceUtility.getString("LOGIN_LOGGING_IN_MESSAGE");
        
        if ($scope.loginInProgress) {
            return;
        }

        $scope.usernameInvalid = $scope.username === "";
        $scope.passwordInvalid = $scope.password === "";

        if ($scope.usernameInvalid || $scope.passwordInvalid) {
            return;
        }

        $scope.loginInProgress = true;

        LoginService.Login($scope.username, $scope.password).then(function(result) {
            $scope.loginInProgress = false;
            $scope.loginMessage = null;
            if (result != null && result.Success === true) {
                $rootScope.$broadcast("LoggedIn");

                $state.go('login.tenants')
                //$scope.handleTenantSelection(result.Result.Tenants);
            } else {
                // Need to fail gracefully if we get no service response at all
                $scope.showLoginHeaderMessage(result.errorMessage);
            }
        });
    };

    $scope.showLoginHeaderMessage = function (message) {
        if (message == null) {
            return;
        }

        if (message.indexOf("Global Auth") > -1) {
            message = ResourceUtility.getString("LOGIN_GLOBAL_AUTH_ERROR");
        }

        $scope.loginErrorMessage = message;
        $scope.showLoginError = true;
    };

    $scope.forgotPasswordClick = function ($event) {
        $state.go('login.forgot');
    };
});