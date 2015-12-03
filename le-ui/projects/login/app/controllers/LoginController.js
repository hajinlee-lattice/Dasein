angular.module('mainApp.login.controllers.LoginController', [
    'ngRoute',
    'mainApp.appCommon.directives.ngEnterDirective',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.core.utilities.ServiceErrorUtility',
    'mainApp.appCommon.utilities.EvergageUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.login.services.LoginService',
    'mainApp.core.services.HelpService',
    'mainApp.core.services.FeatureFlagService',
    'mainApp.login.modals.TenantSelectionModal',
    'mainApp.core.services.ResourceStringsService',
    'mainApp.config.services.ConfigService',
    'mainApp.core.controllers.MainViewController'
])
.controller('LoginController', function ($scope, $http, $rootScope, $compile, ResourceUtility, TimestampIntervalUtility, NavUtility, ServiceErrorUtility, EvergageUtility,
                                         BrowserStorageUtility, HelpService, LoginService, ResourceStringsService, ConfigService, TenantSelectionModal, FeatureFlagService) {

    $("body").addClass("login-body");
    $('[autofocus]').focus();
console.log('LoginController init');
    // Property bindings
    $scope.copyrightString = ResourceUtility.getString('LOGIN_COPYRIGHT', [(new Date()).getFullYear()]);
    $scope.ResourceUtility = ResourceUtility;

    $scope.username = "";
    $scope.password = "";
    $scope.loginMessage = null;
    $scope.loginErrorMessage = null;
    $scope.showLoginError = false;
    $scope.showSuccessMessage = false;
    $scope.successMessage = "";
    $scope.loginInProgess = false;
    $scope.showLoginForm = true;
    $scope.showForgotPassword = false;
    $scope.forgotPasswordUsername = "";
    $scope.forgotPasswordErrorMessage = "";

    // Initialize Evergage as an anonymous user
    EvergageUtility.Initialize({
        userID: null,
        title: null,
        datasetPrefix: "pls",
        company: null
    });

    // Controller methods
    $scope.loginClick = function () {
        $scope.showLoginError = false;
        $scope.loginMessage = ResourceUtility.getString("LOGIN_LOGGING_IN_MESSAGE");
        if ($scope.loginInProgess) {
            return;
        }

        $scope.usernameInvalid = $scope.username === "";
        $scope.passwordInvalid = $scope.password === "";
        if ($scope.usernameInvalid || $scope.passwordInvalid) {
            return;
        }

        $scope.loginInProgess = true;
        LoginService.Login($scope.username, $scope.password).then(function(result) {
            $scope.loginInProgess = false;
            $scope.loginMessage = null;
            if (result != null && result.Success === true) {
                $rootScope.$broadcast("LoggedIn");

                $scope.isLoggedInWithTempPassword = result.Result.MustChangePassword;
                $scope.isPasswordOlderThanNinetyDays = TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(result.Result.PasswordLastModified);
                $scope.handleTenantSelection(result.Result.Tenants);
            } else {
                // Need to fail gracefully if we get no service response at all
                $scope.showLoginHeaderMessage(ResourceUtility.getString("LOGIN_UNKNOWN_ERROR"));
            }
        });
    };

    $scope.handleTenantSelection = function (tenantList) {
        if (tenantList == null || tenantList.length === 0) {
            $scope.showLoginHeaderMessage(ResourceUtility.getString("NO_TENANT_MESSAGE"));
            return;
        }

        if (tenantList.length == 1) {
            $scope.getSessionDocument(tenantList[0]);
        } else {
            var tenantSelectionCallback = function (selectedTenant) {
                $scope.getSessionDocument(selectedTenant);
            };
            TenantSelectionModal.show(tenantList, tenantSelectionCallback);
        }
    };

    $scope.getSessionDocument = function (tenant) {
        LoginService.GetSessionDocument(tenant).then(function(data) {
            if (data != null && data.Success === true) {
                //Initialize Evergage
                EvergageUtility.Initialize({
                    userID: data.Result.User.Identifier,
                    title: data.Result.User.Title,
                    datasetPrefix: "pls",
                    company: data.Ticket.Tenants[0].DisplayName
                });

                $scope.getLocaleSpecificResourceStrings(data.Result.User.Locale);
            } else {
                $scope.showLoginHeaderMessage(ResourceUtility.getString("LOGIN_UNKNOWN_ERROR"));
            }
        });
    };

    $scope.getLocaleSpecificResourceStrings = function (locale) {
        ResourceStringsService.GetInternalResourceStringsForLocale(locale).then(function(result) {
            $scope.getWidgetConfigDoc();
        });
    };

    $scope.getWidgetConfigDoc = function () {
        window.open("/pd/#/jobs/status", "_self");
        ConfigService.GetWidgetConfigDocument().then(function() {
            $("body").removeClass("login-body");
            $rootScope.$broadcast("ShowFooterEvent", true);
            constructMainView(); return;
            getFeatureFlags();
        });
    };

    function getFeatureFlags() {
            console.log('GOT FEATURE FLAGS MOFO')
            constructMainView();
        //FeatureFlagService.GetAllFlags().then(function() {
        //});
    }

    function constructMainView() {
        console.log('loginController constructMainView');
        $http.get('./app/views/MainView.html').success(function (html) {
            var scope = $rootScope.$new();
            scope.isLoggedInWithTempPassword = $scope.isLoggedInWithTempPassword;
            scope.isPasswordOlderThanNinetyDays = $scope.isPasswordOlderThanNinetyDays;
            $compile($("#mainView").html(html))(scope);
        });
    }

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
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.showLoginForm = false;
        $scope.showForgotPassword = true;
    };

    $scope.cancelForgotPasswordClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        $scope.showLoginForm = true;
        $scope.showForgotPasswordError = false;
    };

    $scope.forgotPasswordOkClick = function () {
        $scope.resetPasswordSuccess = false;
        $scope.showForgotPasswordError = false;
        $scope.forgotPasswordUsernameInvalid = $scope.forgotPasswordUsername === "";
        if ($scope.forgotPasswordUsernameInvalid) {
            return;
        }
        LoginService.ResetPassword($scope.forgotPasswordUsername).then(function(result) {
            if (result == null) {
                return;
            }
            if (result.Success === true) {
                $scope.showForgotPassword = false;
                $scope.resetPasswordSuccess = true;
            } else {
                $scope.showForgotPasswordError = true;

                if (result.Error.errorCode == 'LEDP_18018') {
                    $scope.forgotPasswordUsernameInvalid = true;
                    $scope.forgotPasswordErrorMessage = ResourceUtility.getString('RESET_PASSWORD_USERNAME_INVALID');
                } else {
                    $scope.forgotPasswordUsernameInvalid = false;
                    $scope.forgotPasswordErrorMessage = ResourceUtility.getString('RESET_PASSWORD_FAIL');
                }
            }
        });
    };

    $scope.privacyPolicyClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        HelpService.OpenPrivacyPolicy();
    };
});