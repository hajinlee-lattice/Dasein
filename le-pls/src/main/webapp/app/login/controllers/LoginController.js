angular.module('mainApp.login.controllers.LoginController', [
    'ngRoute',
    'mainApp.appCommon.directives.ngEnterDirective',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.ServiceErrorUtility',
    'mainApp.appCommon.utilities.EvergageUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.login.services.LoginService',
    'mainApp.core.services.HelpService',
    'mainApp.login.modals.TenantSelectionModal',
    'mainApp.core.services.ResourceStringsService',
    'mainApp.config.services.GriotConfigService',
    'mainApp.core.controllers.MainViewController'
])
.controller('LoginController', function ($scope, $http, $rootScope, $compile, ResourceUtility, GriotNavUtility, ServiceErrorUtility, EvergageUtility,
    BrowserStorageUtility, HelpService, LoginService, ResourceStringsService, GriotConfigService, TenantSelectionModal) {
    
    $("body").addClass("login-body");
    $('[autofocus]').focus();
    
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
    
    // Controller methods
    $scope.loginClick = function () {
        $scope.showLoginError = false;
        $scope.loginMessage = ResourceUtility.getString("LOGIN_LOGGING_IN_MESSAGE");
        if ($scope.loginInProgess) {
            return;
        }
        
        $scope.usernameInvalid = $scope.username === "" ? true : false;
        $scope.passwordInvalid = $scope.password === "" ? true : false;
        if ($scope.usernameInvalid || $scope.passwordInvalid) {
            return;
        }
        
        $scope.loginInProgess = true;
        LoginService.Login($scope.username, $scope.password).then(function(result) {
            $scope.loginInProgess = false;
            $scope.loginMessage = null;
            if (result != null && result.Success === true) {
                $scope.directToPassword = result.Result.MustChangePassword;
                $scope.handleTenantSelection(result.Result.Tenants); 
            } else {
                // Need to fail gracefully if we get no service response at all
                if (result == null) {
                    $scope.showLoginHeaderMessage(ResourceUtility.getString("LOGIN_UNKNOWN_ERROR"));
                } else {
                    $scope.showLoginHeaderMessage(result.errorMessage);
                }
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
                    datasetPrefix: "pls"
                });
                
                $scope.getLocaleSpecificResourceStrings(data.Result.User.Locale);
            } else {
                $scope.showLoginHeaderMessage(ResourceUtility.getString("LOGIN_UNKNOWN_ERROR"));
            }
        });
    };
    
    $scope.getLocaleSpecificResourceStrings = function (locale) {
        ResourceStringsService.GetResourceStrings(locale).then(function(result) {
            $scope.getWidgetConfigDoc();
        });
    };
    
    //TODO:pierce Add this back when we can configure credentials in PLS
    /*$scope.getConfigDoc = function () {
        GriotConfigService.GetConfigDocument().then(function(result) {
            $scope.getWidgetConfigDoc();
        });
    };*/
    
    $scope.getWidgetConfigDoc = function () {
        GriotConfigService.GetWidgetConfigDocument().then(function(result) {
            $("body").removeClass("login-body");
            $http.get('./app/core/views/MainView.html').success(function (html) {
                var scope = $rootScope.$new();
                scope.directToPassword = $scope.directToPassword || false;
                $compile($("#mainView").html(html))(scope);
            });
        });
    };
    
    $scope.showLoginHeaderMessage = function (message) {
        if (message == null) {
            return;
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
    };
    
    $scope.forgotPasswordOkClick = function () {
        $scope.resetPasswordSuccess = false;
        $scope.showForgotPasswordError = false;
        $scope.forgotPasswordUsernameInvalid = $scope.forgotPasswordUsername === "" ? true : false;
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
                //TODO:pierce need to handle errors from forgot password
                $scope.showForgotPasswordError = true;
                $scope.forgotPasswordUsernameInvalid = false;
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