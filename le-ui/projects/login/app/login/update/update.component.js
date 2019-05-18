angular.module('login.update', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'common.utilities.browserstorage',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.utilities.PasswordUtility',
    'mainApp.login.services.LoginService'
])
.component('loginUpdatePassword', {
    templateUrl: 'app/login/update/update.component.html',
    bindings: {
        logindocument: '<'
    },
    controller: function (
        $state, ResourceUtility, BrowserStorageUtility, PasswordUtility, 
        StringUtility, LoginService, TimestampIntervalUtility, Banner
    ) {
        var vm = this;

        vm.$onInit = function() {
            angular.element('body').addClass('update-password-body');
            
            vm.ResourceUtility = ResourceUtility;
            vm.isLoggedInWithTempPassword = vm.logindocument.MustChangePassword;
            vm.isPasswordOlderThanNinetyDays = TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(vm.logindocument.PasswordLastModified);

            var authenticationRoute = vm.logindocument.AuthenticationRoute || null,
                clientSession = BrowserStorageUtility.getClientSession() || {},
                accessLevel = clientSession.AccessLevel || null;

            vm.mayChangePassword = authenticationRoute !== 'SSO';

            vm.oldPassword = null;
            vm.newPassword = null;
            vm.confirmPassword = null;

            vm.oldPasswordInputError = "";
            vm.newPasswordInputError = "";
            vm.confirmPasswordInputError = "";
            vm.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_HELP");
            vm.saveInProgess = false;
            
            Banner.reset();

            $("#validateAlertError, #changePasswordSuccessAlert").hide();

            if (vm.isPasswordOlderThanNinetyDays) {
                vm.validateErrorMessage = ResourceUtility.getString("NINTY_DAY_OLD_PASSWORD");

                Banner.error({
                    message: vm.validateErrorMessage
                });
            } else if (vm.isLoggedInWithTempPassword) {
                vm.validateErrorMessage = ResourceUtility.getString("MUST_CHANGE_TEMP_PASSWORD");

                Banner.error({
                    message: vm.validateErrorMessage
                });
            }
        };

        function validatePassword() {
            $("#validateAlertError, #changePasswordSuccessAlert").fadeOut();
            vm.oldPasswordInputError = StringUtility.IsEmptyString(vm.oldPassword) ? "error" : "";
            vm.newPasswordInputError = StringUtility.IsEmptyString(vm.newPassword) ? "error" : "";
            vm.confirmPasswordInputError = StringUtility.IsEmptyString(vm.confirmPassword) ? "error" : "";

            if (vm.oldPassword === "") {
                vm.validateErrorMessage = ResourceUtility.getString("LOGIN_PASSWORD_EMPTY_OLDPASSWORD");
                vm.oldPasswordInputError = "error";
                return false;
            }

            if (vm.oldPasswordInputError === "error" || vm.newPasswordInputError === "error" ||
            vm.confirmPasswordInputError === "error") {
                return false;
            }
            
            if (vm.newPassword == vm.oldPassword) {
                vm.validateErrorMessage = ResourceUtility.getString("LOGIN_PASSWORD_UPDATE_ERROR");
                vm.newPasswordInputError = "error";
                vm.confirmPasswordInputError = "error";
                return false;
            }

            if (vm.newPassword !== vm.confirmPassword) {
                vm.validateErrorMessage = ResourceUtility.getString("LOGIN_PASSWORD_MATCH_ERROR");
                vm.newPasswordInputError = "error";
                vm.confirmPasswordInputError = "error";
                return false;
            }

            if (!PasswordUtility.validPassword(vm.newPassword).Valid) {
                vm.newPasswordInputError = "error";
                vm.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_HELP");
                return false;
            }
            
            return true;
        }
        
        function clearChangePasswordField() {
            vm.oldPassword = "";
            vm.newPassword = "";
            vm.confirmPassword = "";
        }
        
        vm.cancelAndLogoutClick = function ($event) {
            clearChangePasswordField();
            LoginService.Logout();
        };

        vm.closeErrorClick = function ($event) {
            if ($event != null) {
                $event.preventDefault();
            }
            
            Banner.reset();
        };
        
        vm.updatePasswordClick = function () {
            if (vm.saveInProgess) {
                return;
            }

            Banner.reset();

            var isValid = validatePassword();

            if (isValid) {
                vm.saveInProgess = true;

                LoginService.ChangePassword(vm.oldPassword, vm.newPassword, vm.confirmPassword).then(function(result) {
                    vm.saveInProgess = false;

                    if (result.Success) {
                        //$("#changePasswordSuccessAlert").fadeIn();
                        BrowserStorageUtility.clear(false);
                        $state.go('login.success');
                        //$rootScope.$broadcast(NavUtility.UPDATE_PASSWORD_NAV_EVENT, {Success: true});
                    } else {
                        if (result.Status == 401) {
                            vm.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_ERROR");
                        } else {
                            vm.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_BAD_CREDS");
                        }

                        Banner.error({
                            message: vm.validateErrorMessage
                        });
                    }
                });
            } else {
                Banner.error({
                    message: vm.validateErrorMessage
                });
            }
        };
    }
})
.component('loginUpdatePasswordSuccess', {
    templateUrl: 'app/login/update/success.component.html',
    controller: function($scope, ResourceUtility, LoginService) {
        var vm = this;

        vm.ResourceUtility = ResourceUtility;
        
        vm.clickRelogin = function(){
            LoginService.Logout();
        };
    }
});