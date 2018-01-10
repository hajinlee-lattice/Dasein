angular.module('login.update', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.utilities.PasswordUtility',
    'mainApp.login.services.LoginService'
])
.component('loginUpdatePassword', {
    templateUrl: 'app/login/update/update.component.html',
    controller: function (
        $scope, $state, ResourceUtility, BrowserStorageUtility, PasswordUtility, 
        StringUtility, LoginService, TimestampIntervalUtility
    ) {
        var vm = this,
            resolve = $scope.$parent.$resolve,
            LoginDocument = resolve.LoginDocument;

        angular.element('body').addClass('update-password-body');
        
        vm.ResourceUtility = ResourceUtility;
        vm.isLoggedInWithTempPassword = LoginDocument.MustChangePassword;
        vm.isPasswordOlderThanNinetyDays = TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(LoginDocument.PasswordLastModified);

        var loginDocument = BrowserStorageUtility.getLoginDocument(),
            authenticationRoute = loginDocument.AuthenticationRoute || null,
            clientSession = BrowserStorageUtility.getClientSession() || {},
            accessLevel = clientSession.AccessLevel || null;

        vm.mayChangePassword = (authenticationRoute !== 'SSO' || (authenticationRoute === 'SSO'  && accessLevel === 'SUPER_ADMIN'));

        vm.oldPassword = null;
        vm.newPassword = null;
        vm.confirmPassword = null;

        vm.oldPasswordInputError = "";
        vm.newPasswordInputError = "";
        vm.confirmPasswordInputError = "";
        vm.showPasswordError = false;
        vm.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_HELP");

        vm.saveInProgess = false;

        $("#validateAlertError, #changePasswordSuccessAlert").hide();

        if (vm.isPasswordOlderThanNinetyDays) {
            vm.showPasswordError = true;
            vm.validateErrorMessage = ResourceUtility.getString("NINTY_DAY_OLD_PASSWORD");
        } else if (vm.isLoggedInWithTempPassword) {
            vm.showPasswordError = true;
            vm.validateErrorMessage = ResourceUtility.getString("MUST_CHANGE_TEMP_PASSWORD");
        }

        function validatePassword () {
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
        
        vm.cancelAndLogoutClick = function ($event) {
            clearChangePasswordField();
            LoginService.Logout();
        };
        
        function clearChangePasswordField() {
            vm.oldPassword = "";
            vm.newPassword = "";
            vm.confirmPassword = "";
        }
        
        vm.closeErrorClick = function ($event) {
            if ($event != null) {
                $event.preventDefault();
            }
            
            vm.showPasswordError = false;
        };
        
        vm.updatePasswordClick = function () {
            if (vm.saveInProgess) {
                return;
            }
            vm.showPasswordError = false;
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
                            vm.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_BAD_CREDS");
                        } else {
                            vm.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_ERROR");
                        }
                        vm.showPasswordError = true;
                    }
                });
            } else {
                vm.showPasswordError = true;
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