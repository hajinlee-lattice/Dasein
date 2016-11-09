angular.module('login.forgot', [
    'mainApp.appCommon.directives.ngEnterDirective',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.login.services.LoginService'
])
.component('loginForgotPassword', {
    templateUrl: 'app/login/forgot/forgot.component.html',
    controller: function($state, ResourceUtility, LoginService) {
        var vm = this;

        vm.ResourceUtility = ResourceUtility;

        vm.forgotPasswordErrorMessage = "";

        vm.cancelForgotPasswordClick = function ($event) {
            if ($event != null) {
                $event.preventDefault();
            }
            
            $state.go('login.form');
        };

        validateEmail = function(string) {
            if (!string) {
                return false;
            }
            var at = string.indexOf('@'),
                dot = string.lastIndexOf('.');
            if (at < 1 || dot - at < 2){
                return false;
            }
            else {
                return true;
            }
        }

        vm.forgotPasswordOkClick = function (forgotPasswordUsername) {
            var forgotPasswordUsername = forgotPasswordUsername || vm.forgotPasswordUsername;

            vm.resetPasswordSuccess = false;
            vm.showForgotPasswordError = false;
            vm.forgotPasswordUsernameInvalid = !validateEmail(forgotPasswordUsername);

            if (vm.forgotPasswordUsernameInvalid) {
                return;
            }

            LoginService.ResetPassword(forgotPasswordUsername).then(function(result) {
                if (result == null) {
                    return;
                }
                if (result.Success === true) {
                    vm.resetPasswordSuccess = true;
                } else {
                    vm.showForgotPasswordError = true;

                    if (result.Error.errorCode == 'LEDP_18018') {
                        vm.forgotPasswordUsernameInvalid = true;
                        vm.forgotPasswordErrorMessage = ResourceUtility.getString('RESET_PASSWORD_USERNAME_INVALID');
                    } else {
                        vm.forgotPasswordUsernameInvalid = false;
                        vm.forgotPasswordErrorMessage = ResourceUtility.getString('RESET_PASSWORD_FAIL');
                    }
                }
            });
        };
    }
});