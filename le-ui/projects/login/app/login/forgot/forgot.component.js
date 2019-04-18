angular.module('login.forgot', [
    'mainApp.appCommon.directives.ngEnterDirective',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.login.services.LoginService'
])
.component('loginForgotPassword', {
    templateUrl: 'app/login/forgot/forgot.component.html',
    controller: function($state, ResourceUtility, LoginService, Banner) {
        var vm = this;

        vm.$onInit = function() {
            vm.ResourceUtility = ResourceUtility;
        };

        vm.cancelForgotPasswordClick = function ($event) {
            if ($event != null) {
                $event.preventDefault();
            }
            
            $state.go('login.form');
        };

        vm.forgotPasswordOkClick = function (forgotPasswordUsername) {
            Banner.reset();

            forgotPasswordUsername = forgotPasswordUsername || vm.forgotPasswordUsername;

            vm.resetPasswordSuccess = false;
            vm.forgotPasswordUsernameInvalid = !validateEmail(forgotPasswordUsername);

            if (vm.forgotPasswordUsernameInvalid) {
                Banner.error({
                    message: ResourceUtility.getString('RESET_PASSWORD_USERNAME_INVALID')
                });
            
                return;
            }

            LoginService.ResetPassword(forgotPasswordUsername).then(function(result) {
                if (result == null) {
                    return;
                }
                
                if (result.Success === true) {
                    vm.resetPasswordSuccess = true;
                } else {
                    var message = '';

                    if (result.Error.errorCode == 'LEDP_18018') {
                        vm.forgotPasswordUsernameInvalid = true;
                        message = ResourceUtility.getString('RESET_PASSWORD_USERNAME_INVALID');
                    } else {
                        vm.forgotPasswordUsernameInvalid = false;
                        message = ResourceUtility.getString('RESET_PASSWORD_FAIL');
                    }
            
                    Banner.error({
                        message: message
                    });
                }
            });
        };

        var validateEmail = function(string) {
            if (!string) {
                return false;
            }

            var at = string.indexOf('@'),
                dot = string.lastIndexOf('.');

            if (at < 1 || dot - at < 2){
                return false;
            } else {
                return true;
            }
        };
    }
});