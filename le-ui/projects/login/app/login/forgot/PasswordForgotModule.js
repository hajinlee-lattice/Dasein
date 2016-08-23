angular.module('login.forgot', [
    'mainApp.appCommon.directives.ngEnterDirective',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.login.services.LoginService'
])
.controller('PasswordForgotController', function ($scope, $state, ResourceUtility, LoginService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.forgotPasswordUsername = "";
    $scope.forgotPasswordErrorMessage = "";

    $scope.cancelForgotPasswordClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        $state.go('login.form');
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
});