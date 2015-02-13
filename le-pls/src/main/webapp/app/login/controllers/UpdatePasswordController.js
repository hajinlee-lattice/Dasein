angular.module('mainApp.login.controllers.UpdatePasswordController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.login.services.LoginService'
])

.controller('UpdatePasswordController', function ($scope, ResourceUtility, BrowserStorageUtility, StringUtility, LoginService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.oldPassword = null;
    $scope.newPassword = null;
    $scope.confirmPassword = null;
    
    $scope.oldPasswordInputError = "";
    $scope.newPasswordInputError = "";
    $scope.confirmPasswordInputError = "";
    
    $scope.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_HELP");
    
    $scope.saveInProgess = false;
    
    $("#validateAlertError, #changePasswordSuccessAlert").hide();
    
    function validatePassword () {
        $("#validateAlertError, #changePasswordSuccessAlert").fadeOut();
        $scope.oldPasswordInputError = StringUtility.IsEmptyString($scope.oldPassword) ? "error" : "";
        $scope.newPasswordInputError = StringUtility.IsEmptyString($scope.newPassword) ? "error" : "";
        $scope.confirmPasswordInputError = StringUtility.IsEmptyString($scope.confirmPassword) ? "error" : "";
        
        if ($scope.oldPasswordInputError === "error" || $scope.newPasswordInputError === "error" ||
        $scope.confirmPasswordInputError === "error") {
            return false;
        }

        if ($scope.newPassword !== $scope.confirmPassword) {
            $scope.validateErrorMessage = ResourceUtility.getString("LOGIN_PASSWORD_MATCH_ERROR");
            $scope.newPasswordInputError = "error";
            $scope.confirmPasswordInputError = "error";
            return false;
        }
        
        if ($scope.newPassword.length < 8) {
            $scope.newPasswordInputError = "error";
            $scope.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_HELP");
            return false;
        }
        
        var uppercase = /[A-Z]/;
        if (!uppercase.test($scope.newPassword)) {
            $scope.newPasswordInputError = "error";
            $scope.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_HELP");
            return false;
        }
        
        var lowercase = /[a-z]/;
        if (!lowercase.test($scope.newPassword)) {
            $scope.newPasswordInputError = "error";
            $scope.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_HELP");
            return false;
        }
        
        var number = /[0-9]/;
        if (!number.test($scope.newPassword)) {
            $scope.newPasswordInputError = "error";
            $scope.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_HELP");
            return false;
        }
        
        return true;
    }
    
    $scope.updatePasswordClick = function () {
        if ($scope.saveInProgess) {
            return;
        }
        
        var isValid = validatePassword();
        if (isValid) {
            $scope.saveInProgess = true;
            LoginService.ChangePassword($scope.oldPassword, $scope.newPassword, $scope.confirmPassword).then(function(result) {
                $scope.saveInProgess = false;
                if (result.success) {
                    $("#changePasswordSuccessAlert").fadeIn();
                } else {
                    if (result.status == 401) {
                        $scope.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_BAD_CREDS");
                    } else {
                        $scope.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_ERROR");
                    }
                    $("#validateAlertError").fadeIn();
                }
            });
        } else {
            $("#validateAlertError").fadeIn();
        }
    };
});