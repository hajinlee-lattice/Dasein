angular.module('login.update', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.core.utilities.PasswordUtility',
    'mainApp.login.services.LoginService'
])

.controller('PasswordUpdateController', function (
    $scope, $state, $rootScope, ResourceUtility, BrowserStorageUtility, PasswordUtility, 
    StringUtility, NavUtility, LoginService, LoginDocument, TimestampIntervalUtility
) {
    angular.element('body').addClass('update-password-body');
    
    $scope.ResourceUtility = ResourceUtility;
    $scope.isLoggedInWithTempPassword = LoginDocument.MustChangePassword;
    $scope.isPasswordOlderThanNinetyDays = TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(LoginDocument.PasswordLastModified);

    $scope.oldPassword = null;
    $scope.newPassword = null;
    $scope.confirmPassword = null;

    $scope.oldPasswordInputError = "";
    $scope.newPasswordInputError = "";
    $scope.confirmPasswordInputError = "";
    $scope.showPasswordError = false;
    $scope.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_HELP");

    $scope.saveInProgess = false;

    $("#validateAlertError, #changePasswordSuccessAlert").hide();

    if ($scope.isPasswordOlderThanNinetyDays) {
        $scope.showPasswordError = true;
        $scope.validateErrorMessage = ResourceUtility.getString("NINTY_DAY_OLD_PASSWORD");
    } else if ($scope.isLoggedInWithTempPassword) {
        $scope.showPasswordError = true;
        $scope.validateErrorMessage = ResourceUtility.getString("MUST_CHANGE_TEMP_PASSWORD");
    }

    function validatePassword () {
        $("#validateAlertError, #changePasswordSuccessAlert").fadeOut();
        $scope.oldPasswordInputError = StringUtility.IsEmptyString($scope.oldPassword) ? "error" : "";
        $scope.newPasswordInputError = StringUtility.IsEmptyString($scope.newPassword) ? "error" : "";
        $scope.confirmPasswordInputError = StringUtility.IsEmptyString($scope.confirmPassword) ? "error" : "";

        if ($scope.oldPassword === "") {
            $scope.validateErrorMessage = ResourceUtility.getString("LOGIN_PASSWORD_EMPTY_OLDPASSWORD");
            $scope.oldPasswordInputError = "error";
            return false;
        }

        if ($scope.oldPasswordInputError === "error" || $scope.newPasswordInputError === "error" ||
        $scope.confirmPasswordInputError === "error") {
            return false;
        }
        
        if ($scope.newPassword == $scope.oldPassword) {
            $scope.validateErrorMessage = ResourceUtility.getString("LOGIN_PASSWORD_UPDATE_ERROR");
            $scope.newPasswordInputError = "error";
            $scope.confirmPasswordInputError = "error";
            return false;
        }

        if ($scope.newPassword !== $scope.confirmPassword) {
            $scope.validateErrorMessage = ResourceUtility.getString("LOGIN_PASSWORD_MATCH_ERROR");
            $scope.newPasswordInputError = "error";
            $scope.confirmPasswordInputError = "error";
            return false;
        }

        if (!PasswordUtility.validPassword($scope.newPassword).Valid) {
            $scope.newPasswordInputError = "error";
            $scope.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_HELP");
            return false;
        }
        
        return true;
    }
    
    $scope.cancelAndLogoutClick = function ($event) {
        clearChangePasswordField();
        LoginService.Logout();
    };
    
    function clearChangePasswordField() {
        $scope.oldPassword = "";
        $scope.newPassword = "";
        $scope.confirmPassword = "";
    }
    
    $scope.closeErrorClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        $scope.showPasswordError = false;
    };
    
    $scope.updatePasswordClick = function () {
        if ($scope.saveInProgess) {
            return;
        }
        $scope.showPasswordError = false;
        var isValid = validatePassword();
        if (isValid) {
            $scope.saveInProgess = true;
            LoginService.ChangePassword($scope.oldPassword, $scope.newPassword, $scope.confirmPassword).then(function(result) {
                $scope.saveInProgess = false;
                console.log('updatePasswordClick',result);
                if (result.Success) {
                    //$("#changePasswordSuccessAlert").fadeIn();
                    BrowserStorageUtility.clear(false);
                    $state.go('login.success');
                    //$rootScope.$broadcast(NavUtility.UPDATE_PASSWORD_NAV_EVENT, {Success: true});
                } else {
                    if (result.Status == 401) {
                        $scope.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_BAD_CREDS");
                    } else {
                        $scope.validateErrorMessage = ResourceUtility.getString("CHANGE_PASSWORD_ERROR");
                    }
                    $scope.showPasswordError = true;
                }
            });
        } else {
            $scope.showPasswordError = true;
        }
    };
})
.controller('PasswordUpdateSuccessController', function ($scope, ResourceUtility, LoginService) {

    $scope.ResourceUtility = ResourceUtility;
    $scope.clickRelogin = function(){
        LoginService.Logout();
    };

});