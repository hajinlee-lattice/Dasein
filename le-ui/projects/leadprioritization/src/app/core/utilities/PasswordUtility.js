angular.module('mainApp.core.utilities.PasswordUtility', ['mainApp.appCommon.utilities.ResourceUtility',])
.service('PasswordUtility', function (ResourceUtility) {

    this.validPassword = function(password) {
        var result = {
            Valid: true,
            ErrorMsg: null
        };

        if (password.length < 8) {
            result.Valid = false;
            result.ErrorMsg = ResourceUtility.getString("CHANGE_PASSWORD_HELP");
            return result;
        }

        var uppercase = /[A-Z]/;
        if (!uppercase.test(password)) {
            result.Valid = false;
            result.ErrorMsg = ResourceUtility.getString("CHANGE_PASSWORD_HELP");
            return result;
        }

        var lowercase = /[a-z]/;
        if (!lowercase.test(password)) {
            result.Valid = false;
            result.ErrorMsg = ResourceUtility.getString("CHANGE_PASSWORD_HELP");
            return result;
        }

        var number = /[0-9]/;
        if (!number.test(password)) {
            result.Valid = false;
            result.ErrorMsg = ResourceUtility.getString("CHANGE_PASSWORD_HELP");
            return result;
        }

        return result;
    };

});
