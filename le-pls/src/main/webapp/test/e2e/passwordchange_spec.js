'use strict'

describe('forgot password tests:', function() {

    var loginPage = require('./po/login.po');
    var passwordChange = require('./po/passwordchange.po');

    it('forgot wrong user password should generate error', function() {
        changePasswordWithWrongUsername_assertError();
    });

    it('change password to empty string should generate error', function() {
        changePasswordToEmptyString_assertError();
    });

    it('change password to current password should generate error', function() {
        changePasswordToCurrentPassword_assertError();
    });

    it('change to password that fails security stands should generate error', function() {
        changePasswordToUnsafePassword_assertError();
    });

    it('change to password that does not match confirm password should generate error', function() {
        changePasswordButNewPasswordAndConfirmPasswordAreDifferent_assertError();
    });

    it('should pass happy case', function() {
        // changePassword_assertItWorked();
    });

    function changePasswordWithWrongUsername_assertError() {
        loginPage.get();
        browser.waitForAngular();
        element(by.linkText('Forgot Password?')).click();
        browser.waitForAngular();
        element(by.model('forgotPasswordUsername')).sendKeys(passwordChange.params.invalidUserUsername);
        element(by.buttonText('Send Email')).click();
        passwordChange.assertPasswordResetEmailSent(false);
    }

    function changePasswordToEmptyString_assertError() {
        passwordChange.loginAsTestingUserWithPasswordAndTenant(passwordChange.params.passwordTestingPassword);
        passwordChange.navigateFromHomePageToChangePasswordPage();
        passwordChange.changePasswordFromOldToNew(passwordChange.params.passwordTestingPassword, "");
        passwordChange.assertPasswordChangeSuccessful(false);
    }

    function changePasswordToCurrentPassword_assertError() {
        passwordChange.loginAsTestingUserWithPasswordAndTenant(passwordChange.params.passwordTestingPassword);
        passwordChange.navigateFromHomePageToChangePasswordPage();
        passwordChange.changePasswordFromOldToNew(passwordChange.params.passwordTestingPassword, passwordChange.params.passwordTestingPassword);
        passwordChange.assertPasswordChangeSuccessful(false);
    }

    function changePasswordToUnsafePassword_assertError() {
        passwordChange.loginAsTestingUserWithPasswordAndTenant(passwordChange.params.passwordTestingPassword);
        passwordChange.navigateFromHomePageToChangePasswordPage();
        passwordChange.changePasswordFromOldToNew(passwordChange.params.passwordTestingPassword, passwordChange.params.passwordTestingUnsafePassword);
        passwordChange.assertPasswordChangeSuccessful(false);
    }

    function changePasswordButNewPasswordAndConfirmPasswordAreDifferent_assertError() {
        passwordChange.loginAsTestingUserWithPasswordAndTenant(passwordChange.params.passwordTestingPassword);
        passwordChange.navigateFromHomePageToChangePasswordPage();
        element(by.model('oldPassword')).sendKeys(passwordChange.params.passwordTestingPassword);
        element(by.model('newPassword')).sendKeys(passwordChange.params.passwordTestingAlternativePassword);
        element(by.model('confirmPassword')).sendKeys(passwordChange.params.passwordTestingPassword);
        element(by.buttonText('Update')).click();
        browser.waitForAngular();
        passwordChange.assertPasswordChangeSuccessful(false);
    }

    function changePassword_assertItWorked() {
        //=======================================================
        // Login and change password to alternative password
        //=======================================================
        passwordChange.loginAsTestingUserWithPasswordAndTenant(passwordChange.params.passwordTestingPassword);
        passwordChange.navigateFromHomePageToChangePasswordPage();
        passwordChange.changePasswordFromOldToNew(passwordChange.params.passwordTestingPassword, passwordChange.params.passwordTestingAlternativePassword);
        passwordChange.assertPasswordChangeSuccessful(true);
        element(by.buttonText('Return to Login')).click();
        browser.waitForAngular();
        loginPage.assertLoggedIn(false);
        //=======================================================
        // Login with alternative password and assert it worked
        //=======================================================
        passwordChange.loginAsTestingUserWithPasswordAndTenant(passwordChange.params.passwordTestingAlternativePassword);
        loginPage.assertLoggedIn(true);
    }
});
