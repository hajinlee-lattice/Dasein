'use strict'

var PasswordChange = function() {

    var loginPage = require('./login.po');
    var userDropdown = require('./userdropdown.po');

    this.params = {
        invalidUserUsername:    'pls-invalid-user-tester@test.lattice-engines.com',
        passwordTestingUserUsername: 'pls-password-tester@test.lattice-engines.ext',
        passwordTestingPassword: 'Lattice123',
        passwordTestingAlternativePassword: 'Engines123',
        passwordTestingUnsafePassword: 'unsafe'
    };

    this.assertPasswordResetEmailSent = function(expected) {
        if (expected) {
            expect(element(by.css('div.form-signin')).isPresent()).toBe(true);
        } else {
            expect(element(by.css('div.global-error')).isPresent()).toBe(true);
        }
    };

    this.assertPasswordChangeSuccessful = function(expected) {
        if (expected) {
            expect(element(by.cssContainingText('h1', 'successfully changed your password')).isPresent()).toBe(true);
        } else {
            expect(element(by.css('div.global-error')).isPresent()).toBe(true);
        }
    };

    this.changePasswordFromOldToNew = function(oldPassword, newPassword) {
        element(by.model('oldPassword')).sendKeys(oldPassword);
        element(by.model('newPassword')).sendKeys(newPassword);
        element(by.model('confirmPassword')).sendKeys(newPassword);
        element(by.buttonText('Update')).click();
        browser.waitForAngular();
    };

    this.navigateFromHomePageToChangePasswordPage = function() {
        userDropdown.toggleDropdown();
        expect(userDropdown.updatePassword.isDisplayed()).toBe(true);
        userDropdown.updatePassword.click();
        browser.waitForAngular();
    };

    this.loginAsTestingUserWithPasswordAndTenant = function(password, tenant) {
        loginPage.loginUser(this.params.passwordTestingUserUsername, password, tenant);
    };
};

module.exports = new PasswordChange();
