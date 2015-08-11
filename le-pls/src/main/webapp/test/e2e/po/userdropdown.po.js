'use strict';

var UserDropdown = function() {
    this.signout = element(by.linkText('Sign Out'));
    this.ManageUsersLink = element(by.linkText('Manage Users'));
    this.ActivateModelLink = element(by.linkText('Activate Model'));
    this.SystemSetupLink = element(by.linkText('System Setup'));
    this.ModelCreationHistoryLink = element(by.linkText('Model Creation History'));
    this.updatePassword = element(by.linkText('Update Password'));

    this.getUserLink = function(name) {
        return element(by.linkText(name));
    };

    this.toggleDropdown = function() {
        element(by.css('a.nav-personal')).click();
        browser.waitForAngular();
    };
};

module.exports = new UserDropdown();