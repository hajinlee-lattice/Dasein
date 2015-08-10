'use strict';

var UserDropdown = function() {
    this.signout = element(by.linkText('Sign Out'));
    this.ManageUsersLink = element(by.linkText('Manage Users'));
    this.ActivateModelLink = element(by.linkText('Activate Model'));
    this.SystemSetupLink = element(by.linkText('System Setup'));
    this.updatePassword = element(by.linkText('Update Password'));

    this.getUserLink = function(name) {
        return element(by.linkText(name));
    };

    this.toggleDropdown = function() {
        element(by.css('a.nav-personal')).click();
    };
};

module.exports = new UserDropdown();