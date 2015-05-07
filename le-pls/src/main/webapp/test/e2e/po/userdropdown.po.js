'use strict';

var UserDropdown = function() {
    this.signout = element(by.linkText('Sign Out'));
    this.ManageUsersLink = element(by.linkText('Manage Users'));
    this.MultipleModelsLink = element(by.linkText('Multiple Model Setup'));
    this.SystemSetupLink = element(by.linkText('System Setup'));

    this.getUserLink = function(name) {
        return element(by.linkText(name));
    };

    this.toggleDropdown = function() {
        element(by.css('a.nav-personal')).click();
    };
};

module.exports = new UserDropdown();