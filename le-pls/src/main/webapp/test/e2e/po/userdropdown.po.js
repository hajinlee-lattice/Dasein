'use strict';

var UserDropdown = function() {
    this.signout = element(by.linkText('Sign Out'));
    this.ManageUsersLink = element(by.linkText('Manage Users'));

    this.getUserLink = function(name) {
        return element(by.linkText(name));
    };

    this.toggleDropdown = function() {
        element(by.css('a.dropdown-toggle')).click();
    };
};

module.exports = new UserDropdown();