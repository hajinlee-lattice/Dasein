'use strict';

var UserDropdown = function() {
    this.signout = element(by.linkText('Sign Out'));

    this.toggleDropdown = function() {
        element(by.css('a.user-dropdown')).click();
    };
};

module.exports = new UserDropdown();