'use strict';

var UserDropdown = function() {
    this.signout = element(by.linkText('Sign Out'));
    this.updatePassword = element(by.linkText('Update Password'));

    this.getUserLink = function(name) {
        return element(by.linkText(name));
    };

    this.toggleDropdown = function() {
        element(by.css('.profile-nav > a')).click();
        browser.waitForAngular();
    };
};

module.exports = new UserDropdown();