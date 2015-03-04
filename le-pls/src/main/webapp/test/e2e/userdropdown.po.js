'use strict';

var UserDropdown = function() {
    this.signout = element(by.linkText('Sign Out'));

    this.getUserLink = function(name) {
        return element(by.linkText(name));
    };    
};

module.exports = new UserDropdown();