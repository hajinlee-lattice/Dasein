'use strict';

var UserManagement = function() {
    var userDropdown = require('./userdropdown.po');

    this.randomName = function(n) {
        var text = "";
        var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        for( var i=0; i < n; i++ )
            text += possible.charAt(Math.floor(Math.random() * possible.length));

        return text;
    };

    this.testManageUserLink = function(expected) {
        if (expected) {
            expect(userDropdown.ManageUsersLink.isDisplayed()).toBe(true);
        } else {
            expect(userDropdown.ManageUsersLink.isPresent()).toBe(false);
        }

    };
};

module.exports = new UserManagement();