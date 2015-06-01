'use strict';

var UserManagement = function() {
    var userDropdown = require('./userdropdown.po');
    
    this.AddNewUserLink = element(by.css('#usermgmt-btn-add-user'));

    this.randomName = function(n) {
        var text = "";
        var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        for( var i=0; i < n; i++ )
            text += possible.charAt(Math.floor(Math.random() * possible.length));

        return text;
    };

    this.assertManageUsersIsVisible = function (expected) {
        expect(userDropdown.ManageUsersLink.isPresent()).toBe(expected);
    };
    
    this.createNewUser = function (name) {
        element(by.model('user.FirstName')).sendKeys(name);
        element(by.model('user.LastName')).sendKeys(name);
        element(by.model('user.Email')).sendKeys(name + "@gmail.com");
        element(by.css('#add-user-btn-save')).click();
        expect(element(by.css("#add-user-btn-ok")).isPresent()).toBe(true);
        element(by.css('#add-user-btn-ok')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    };
};

module.exports = new UserManagement();