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
        if (expected) {
            expect(userDropdown.ManageUsersLink.getWebElement().isDisplayed()).toBe(expected);
        } else {
            expect(userDropdown.ManageUsersLink.isPresent()).toBe(expected);
        }

    };
    
    this.createNewUser = function (name) {
        element(by.model('user.FirstName')).sendKeys(name);
        element(by.model('user.LastName')).sendKeys(name);
        element(by.model('user.Email')).sendKeys(name + "@gmail.com");
        element(by.css('#add-user-btn-save')).click();
        expect(element(by.css("#add-user-btn-ok")).isPresent()).toBe(true);
        element(by.css('#add-user-btn-ok')).click();
        browser.waitForAngular();
        browser.driver.sleep(1500);
    };

    this.createNewUser = function(firstName, lastName, email) {
        this.AddNewUserLink.click();
        browser.driver.sleep(1000);
        element(by.model('user.FirstName')).sendKeys(firstName);
        element(by.model('user.LastName')).sendKeys(lastName);
        element(by.model('user.Email')).sendKeys(email);
        element(by.css('#add-user-btn-save')).click();
        browser.driver.sleep(5000);
    };

    this.assertAdminLinkIsVisible = function(expected) {
        element.all(by.css('a.model')).first().click();
        browser.driver.wait(element(by.css('a.back-button')).isPresent(),
            10000, 'tabs list should appear with in 10 sec.');

        element(by.linkText('SAMPLE LEADS')).click();
        if (expected) {
            expect(element(by.linkText('Admin')).getWebElement().isDisplayed()).toBe(expected);
        } else {
            expect(element(by.linkText('Admin')).isPresent()).toBe(expected);
        }
    };

    this.waitAndSleep = function() {
        browser.waitForAngular();
        browser.driver.sleep(5000);
    };
};

module.exports = new UserManagement();