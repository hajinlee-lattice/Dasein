'use strict';

var UserManagement = function() {
    var userDropdown = require('./userdropdown.po');
    
    this.AddNewUserLink = element(by.css('#usermgmt-btn-add-user'));
    this.tempUserFirstName = 'Temp';
    this.tempUserLastName = 'User';

    // the '0' and 'ext' necessary to have the user show up as first in the user managemnt list
    this.tempUserEmail = '0000tempuser@lattice-engines.ext'; 

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

    this.enterUserInfoAndClickOkay = function(firstName, lastName, email) {
        expect(element(by.css('#add-user-modal')).isPresent()).toBe(true);
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