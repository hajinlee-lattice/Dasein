'use strict';

var UserManagement = function() {
    var userDropdown = require('./userdropdown.po');
    var helper = require('./helper.po');
    
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

    this.canSeeManageUsersLink = function (expected) {
        helper.elementExists(userDropdown.ManageUsersLink, expected,
            expected ? "should see Manage Users" : "should not see Manage Users");
    };

    this.canSeeSystemSetupLink = function (expected) {
        helper.elementExists(userDropdown.SystemSetupLink, expected,
            expected ? "should see System Setup" : "should not see System Setup");
    };

    this.canSeeActivateModelLink = function (expected) {
        helper.elementExists(userDropdown.ActivateModelLink, expected,
            expected ? "should see Activate Model" : "should not see Activate Model");
    };

    this.canSeeModelCreationHistoryLink = function (expected) {
        helper.elementExists(userDropdown.ModelCreationHistoryLink, expected,
            expected ? "should see Model Creation History" : "should not see Model Creation History");
    };

    this.canSeeSetupLink = function (expected) {
        helper.elementExists(userDropdown.SetupLink, expected,
            expected ? "should see Manage Fields" : "should not see Manage Fields");
    };

    this.canSeeHiddenAdminLink = function(expected) {
        element.all(by.css('a.model')).first().click();
        browser.driver.wait(element(by.css('a.back-button')).isPresent(),
            10000, 'tabs list should appear with in 10 sec.');
        element(by.linkText('SAMPLE LEADS')).click();
        helper.elementExists(element(by.linkText('Admin')), expected,
            expected ? "should see Admin link" : "should not see Admin link");
    };

    this.enterUserInfoAndClickOkay = function(firstName, lastName, email) {
        expect(element(by.css('#add-user-modal')).isPresent()).toBe(true);
        element(by.model('user.FirstName')).sendKeys(firstName);
        element(by.model('user.LastName')).sendKeys(lastName);
        element(by.model('user.Email')).sendKeys(email);
        element(by.css('#add-user-btn-save')).click();
        browser.driver.sleep(5000);
    };

    this.canEditAndDeleteUser = function(username, expected) {
        var message = "be able to delete user " + username;
        element.all(by.repeater('user in users')).each(function(tr){
            tr.getInnerHtml().then(function(html){
                if (html.indexOf(username) > -1) {
                    expect(helper.elementExists(tr.element(by.css("i.fa-trash-o")), expected,
                        expected ? "should " + message : "should not " + message));
                }
            });
        });
    };

    this.canSeeUser = function(username, expected) {
        var message = "be able to see user " + username;
        element(by.css('.user-list')).element(by.css('table')).getInnerHtml().then(function(html){
            expect(html.indexOf(username) > -1).toBe(expected, expected ? "should " + message : "should not " + message)
        });
    };

    this.waitAndSleep = function() {
        browser.waitForAngular();
        browser.driver.sleep(12000);
    };
};

module.exports = new UserManagement();