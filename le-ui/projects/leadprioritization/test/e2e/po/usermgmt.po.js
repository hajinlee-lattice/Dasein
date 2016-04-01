'use strict';

var UserManagement = function() {
    var userDropdown = require('./userdropdown.po');
    var siderbar = require('./siderbar.po');
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

    this.canSeePredictionModelsLink = function (expected) {
        helper.elementExists(siderbar.PredictionModelsLink, expected,
            expected ? "should see Prediction Models" : "should not see Prediction Models");
    };

    this.canSeeCreateModelLink = function (expected) {
        helper.elementExists(siderbar.CreateModelLink, expected,
            expected ? "should see Create a Model" : "should not see Create a Model");
    };

    this.canSeeManageUsersLink = function (expected) {
        helper.elementExists(siderbar.ManageUsersLink, expected,
            expected ? "should see Manage Users" : "should not see Manage Users");
    };

    this.canSeeModelCreationHistoryLink = function (expected) {
        helper.elementExists(siderbar.ModelCreationHistoryLink, expected,
            expected ? "should see Model Creation History" : "should not see Model Creation History");
    };

    this.canSeeJobsLink = function (expected) {
        helper.elementExists(siderbar.JobsLink, expected,
            expected ? "should see Jobs" : "should not see Jobs");
    };

    this.canSeeMarketoSettingsLink = function (expected) {
        helper.elementExists(siderbar.MarketoSettingsLink, expected,
            expected ? "should see Marketo Settings" : "should not see Marketo Settings");
    };

    this.canSeeAttributesLink = function (expected) {
        helper.elementExists(siderbar.AttributesLink, expected,
            expected ? "should see Attributes" : "should not see Attributes");
    };

    this.canSeePerformanceLink = function (expected) {
        helper.elementExists(siderbar.PerformanceLink, expected,
            expected ? "should see Performance" : "should not see Performance");
    };

    this.canSeeSampleLeadsLink = function (expected) {
        helper.elementExists(siderbar.SampleLeadsLink, expected,
            expected ? "should see Sample Leads" : "should not see Sample Leads");
    };

    this.canSeeModelSummaryLink = function (expected) {
        helper.elementExists(siderbar.ModelSummaryLink, expected,
            expected ? "should see Model Summary" : "should not see Model Summary");
    }

    this.canSeeScoringLink = function (expected) {
        helper.elementExists(siderbar.ScoringLink, expected,
            expected ? "should see Scoring" : "should not see Scoring");
    }

    this.canSeeRefineAndCloneLink = function (expected) {
        helper.elementExists(siderbar.RefineAndCloneLink, expected,
            expected ? "should see Refine And Clone" : "should not see Refine And Clone");
    }

    this.canSeeHiddenAdminLink = function(expected) {
        element(by.linkText('Sample Leads')).click();
        helper.elementExists(element(by.linkText('Admin')), expected,
            expected ? "should see Admin link" : "should not see Admin link");
    };

    this.clickFirstModel = function() {
        element.all(by.css('div.model')).first().click();
        browser.waitForAngular();
        browser.driver.wait(element(by.css('div.menu-header')).isPresent(),
            10000, 'model page should appear with in 10 sec.');
    }

    this.backToAllModelsPage = function() {
        element(by.css('#nav .menu-header > a')).click();
        browser.waitForAngular();
        browser.driver.sleep(2000);
    }

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
        element(by.css('.user-management-list')).element(by.css('table')).getInnerHtml().then(function(html){
            expect(html.indexOf(username) > -1).toBe(expected, expected ? "should " + message : "should not " + message)
        });
    };

    this.waitAndSleep = function() {
        browser.waitForAngular();
        browser.driver.sleep(5000);
    };
};

module.exports = new UserManagement();