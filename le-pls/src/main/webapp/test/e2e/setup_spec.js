'use strict';

describe('setup tests', function () {

    var helper = require('./po/helper.po');
    var loginPage = require('./po/login.po');
    var userDropdown = require('./po/userdropdown.po');
    var setup = require('./po/setup.po');
    var manageFields = require('./po/managefields.po');

    it('should validate that click manage field link to show manage fields page', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select Setup Link
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SetupLink.isPresent().then(function (present){
            if (present) { // Need to verify the link present or not by feature flag
                userDropdown.SetupLink.click();
                browser.waitForAngular();
                browser.driver.sleep(3000);

                helper.elementExists(element(by.id("setup")), true);
                helper.elementExists(element(by.id("manageFieldsWidget")), true);
            }
        });

        //==================================================
        // Logout
        //==================================================
        loginPage.logout();
    });

/*
    it('should validate that edit and cancel button', function () {
        loginPage.loginAsSuperAdmin();
        clickSetupLink();

        var grid = element(by.id('fieldsGrid'));
        grid.isDisplayed().then(function (displayed) {
            if (displayed) {
                manageFields.testBatchEditable(grid);
            }
        });

        loginPage.logout();
    });

    it('should validate that edit fields', function () {
        loginPage.loginAsSuperAdmin();
        clickSetupLink();

        var grid = element(by.id('fieldsGrid'));
        grid.isDisplayed().then(function (displayed) {
            if (displayed) {
                manageFields.testBatchEdit(grid);
            }
        });

        loginPage.logout();
    });
*/

    function clickSetupLink() {
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        expect(userDropdown.SetupLink.isPresent()).toBe(true);
        userDropdown.SetupLink.click();
        browser.waitForAngular();
        browser.driver.sleep(12000);
    }

});