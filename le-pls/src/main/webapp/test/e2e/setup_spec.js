'use strict';

describe('setup tests', function () {

    var helper = require('./po/helper.po');
    var loginPage = require('./po/login.po');
    var userDropdown = require('./po/userdropdown.po');
    var setup = require('./po/setup.po');

    it('should validate that click each link in nav will show the right panel', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select Setup Link
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        expect(userDropdown.SetupLink.isPresent()).toBe(true);
        userDropdown.SetupLink.click();
        browser.waitForAngular();
        expect(element(by.id('setup')).getWebElement().isDisplayed()).toBe(true);

        //==================================================
        // Toggle Nav Link
        //==================================================
        /*
        expect(element(by.css('.setup-main-panel data-setup-home')).getWebElement().isDisplayed()).toBe(false);
        setup.getNavLinkByNodeName('home').click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(element(by.css('.setup-main-panel data-setup-home')).getWebElement().isDisplayed()).toBe(true);

        expect(element(by.css('.setup-main-panel data-setup-data')).getWebElement().isDisplayed()).toBe(false);
        setup.getNavLinkByNodeName('data').click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(element(by.css('.setup-main-panel data-setup-data')).getWebElement().isDisplayed()).toBe(true);
        */

        expect(element(by.css('.setup-main-panel data-manage-fields')).getWebElement().isDisplayed()).toBe(false);
        setup.getNavLinkByNodeName('manageFields').click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(element(by.css('.setup-main-panel data-manage-fields')).getWebElement().isDisplayed()).toBe(true);

        //==================================================
        // Logout
        //==================================================
        loginPage.logout();
    });

});