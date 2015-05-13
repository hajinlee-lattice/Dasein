'use strict';

describe('system setup tests', function () {

    var loginPage = require('./po/login.po');
    var userDropdown = require('./po/userdropdown.po');
    var systemSetup = require('./po/systemsetup.po');

    it('should validate that you can go to the System Setup page', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select System Setup Tab
        //==================================================
        userDropdown.toggleDropdown();
        expect(userDropdown.SystemSetupLink.isDisplayed()).toBe(true);
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();

        loginPage.logout();
    });

    it('should validate that entering invalid Eloqua credentials will fail', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select System Setup Tab
        //==================================================
        userDropdown.toggleDropdown();
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        //==================================================
        // Enter Eloqua Credentials
        //==================================================
        systemSetup.enterBadEloquaCredentials();
        expect(element(by.css('.js-eloqua-form .alert-danger')).getText()).toBe("Credentials are invalid.");
        loginPage.logout();
    }, 10000);

    it('should validate that you can enter Eloqua credentials', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select System Setup Tab
        //==================================================
        userDropdown.toggleDropdown();
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        //==================================================
        // Enter Eloqua Credentials
        //==================================================
        systemSetup.enterValidEloquaCredentials();
        expect(element(by.css('.js-eloqua-form .alert-danger')).getText()).toBe("");

        loginPage.logout();
    });

    it('should validate that entering invalid SFDC sandbox credentials will fail', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select System Setup Tab
        //==================================================
        userDropdown.toggleDropdown();
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        //==================================================
        // Enter SFDC Sandbox Credentials
        //==================================================
        element(by.css('a[href="#formSandbox"]')).click();
        systemSetup.enterBadSfdcSandboxCredentials();
        expect(element(by.css('#formSandbox .alert-danger')).getText()).toBe("Credentials are invalid.");

        loginPage.logout();
    });

    it('should validate that you can enter SFDC sandbox credentials', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select System Setup Tab
        //==================================================
        userDropdown.toggleDropdown();
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        //==================================================
        // Enter SFDC Sandbox Credentials
        //==================================================
        element(by.css('a[href="#formSandbox"]')).click();
        systemSetup.enterValidSfdcSandboxCredentials();
        expect(element(by.css('#formSandbox .alert-danger')).getText()).toBe("");

        loginPage.logout();
    });

    it('should validate that entering invalid SFDC production credentials will fail', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select System Setup Tab
        //==================================================
        userDropdown.toggleDropdown();
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        //==================================================
        // Enter SFDC Production Credentials
        //==================================================
        systemSetup.enterBadSfdcProductionCredentials();
        expect(element(by.css('#formProduction .alert-danger')).getText()).toBe("Credentials are invalid.");

        loginPage.logout();
    });

    it('should validate that you can enter SFDC production credentials', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select System Setup Tab
        //==================================================
        userDropdown.toggleDropdown();
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        //==================================================
        // Enter SFDC Production Credentials
        //==================================================
        systemSetup.enterValidSfdcProductionCredentials();
        expect(element(by.css('#formProduction .alert-danger')).getText()).toBe("");

        loginPage.logout();
    });

});