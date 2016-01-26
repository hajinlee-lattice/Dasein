'use strict';

describe('system setup tests', function () {

    var helper = require('./po/helper.po');
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
        browser.waitForAngular();
        expect(userDropdown.SystemSetupLink.isPresent()).toBe(true);
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();
        systemSetup.waitForSfdcCredentials();

        //==================================================
        // Logout
        //==================================================
        loginPage.logout();
    });

    it('should validate that you can enter Eloqua credentials', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select System Setup Tab
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();
        systemSetup.waitForEloquaCredentials();

        //==================================================
        // Enter Bad Eloqua Credentials
        //==================================================
        systemSetup.enterBadEloquaCredentials();
        expect(element(by.css('.js-eloqua-form .alert-danger')).getText()).toBe("Credentials are invalid.");

        browser.driver.sleep(2000);

        //==================================================
        // Enter Valid Eloqua Credentials
        //==================================================
        systemSetup.enterValidEloquaCredentials();
        expect(element(by.css('.js-eloqua-form .alert-danger')).getText()).toBe("");

        //==================================================
        // Logout
        //==================================================
        loginPage.logout();
    });

    it('should validate that you can enter Marketo credentials', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin(browser.params.alternativeTenantId);

        //==================================================
        // Select System Setup Tab
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();
        systemSetup.waitForMarketoCredentials();

        //==================================================
        // Enter Bad Marketo Credentials
        //==================================================
        systemSetup.enterBadMarketoCredentials();
        expect(element.all(by.css('.formMT .alert-danger')).first().getText()).toBe("Credentials are invalid.");

        browser.driver.sleep(2000);

        //==================================================
        // Enter Valid Marketo Credentials
        //==================================================
        systemSetup.enterValidMarketoCredentials();
        expect(element.all(by.css('.formMT .alert-danger')).first().getText()).toBe("");

        //==================================================
        // Logout
        //==================================================
        loginPage.logout();
    });

    //it('should validate that you can enter SFDC sandbox credentials', function () {
    //    //==================================================
    //    // Login
    //    //==================================================
    //    loginPage.loginAsSuperAdmin();
    //
    //    //==================================================
    //    // Select System Setup Tab
    //    //==================================================
    //    userDropdown.toggleDropdown();
    //    browser.waitForAngular();
    //    userDropdown.SystemSetupLink.click();
    //    browser.waitForAngular();
    //    systemSetup.waitForSfdcCredentials();
    //
    //    //==================================================
    //    // Enter Bad SFDC Sandbox Credentials
    //    //==================================================
    //    element(by.css('a[href="#formSandbox"]')).click();
    //    systemSetup.waitForSfdcSandboxCredentials();
    //    systemSetup.enterBadSfdcSandboxCredentials();
    //    expect(element(by.css('#formSandbox .alert-danger')).getText()).toBe("Credentials are invalid.");
    //
    //    browser.driver.sleep(2000);
    //
    //    //==================================================
    //    // Enter Valid SFDC Sandbox Credentials
    //    //==================================================
    //    systemSetup.enterValidSfdcSandboxCredentials();
    //    expect(element(by.css('#formSandbox .alert-danger')).getText()).toBe("");
    //
    //    //==================================================
    //    // Logout
    //    //==================================================
    //    loginPage.logout();
    //});

    it('should validate SFDC production credentials', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select System Setup Tab
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();
        systemSetup.waitForSfdcCredentials();

        //==================================================
        // Enter Bad SFDC Production Credentials
        //==================================================
        systemSetup.enterBadSfdcProductionCredentials();
        expect(element(by.css('#formProduction .alert-danger')).getText()).toBe("Credentials are invalid.");

        browser.driver.sleep(2000);

        //==================================================
        // Enter Valid SFDC Production Credentials
        //==================================================
        systemSetup.enterValidSfdcProductionCredentials();
        expect(element(by.css('#formProduction .alert-danger')).getText()).toBe("");

        //==================================================
        // Logout
        //==================================================
        loginPage.logout();
    });

    it('should verify that Eloqua and SFDC credentials are saved', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select System Setup Tab
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();
        systemSetup.waitForEloquaCredentials();

        //==================================================
        // Verify Eloqua Credentials
        //==================================================
        browser.driver.sleep(2000);
        systemSetup.verifyEloquaCredentialsSaved();
        systemSetup.verifySfdcProductionCredentialsSaved();

        //element(by.css('a[href="#formSandbox"]')).click();
        //systemSetup.waitForSfdcSandboxCredentials();
        //systemSetup.verifySfdcSandboxCredentialsSaved();

        //==================================================
        // Logout
        //==================================================
        loginPage.logout();
    });

    it('should verify that Marketo credentials are saved', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin(browser.params.alternativeTenantId);

        //==================================================
        // Select System Setup Tab
        //==================================================
        navigateToSystemSetup();
        systemSetup.waitForMarketoCredentials();
        browser.driver.sleep(5000);

        //==================================================
        // Verify Eloqua Credentials
        //==================================================
        systemSetup.verifyMarketoCredentialsSaved();

        //==================================================
        // Logout
        //==================================================
        loginPage.logout();
    });

    function navigateToSystemSetup() {
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();
    }

});