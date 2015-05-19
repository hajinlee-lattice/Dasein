'use strict';

describe('system setup tests', function () {

    var loginPage = require('./po/login.po');
    var userDropdown = require('./po/userdropdown.po');
    var systemSetup = require('./po/systemsetup.po');

    beforeEach(function(){
        loginPage.loginAsSuperAdmin();
        loginPage.logout();
        browser.driver.sleep(10000);
    });

    afterEach(function(){
        loginPage.logout();
        browser.driver.sleep(25000);
    });

    it('should validate that you can go to the System Setup page', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select System Setup Tab
        //==================================================
        userDropdown.toggleDropdown();
        expect(userDropdown.SystemSetupLink.isPresent()).toBe(true);
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();
        systemSetup.waitForSfdcCredentials();
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
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();
        systemSetup.waitForEloquaCredentials();

        //==================================================
        // Enter Bad Eloqua Credentials
        //==================================================
        systemSetup.enterBadEloquaCredentials();
        expect(element(by.css('.js-eloqua-form .alert-danger')).getText()).toBe("Credentials are invalid.");

        //==================================================
        // Enter Valid Eloqua Credentials
        //==================================================
        //systemSetup.enterValidEloquaCredentials();
        //expect(element(by.css('.js-eloqua-form .alert-danger')).getText()).toBe("");
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
        userDropdown.SystemSetupLink.click();
        browser.waitForAngular();
        systemSetup.waitForMarketoCredentials();

        //==================================================
        // Enter Bad Marketo Credentials
        //==================================================
        systemSetup.enterBadMarketoCredentials();
        expect(element.all(by.css('.formMT .alert-danger')).first().getText()).toBe("Credentials are invalid.");

        //==================================================
        // Enter Valid Marketo Credentials
        //==================================================
        //systemSetup.enterValidMarketoCredentials();
        //expect(element.all(by.css('.formMT .alert-danger')).first().getText()).toBe("");
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
    //    userDropdown.SystemSetupLink.click();
    //    browser.waitForAngular();
    //    systemSetup.waitForSfdcCredentials();
    //
    //    //==================================================
    //    // Enter Valid SFDC Sandbox Credentials
    //    //==================================================
    //    element(by.css('a[href="#formSandbox"]')).click();
    //    systemSetup.waitForSfdcSandboxCredentials();
    //    systemSetup.enterValidSfdcSandboxCredentials();
    //    expect(element(by.css('#formSandbox .alert-danger')).getText()).toBe("");
    //});

    it('should validate that bad SFDC sandbox credentials failed', function () {
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
        systemSetup.waitForSfdcCredentials();

        //==================================================
        // Enter Bad SFDC Sandbox Credentials
        //==================================================
        element(by.css('a[href="#formSandbox"]')).click();
        systemSetup.waitForSfdcSandboxCredentials();
        systemSetup.enterBadSfdcSandboxCredentials();
        expect(element(by.css('#formSandbox .alert-danger')).getText()).toBe("Credentials are invalid.");
    });

    //it('should validate that you can enter SFDC production credentials', function () {
    //    //==================================================
    //    // Login
    //    //==================================================
    //    loginPage.loginAsSuperAdmin();
    //
    //    //==================================================
    //    // Select System Setup Tab
    //    //==================================================
    //    userDropdown.toggleDropdown();
    //    userDropdown.SystemSetupLink.click();
    //    browser.waitForAngular();
    //    systemSetup.waitForSfdcCredentials();
    //
    //    //==================================================
    //    // Enter Valid SFDC Production Credentials
    //    //==================================================
    //    systemSetup.enterValidSfdcProductionCredentials();
    //    expect(element(by.css('#formProduction .alert-danger')).getText()).toBe("");
    //});

    it('should validate that bad SFDC production credentials failed', function () {
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
        systemSetup.waitForSfdcCredentials();

        //==================================================
        // Enter Bad SFDC Production Credentials
        //==================================================
        systemSetup.enterBadSfdcProductionCredentials();
        expect(element(by.css('#formProduction .alert-danger')).getText()).toBe("Credentials are invalid.");
    });

});