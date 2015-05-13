'use strict';

var SystemSetup = function() {
    this.enterValidSfdcProductionCredentials = function() {
        //element(by.css('.js-crm-production-edit-button')).click();
        element(by.model('crmProductionCredentials.UserName')).clear();
        element(by.model('crmProductionCredentials.UserName')).sendKeys("apeters-widgettech@lattice-engines.com");
        element(by.model('crmProductionCredentials.Password')).clear();
        element(by.model('crmProductionCredentials.Password')).sendKeys("Happy2010");
        element(by.model('crmProductionCredentials.SecurityToken')).clear();
        element(by.model('crmProductionCredentials.SecurityToken')).sendKeys("oIogZVEFGbL3n0qiAp6F66TC");
        element(by.css('.js-crm-production-save-button')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    };
    
    this.enterBadSfdcProductionCredentials = function() {
        //element(by.css('.js-crm-production-edit-button')).click();
        element(by.model('crmProductionCredentials.UserName')).clear();
        element(by.model('crmProductionCredentials.UserName')).sendKeys("nope");
        element(by.model('crmProductionCredentials.Password')).clear();
        element(by.model('crmProductionCredentials.Password')).sendKeys("nope");
        element(by.model('crmProductionCredentials.SecurityToken')).clear();
        element(by.model('crmProductionCredentials.SecurityToken')).sendKeys("nope");
        element(by.css('.js-crm-production-save-button')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    };

    this.waitForSfdcSandboxCredentials = function() {
        browser.driver.wait(element(by.model('crmSandboxCredentials.UserName')).isPresent(), 5000,
            "SFDC sandbox crendentials form should show up within 5 sec.");
    };
    
    this.enterValidSfdcSandboxCredentials = function() {
        //element(by.css('.js-crm-sandbox-edit-button')).click();
        element(by.model('crmSandboxCredentials.UserName')).clear();
        element(by.model('crmSandboxCredentials.UserName')).sendKeys("apeters-widgettech@lattice-engines.com");
        element(by.model('crmSandboxCredentials.Password')).clear();
        element(by.model('crmSandboxCredentials.Password')).sendKeys("Happy2010");
        element(by.model('crmSandboxCredentials.SecurityToken')).clear();
        element(by.model('crmSandboxCredentials.SecurityToken')).sendKeys("oIogZVEFGbL3n0qiAp6F66TC");
        element(by.css('.js-crm-sandbox-save-button')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    };
    
    this.enterBadSfdcSandboxCredentials = function() {
        //element(by.css('.js-crm-sandbox-edit-button')).click();
        element(by.model('crmSandboxCredentials.UserName')).clear();
        element(by.model('crmSandboxCredentials.UserName')).sendKeys("nope");
        element(by.model('crmSandboxCredentials.Password')).clear();
        element(by.model('crmSandboxCredentials.Password')).sendKeys("nope");
        element(by.model('crmSandboxCredentials.SecurityToken')).clear();
        element(by.model('crmSandboxCredentials.SecurityToken')).sendKeys("nope");
        element(by.css('.js-crm-sandbox-save-button')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    };

    this.waitForSfdcCredentials = function() {
        browser.driver.wait(element(by.css('a[href="#formSandbox"]')).isPresent(), 10000,
            "SFDC crendentials form should show up within 10 sec.");
    };

    this.enterValidEloquaCredentials = function() {
        //element(by.css('.js-eloqua-edit-button')).click();
        element(by.css('.js-eloqua-form input.js-user-name')).clear();
        element(by.css('.js-eloqua-form input.js-user-name')).sendKeys("Matt.Sable");
        element(by.css('.js-eloqua-form input.js-password')).clear();
        element(by.css('.js-eloqua-form input.js-password')).sendKeys("Lattice1");
        element(by.css('.js-eloqua-form input.js-company')).clear();
        element(by.css('.js-eloqua-form input.js-company')).sendKeys("TechnologyPartnerLatticeEngines");
        element(by.css('.js-eloqua-save-button')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    };
    
    this.enterBadEloquaCredentials = function() {
        //element(by.css('.js-eloqua-edit-button')).click();
        element(by.css('.js-eloqua-form input.js-user-name')).clear();
        element(by.css('.js-eloqua-form input.js-user-name')).sendKeys("nope");
        element(by.css('.js-eloqua-form input.js-password')).clear();
        element(by.css('.js-eloqua-form input.js-password')).sendKeys("nope");
        element(by.css('.js-eloqua-form input.js-company')).clear();
        element(by.css('.js-eloqua-form input.js-company')).sendKeys("nope");
        element(by.css('.js-eloqua-save-button')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    };

    this.waitForEloquaCredentials = function() {
        browser.driver.wait(element(by.css('.js-eloqua-form input.js-user-name')).isPresent(), 10000,
            "Eloqua crendentials form should show up within 10 sec.");
        browser.driver.sleep(5000);
    };
};

module.exports = new SystemSetup();