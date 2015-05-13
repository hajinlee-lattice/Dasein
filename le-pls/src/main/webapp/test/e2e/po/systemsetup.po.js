'use strict';

var SystemSetup = function() {
    this.enterValidSfdcProductionCredentials = function() {
        element(by.model('crmProductionCredentials.UserName')).clear();
        element(by.model('crmProductionCredentials.UserName')).sendKeys("apeters-widgettech@lattice-engines.com");
        element(by.model('crmProductionCredentials.Password')).clear();
        element(by.model('crmProductionCredentials.Password')).sendKeys("Happy2010");
        element(by.model('crmProductionCredentials.SecurityToken')).clear();
        element(by.model('crmProductionCredentials.SecurityToken')).sendKeys("oIogZVEFGbL3n0qiAp6F66TC");
        element(by.css('.js-crm-production-save-button')).click();
        browser.waitForAngular();
    };
    
    this.enterBadSfdcProductionCredentials = function() {
        element(by.model('crmProductionCredentials.UserName')).clear();
        element(by.model('crmProductionCredentials.UserName')).sendKeys("nope");
        element(by.model('crmProductionCredentials.Password')).clear();
        element(by.model('crmProductionCredentials.Password')).sendKeys("nope");
        element(by.model('crmProductionCredentials.SecurityToken')).clear();
        element(by.model('crmProductionCredentials.SecurityToken')).sendKeys("nope");
        element(by.css('.js-crm-production-save-button')).click();
        browser.waitForAngular();
    };
    
    this.enterValidSfdcSandboxCredentials = function() {
        element(by.model('crmSandboxCredentials.UserName')).clear();
        element(by.model('crmSandboxCredentials.UserName')).sendKeys("apeters-widgettech@lattice-engines.com");
        element(by.model('crmSandboxCredentials.Password')).clear();
        element(by.model('crmSandboxCredentials.Password')).sendKeys("Happy2010");
        element(by.model('crmSandboxCredentials.SecurityToken')).clear();
        element(by.model('crmSandboxCredentials.SecurityToken')).sendKeys("oIogZVEFGbL3n0qiAp6F66TC");
        element(by.css('.js-crm-sandbox-save-button')).click();
        browser.waitForAngular();
    };
    
    this.enterBadSfdcSandboxCredentials = function() {
        element(by.model('crmSandboxCredentials.UserName')).clear();
        element(by.model('crmSandboxCredentials.UserName')).sendKeys("nope");
        element(by.model('crmSandboxCredentials.Password')).clear();
        element(by.model('crmSandboxCredentials.Password')).sendKeys("nope");
        element(by.model('crmSandboxCredentials.SecurityToken')).clear();
        element(by.model('crmSandboxCredentials.SecurityToken')).sendKeys("nope");
        element(by.css('.js-crm-sandbox-save-button')).click();
        browser.waitForAngular();
    };
    
    this.enterValidEloquaCredentials = function() {
        element(by.css('.js-eloqua-form input.js-user-name')).clear();
        element(by.css('.js-eloqua-form input.js-user-name')).sendKeys("Matt.Sable");
        element(by.css('.js-eloqua-form input.js-password')).clear();
        element(by.css('.js-eloqua-form input.js-password')).sendKeys("Lattice1");
        element(by.css('.js-eloqua-form input.js-company')).clear();
        element(by.css('.js-eloqua-form input.js-company')).sendKeys("TechnologyPartnerLatticeEngines");
        element(by.css('.js-eloqua-save-button')).click();
        browser.waitForAngular();
    };
    
    this.enterBadEloquaCredentials = function() {
        element(by.css('.js-eloqua-form input.js-user-name')).clear();
        element(by.css('.js-eloqua-form input.js-user-name')).sendKeys("nope");
        element(by.css('.js-eloqua-form input.js-password')).clear();
        element(by.css('.js-eloqua-form input.js-password')).sendKeys("nope");
        element(by.css('.js-eloqua-form input.js-company')).clear();
        element(by.css('.js-eloqua-form input.js-company')).sendKeys("nope");
        element(by.css('.js-eloqua-save-button')).click();
        browser.waitForAngular();
    };

};

module.exports = new SystemSetup();