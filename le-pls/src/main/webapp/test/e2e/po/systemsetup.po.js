'use strict';

var SystemSetup = function() {
    var helper = require('./helper.po');

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

    this.verifySfdcProductionCredentialsSaved = function() {
        expect(element(by.model('crmProductionCredentials.UserName')).getAttribute("readonly")).toBeTruthy();
        expect(element(by.model('crmProductionCredentials.Password')).getAttribute("readonly")).toBeTruthy();
        expect(element(by.model('crmProductionCredentials.SecurityToken')).getAttribute("readonly")).toBeTruthy();
        expect(element(by.model('crmProductionCredentials.UserName')).getAttribute("value")).toBe("apeters-widgettech@lattice-engines.com");
        helper.elementExists(element(by.css('.js-crm-production-edit-button')), true);
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
        element(by.model('crmSandboxCredentials.UserName')).sendKeys("tsanghavi@lattice-engines.com.sandbox2");
        element(by.model('crmSandboxCredentials.Password')).clear();
        element(by.model('crmSandboxCredentials.Password')).sendKeys("Happy2010");
        element(by.model('crmSandboxCredentials.SecurityToken')).clear();
        element(by.model('crmSandboxCredentials.SecurityToken')).sendKeys("5aGieJUACRPQ21CG3nUwn8iz");
        element(by.css('.js-crm-sandbox-save-button')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    };

    this.verifySfdcSandboxCredentialsSaved = function() {
        expect(element(by.model('crmSandboxCredentials.UserName')).getAttribute("readonly")).toBeTruthy();
        expect(element(by.model('crmSandboxCredentials.Password')).getAttribute("readonly")).toBeTruthy();
        expect(element(by.model('crmSandboxCredentials.SecurityToken')).getAttribute("readonly")).toBeTruthy();
        expect(element(by.model('crmSandboxCredentials.UserName')).getAttribute("value")).toBe("tsanghavi@lattice-engines.com.sandbox2");
        helper.elementExists(element(by.css('.js-crm-sandbox-edit-button')), true);
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

    this.verifyEloquaCredentialsSaved = function() {
        expect(element(by.css('.js-eloqua-form input.js-user-name')).getAttribute("readonly")).toBeTruthy();
        expect(element(by.css('.js-eloqua-form input.js-password')).getAttribute("readonly")).toBeTruthy();
        expect(element(by.css('.js-eloqua-form input.js-company')).getAttribute("readonly")).toBeTruthy();
        expect(element(by.css('.js-eloqua-form input.js-user-name')).getAttribute("value")).toBe("Matt.Sable");
        expect(element(by.css('.js-eloqua-form input.js-company')).getAttribute("value")).toBe("TechnologyPartnerLatticeEngines");
        helper.elementExists(element(by.css('.js-eloqua-edit-button')), true);
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
    };

    this.enterValidMarketoCredentials = function() {
        var formMT = element.all(by.css('div.formMT')).first();
        formMT.element(by.model('mapCredentials.UserName')).clear();
        formMT.element(by.model('mapCredentials.UserName')).sendKeys("latticeenginessandbox1_9026948050BD016F376AE6");
        formMT.element(by.model('mapCredentials.Password')).clear();
        formMT.element(by.model('mapCredentials.Password')).sendKeys("41802295835604145500BBDD0011770133777863CA58");
        formMT.element(by.model('mapCredentials.Url')).clear();
        formMT.element(by.model('mapCredentials.Url')).sendKeys("https://na-sj02.marketo.com/soap/mktows/2_0");
        element(by.css('.js-marketo-save-button')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    };

    this.verifyMarketoCredentialsSaved = function() {
        expect(element(by.model('mapCredentials.UserName')).getAttribute("readonly")).toBeTruthy();
        expect(element(by.model('mapCredentials.Password')).getAttribute("readonly")).toBeTruthy();
        expect(element(by.model('mapCredentials.Url')).getAttribute("readonly")).toBeTruthy();
        expect(element(by.model('mapCredentials.UserName')).getAttribute("value")).toBe("latticeenginessandbox1_9026948050BD016F376AE6");
        expect(element(by.model('mapCredentials.Url')).getAttribute("value")).toBe("https://na-sj02.marketo.com/soap/mktows/2_0");
        helper.elementExists(element(by.css('.js-marketo-edit-button')), true);
    };

    this.enterBadMarketoCredentials = function() {
        var formMT = element.all(by.css('div.formMT')).first();
        formMT.element(by.model('mapCredentials.UserName')).clear();
        formMT.element(by.model('mapCredentials.UserName')).sendKeys("nope");
        formMT.element(by.model('mapCredentials.Password')).clear();
        formMT.element(by.model('mapCredentials.Password')).sendKeys("nope");
        formMT.element(by.model('mapCredentials.Url')).clear();
        formMT.element(by.model('mapCredentials.Url')).sendKeys("nope");
        element(by.css('.js-marketo-save-button')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    };

    this.waitForMarketoCredentials = function() {
        browser.driver.wait(element(by.css('div.formMT')).isPresent(), 10000,
            "Eloqua crendentials form should show up within 10 sec.");
    };
};

module.exports = new SystemSetup();