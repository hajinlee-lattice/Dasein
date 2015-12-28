'use strict';

var TenantDeployment = function() {
    var helper = require('./helper.po');

    this.testDownloadSfdcGuide = function () {
        var downloadLink = element(by.linkText('Salesforce Configuration Guide'));
        helper.elementExists(downloadLink, true);
        downloadLink.getAttribute('download').then(function (download) {
            var fileName = download + '.pdf';
            helper.removeFile(fileName);
            downloadLink.click();
            helper.fileExists(fileName);
        });
    };

    this.testImportAndClearData = function () {
        element(by.css('.js-crm-production-edit-button')).isDisplayed().then(function (displayed){
            if (displayed) {
                element(by.css('.js-crm-production-edit-button')).click();
            }
            enterCredentials();
            element(by.css('.js-crm-production-save-button')).click();
            browser.waitForAngular();
            browser.driver.sleep(15000);

            helper.elementExists(element(by.id('importAndEnrichData')), true);
            cancelAndClearDeployment();
        });
    };

    function enterCredentials() {
        element(by.model('productionCredentials.UserName')).clear();
        element(by.model('productionCredentials.UserName')).sendKeys("apeters-widgettech@lattice-engines.com");
        element(by.model('productionCredentials.Password')).clear();
        element(by.model('productionCredentials.Password')).sendKeys("Happy2010");
        element(by.model('productionCredentials.SecurityToken')).clear();
        element(by.model('productionCredentials.SecurityToken')).sendKeys("oIogZVEFGbL3n0qiAp6F66TC");
    }

    function cancelAndClearDeployment() {
        var cancelLink = element(by.id('importAndEnrichData')).element(by.linkText('Cancel'));
        cancelLink.isPresent().then(function (present) {
            if (present) {
                cancelLink.click();
                browser.waitForAngular();
                browser.driver.sleep(500);
                element(by.id('cancel-deployment-step-yes')).click();
                browser.waitForAngular();
                browser.driver.sleep(10000);

                var clearLink = element(by.id('clearDeploymentLink'));
                clearLink.getAttribute('class').then(function (classes) {
                    if (classes.indexOf('disabled') < 0) {
                        clearLink.click();
                        browser.waitForAngular();
                        browser.driver.sleep(500);
                        element(by.id('clear-deployment-yes')).click();
                        browser.waitForAngular();
                    }
                });
            }
        });
    }

    this.testClearDeploymentLink = function () {
        var clearLink = element(by.id('clearDeploymentLink'));
        helper.elementExists(clearLink, true);
        expect(clearLink.getAttribute('class')).not.toContain('disabled');
        clearLink.click();
        browser.waitForAngular();
        browser.driver.sleep(500);
        element(by.id('clear-deployment-no')).click();
        browser.waitForAngular();
        browser.driver.sleep(500);
    };

    this.testDoneButton = function () {
        var doneBtn = element(by.id('finishDeploymentBtn'));
        doneBtn.isPresent().then(function (present) {
            if (present) {
                doneBtn.click();
                browser.waitForAngular();
                browser.driver.sleep(3000);
                helper.elementExists(element(by.id('deploymentWizard'), false));
            }
        });
    };
};

module.exports = new TenantDeployment();