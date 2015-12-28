'use strict';

describe('tenant deployment tests', function () {

    var helper = require('./po/helper.po');
    var loginPage = require('./po/login.po');
    var userDropdown = require('./po/userdropdown.po');
    var tenantDeployment = require('./po/tenantdeployment.po');

    it('should validate that you can click deployment wizard link to show deployment wizard page', function () {
        login();

        //==================================================
        // Click Deployment Wizard Link
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.TenantDeploymentWizard.isPresent().then(function (present){
            if (present) {
                userDropdown.TenantDeploymentWizard.click();
                browser.waitForAngular();
                browser.driver.sleep(5000);

                helper.elementExists(element(by.id('deploymentWizard')), true);
            }
        });

        loginPage.logout();
    });

    it('should validate that you can download SFDC guide', function () {
        login();

        //==================================================
        // Download SFDC Guide
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.TenantDeploymentWizard.isPresent().then(function (present){
            if (present) {
                userDropdown.TenantDeploymentWizard.click();
                browser.waitForAngular();
                browser.driver.sleep(5000);

                element(by.id('credentials')).isPresent().then(function (present){
                    if (present) {
                        tenantDeployment.testDownloadSfdcGuide();
                    }
                });
            }
        });

        loginPage.logout();
    });

    it('should validate that you can import and clear data', function () {
        login();

        //==================================================
        // Import and Clear Data
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.TenantDeploymentWizard.isPresent().then(function (present){
            if (present) {
                userDropdown.TenantDeploymentWizard.click();
                browser.waitForAngular();
                browser.driver.sleep(5000);

                element(by.id('credentials')).isPresent().then(function (present){
                    if (present) {
                        tenantDeployment.testImportAndClearData();
                    }
                });
            }
        });

        loginPage.logout();
    });

    it('should validate that you can click clear deployment link', function () {
        login();

        //==================================================
        // Clear Tenant Deployment Link
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.TenantDeploymentWizard.isPresent().then(function (present){
            if (present) {
                userDropdown.TenantDeploymentWizard.click();
                browser.waitForAngular();
                browser.driver.sleep(5000);

                element(by.id('importAndEnrichData')).isPresent().then(function (present){
                    if (present) {
                        tenantDeployment.testClearDeploymentLink();
                    }
                });
            }
        });

        loginPage.logout();
    });

    it('should validate that you can click done button', function () {
        login();

        //==================================================
        // Done Button
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.TenantDeploymentWizard.isPresent().then(function (present){
            if (present) {
                userDropdown.TenantDeploymentWizard.click();
                browser.waitForAngular();
                browser.driver.sleep(5000);

                element(by.id('importAndEnrichData')).isPresent().then(function (present){
                    if (present) {
                        tenantDeployment.testDoneButton();
                    }
                });
            }
        });

        loginPage.logout();
    });

    function login() {
        // Login without verify page-title.
        loginPage.login(browser.params.superAdminUsername, browser.params.testingUserPassword);
    };
});