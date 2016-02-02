'use strict';

describe('manage fields tests', function () {

    var helper = require('./po/helper.po');
    var loginPage = require('./po/login.po');
    var userDropdown = require('./po/userdropdown.po');
    var leadEnrichment = require('./po/leadenrichment.po');

    it('should validate that you can click lead enrichment link to show lead enrichment page', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Click Lead Enrichment Link
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.LeadEnrichment.isPresent().then(function (present){
            if (present) {
                userDropdown.LeadEnrichment.click();
                browser.waitForAngular();
                browser.driver.sleep(3000);

                helper.elementExists(element(by.id("leadEnrichment")), true);
            }
        });

        loginPage.logout();
    });

    it('should validate that you can click all attributes link', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Show All Attributes
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.LeadEnrichment.isPresent().then(function (present){
            if (present) {
                clickLeadEnrichmentLink();
                element(by.id('saveLeadEnrichmentAttributesButton')).isDisplayed().then(function (displayed) {
                    if (displayed) {
                        leadEnrichment.testClickAllAttributes();
                    }
                });
            }
        });

        loginPage.logout();
    });

    it('should validate that you can add attributes', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Add Attributes
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.LeadEnrichment.isPresent().then(function (present){
            if (present) {
                clickLeadEnrichmentLink();
                element(by.id('saveLeadEnrichmentAttributesButton')).isDisplayed().then(function (displayed) {
                    if (displayed) {
                        leadEnrichment.testAddAttributes();
                    }
                });
            }
        });

        loginPage.logout();
    });

    it('should validate that you can remove attributes', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Remove Attributes
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.LeadEnrichment.isPresent().then(function (present){
            if (present) {
                clickLeadEnrichmentLink();
                element(by.id('saveLeadEnrichmentAttributesButton')).isDisplayed().then(function (displayed) {
                    if (displayed) {
                        leadEnrichment.testRemoveAttributes();
                    }
                });
            }
        });

        loginPage.logout();
    });

    it('should validate that you can save attributes', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Save Attributes
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.LeadEnrichment.isPresent().then(function (present){
            if (present) {
                clickLeadEnrichmentLink();
                element(by.id('saveLeadEnrichmentAttributesButton')).isDisplayed().then(function (displayed) {
                    if (displayed) {
                        leadEnrichment.testSaveAttributes();
                    }
                });
            }
        });

        loginPage.logout();
    });

    function clickLeadEnrichmentLink() {
        userDropdown.LeadEnrichment.click();
        browser.waitForAngular();
        browser.driver.sleep(15000);
    }
});