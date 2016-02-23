'use strict';

var LeadEnrichment = function() {
    var helper = require('./helper.po');

    this.testClickAllAttributes = function () {
        var allAttributesLink = element(by.id('showAllAttributesButton'));
        helper.elementExists(allAttributesLink, true);
        allAttributesLink.click();
        sleep(1000);
        var backLeadEnrichment = element(by.id('backLeadEnrichmentButton'));
        helper.elementExists(backLeadEnrichment, true);
        backLeadEnrichment.click();
        sleep(1000);
        element(by.id('showAllAttributesButton')).isDisplayed().then(function (displayed){
            expect(displayed).toBe(true);
        });
    };

    this.testAddAttributes = function () {
        var availableAttributesList = element(by.id("availableAttributesList"));
        var availableAttributeItems = availableAttributesList.all(by.tagName('li'));
        availableAttributeItems.count().then(function (count) {
            if (count > 0) {
                availableAttributeItems.get(0).click();
                element(by.id('addLeadEnrichmentAttribute')).click();
                sleep(500);

                availableAttributesList.all(by.tagName('li')).count().then(function (newCount) {
                    expect(newCount).toEqual(count - 1);
                });
            }
        });
    };

    this.testRemoveAttributes = function () {
        var selectedAttributesList = element(by.id('selectedAttributesList'));
        var selectedAttributeItems = selectedAttributesList.all(by.tagName('li'));
        selectedAttributeItems.count().then(function (count) {
            if (count >= 20) {
                element(by.id('selectedAttributesLabel')).getAttribute('class').then(function (classes) {
                    expect(classes.split(' ').indexOf('warning') > -1).toBe(true);
                });
            }

            if (count > 0) {
                selectedAttributeItems.get(0).click();
                element(by.id('reomveLeadEnrichmentAttribute')).click();
                sleep(500);

                selectedAttributesList.all(by.tagName('li')).count().then(function (newCount) {
                    expect(newCount).toEqual(count - 1);
                });
            }
        });
    };

    this.testSaveAttributes = function () {
        var saveButton = element(by.id('saveLeadEnrichmentAttributesButton'));
        saveButton.click();
        sleep(500);
        var noBotton = element(by.id('save-attributes-no'));
        helper.elementExists(noBotton, true);
        noBotton.click();
        sleep(500);
        helper.elementExists(element(by.id('save-attributes-no')), false);

        saveButton.click();
        sleep(500);
        var yesBotton = element(by.id('save-attributes-yes'));
        helper.elementExists(yesBotton, true);
        yesBotton.click();
        sleep(10000);
        element(by.id('saveAttributesConfirm')).isPresent().then(function (confirmPresent) {
            if (confirmPresent) {
                element(by.id('verifyFieldsError')).isDisplayed().then(function (errorDisplayed){
                    expect(errorDisplayed).toBe(false);
                    if (!errorDisplayed) {
                        element(by.id('invalidFields')).isDisplayed().then(function (invalidFieldsDisplayed){
                            if (invalidFieldsDisplayed) {
                                noBotton.click();
                                sleep(500);
                            } else {
                                sleep(24000);
                                element(by.id('backLeadEnrichmentButton')).isDisplayed().then(function (displayed){
                                    expect(displayed).toBe(true);
                                });
                            }
                        });
                    }
                });
            } else {
                sleep(24000);
                element(by.id('backLeadEnrichmentButton')).isDisplayed().then(function (displayed){
                    expect(displayed).toBe(true);
                });
            }
        });
    };

    function sleep(ms) {
        browser.waitForAngular();
        browser.driver.sleep(ms);
    }
};

module.exports = new LeadEnrichment();