'use strict';

var LeadEnrichment = function() {
    var helper = require('./helper.po');

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

    function sleep(ms) {
        browser.waitForAngular();
        browser.driver.sleep(ms);
    }
};

module.exports = new LeadEnrichment();