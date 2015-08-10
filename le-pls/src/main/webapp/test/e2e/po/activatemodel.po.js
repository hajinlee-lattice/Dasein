'use strict';

var ActivateModel = function() {
    this.clickAddSegment = function() {          
        element(by.css('.btn-primary')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    };
    
    this.addNewSegment = function(name) {          
        element(by.model('newSegment.Name')).sendKeys(name);
        browser.waitForAngular();
        browser.driver.sleep(1000);
        element(by.css('.js-add-segment')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    };

};

module.exports = new ActivateModel();