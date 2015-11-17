'use strict';

var ModelDetails = function() {
    this.toggleDetails = function() {    	   
        element(by.css('.toggleDetails')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    };  	

};

module.exports = new ModelDetails();