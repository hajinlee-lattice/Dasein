'use strict';

var Tenants = function() {
    this.getTenantByIndex = function(index) {    	
        return element(by.repeater('tenant in tenantList').row(index));
    };  	

    this.getTenantByName = function(name) {    	
        return element(by.cssContainingText('option', name));
    };

    this.selectTenantByIndex = function(index) {
        browser.driver.wait(function(){
            return element(by.css('select#tenantSelectionInput')).isDisplayed();
        }, 10000, 'tenant list should appear with in 10 sec.');
        element(by.repeater('tenant in tenantList').row(index)).click();
        browser.driver.wait(function(){
            return element(by.css('div.page-title')).isDisplayed();
        }, 10000, 'model list page should appear with in 10 sec.');
    };

};

module.exports = new Tenants();