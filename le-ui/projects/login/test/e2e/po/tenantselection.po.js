'use strict';

var Tenants = function() {
    this.getTenantByIndex = function(index) {    	
        return element(by.repeater('tenant in tenantList').row(index));
    };

    function selectTenantByName(name) {
        element(by.css('select option[value="'+ name + '"]')).click();
    }

    this.selectTenantByIndex = function(index) {
        browser.wait(function(){
            return element(by.css('select#tenantSelectionInput')).isPresent();
        }, 10000, 'tenant list should appear with in 10 sec.');
        element(by.repeater('tenant in tenantList').row(index)).click();
    };

    this.selectTenantByName = function(name) {
        browser.wait(function(){
            return element(by.css('select#tenantSelectionInput')).isPresent();
        }, 10000, 'tenant list should appear with in 10 sec.');
        selectTenantByName(name);
    };

    this.tenantSelectionIsPresent = function(){
        return element(by.css('select#tenantSelectionInput')).isPresent();
    };


};

module.exports = new Tenants();