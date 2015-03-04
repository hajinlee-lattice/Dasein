'use strict';

var Tenants = function() {
    this.getTenantByIndex = function(index) {    	
        return element(by.repeater('tenant in tenantList').row(index));
    };  	

    this.getTenantByName = function(name) {    	
        return element(by.cssContainingText('option', name));
    };  	

};

module.exports = new Tenants();