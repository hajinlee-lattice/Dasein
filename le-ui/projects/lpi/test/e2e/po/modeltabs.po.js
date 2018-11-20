'use strict';

var ModelTabs = function() {
    this.getTabByIndex = function(index) {    	
        return element(by.repeater('tab in tabs').row(index));
    };  	

};

module.exports = new ModelTabs();