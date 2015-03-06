'use strict';

var ModelList = function() {
    this.getAnyModel = function() {    	   
     	return element(by.binding('createdDate'));      
    };  	

};

module.exports = new ModelList();