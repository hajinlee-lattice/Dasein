'use strict';

var ModelList = function() {
    this.getAnyModel = function() {    	   
     	return element(by.binding('createdDate'));      
    };

    this.xpath = {
        ModelTileWidget: '//div[@data-model-list-tile-widget]',
        ModelNameInput:  '//input[@data-ng-model="data.name"]',
        SaveModelName : '//button[@data-ng-click="submit($event)"]',
        CancelEditModelName : '//button[@data-ng-click="cancel($event)"]',
        EditModelNameError : '//label[@data-ng-show="showNameEditError"]'
    };

};

module.exports = new ModelList();