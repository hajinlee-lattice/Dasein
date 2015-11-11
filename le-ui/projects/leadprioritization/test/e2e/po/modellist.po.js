'use strict';

var ModelList = function() {
    this.getAnyModel = function() {
        return element.all(by.binding('createdDate')).get(0);
    };

    this.getFirstModelTile = function() {
        return element.all(by.xpath(this.xpath.ModelTileWidget)).first();
    };

    this.xpath = {
        ModelTileWidget: '//div[@data-model-list-tile-widget]',
        ModelNameInput:  '//input[@data-ng-model="data.name"]',
        SaveModelName : '//button[@data-ng-click="submit($event)"]',
        CancelEditModelName : '//button[@data-ng-click="cancel($event)"]',
        EditModelNameError : '//label[@data-ng-show="showNameEditError"]',
        DeleteModelLink: '//a[@data-ng-click="deleteModelClick($event)"]'
    };

};

module.exports = new ModelList();