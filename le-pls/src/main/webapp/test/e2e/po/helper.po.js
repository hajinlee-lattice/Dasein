'use strict';

var Helper = function() {

    this.elementExists = function (elem, expected){
        if (expected) {
            expect(elem.getWebElement().isDisplayed()).toBe(true);
        } else {
            expect(elem.getWebElement().isDisplayed()).toBe(false);
        }
    };

};

module.exports = new Helper();