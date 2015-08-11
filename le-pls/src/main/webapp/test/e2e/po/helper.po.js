'use strict';

var Helper = function() {

    var fs = require('fs');

    this.elementExists = function (elem, expected, message){
        if (expected) {
            browser.driver.wait(function() {
                return elem.getWebElement().isDisplayed();
            }, 3000, message || "the web element should appear within 3 seconds: " + elem);
        } else {
            elem.isPresent().then(function(present) {
               if (present) {
                   expect(elem.getWebElement().isDisplayed()).toBe(false, message || "the web element should not be displayed.");
               }
            });
        }
    };

    this.fileExists = function(filename) {
        browser.driver.wait(function() {
            return fs.existsSync(browser.params.downloadRoot + filename);
        }, 3000, filename + " should be downloaded within 3 seconds.");
    };

    this.removeFile = function(filename) {
        if (fs.existsSync(browser.params.downloadRoot + filename)) {
            fs.unlinkSync(browser.params.downloadRoot + filename);
        }
    };

};

module.exports = new Helper();