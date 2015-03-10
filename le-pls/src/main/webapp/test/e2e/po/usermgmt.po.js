'use strict';

var UserManagement = function() {
    this.getPanelBody = function() {
        return browser.driver.findElement(By.xpath("//div[@class='panel-body']"));
    }

    this.getAddNewUserButton = function() {
        return browser.driver.findElement(By.xpath("//button[@data-ng-click='addUserClicked($event)']"));
    }

    this.getAddNewUserCancelButton = function() {
        return browser.driver.findElement(By.xpath("//button[@data-ng-hide='showAddUserSuccess' and @data-ng-click='cancelClick()']"));
    }

    this.getUserLink = function(name) {
        return element(by.linkText(name));
    };
};

module.exports = new UserManagement();