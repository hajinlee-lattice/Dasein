'use strict';

var UserManagement = function() {
    this.getPanelBody = function() {
        return browser.driver.findElement(By.xpath("//div[@class='panel-body']"));
    };

    this.getAddNewUserModal = function(){
        return browser.driver.findElement(By.xpath("//div[@data-ng-controller='AddUserController']"));
    };

    this.getAddNewUserButton = function() {
        return browser.driver.findElement(By.xpath("//button[@data-ng-click='addUserClicked($event)']"));
    };

    this.getAddNewUserSaveButton = function() {
        return browser.driver.findElement(By.xpath("//button[@data-ng-click='addUserClick($event)' and @data-ng-hide='showAddUserSuccess']"));
    };

    this.getAddNewUserCancelButton = function() {
        return browser.driver.findElement(By.xpath("//button[@data-ng-hide='showAddUserSuccess' and @data-ng-click='cancelClick()']"));
    };

    this.getAddNewUserSuccessOKButton = function() {
        return browser.driver.findElement(By.xpath("//button[@data-ng-show='showAddUserSuccess']"));
    };

    this.getAddNewUserCrossSymbol = function() {
        return browser.driver.findElement(By.xpath("//button[@class='close' and @data-ng-click='cancelClick()']"));
    };

    this.getAddNewUserSuccessAlert = function() {
        return browser.driver.findElement(By.xpath("//div[@class='alert alert-success' and @data-ng-show='showAddUserSuccess']"));
    };

    this.getUserLink = function(name) {
        return element(by.linkText(name));
    };
};

module.exports = new UserManagement();