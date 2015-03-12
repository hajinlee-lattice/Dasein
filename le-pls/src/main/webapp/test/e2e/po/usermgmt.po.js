'use strict';

var UserManagement = function() {
    this.getPanelBody = function() {
        return browser.driver.findElement(By.xpath("//div[@class='panel-body']"));
    };

    this.selectUser = function(username) {
        function clickCheckbox(row) {
            row.all(by.css('td')).getText().then(function (text) {
                if (text[3] === username) {
                    //console.log(text[3]);
                    row.element(by.css('input')).click();
                }
            });
        }
        element.all(by.repeater('user in data')).then(function(userRows){
            for (var i in userRows) {
                clickCheckbox(userRows[i]);
            }
        });
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

    this.getDeleteUsersButton = function() {
        return browser.driver.findElement(By.xpath("//button[@data-ng-click='deleteUsersClicked($event)']"));
    };

    this.getDeleteUserModal = function() {
        return element(by.xpath('//div[@data-ng-controller="DeleteUsersController"]'));
    };

};

module.exports = new UserManagement();