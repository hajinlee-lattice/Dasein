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

    this.xpath = {
        PanelBody: "//div[@class='panel-body']"
    };

    this.getAddNewUserModal = function(){
        return element(by.xpath("//div[@data-ng-controller='AddUserController']"));
    };

    this.getAddNewUserButton = function() {
        return element(by.buttonText('ADD NEW USER'));
    };

    this.getAddNewUserSaveButton = function() {
        return element(by.buttonText('SAVE'));
    };

    this.getAddNewUserCancelButton = function() {
        return element(by.buttonText('CANCEL'));
    };

    this.getAddNewUserSuccessOKButton = function() {
        return element(by.buttonText('OK'));
    };

    this.getAddNewUserCrossSymbol = function() {
        return element(by.xpath("//button[@class='close' and @data-ng-click='cancelClick()']"));
    };

    this.getAddNewUserSuccessAlert = function() {
        return element(by.xpath("//div[@class='alert alert-success' and @data-ng-show='showAddUserSuccess']"));
    };

    this.getDeleteUsersButton = function() {
        return element(by.xpath("//button[@data-ng-click='deleteUsersClicked($event)']"));
    };

    this.getDeleteUserModal = function() {
        return element(by.xpath('//div[@data-ng-controller="DeleteUsersController"]'));
    };

};

module.exports = new UserManagement();