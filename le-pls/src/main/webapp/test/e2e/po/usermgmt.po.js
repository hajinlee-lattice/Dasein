'use strict';

var UserManagement = function() {
    this.addNewUser = element(by.text('Add New User'));

    this.getUserLink = function(name) {
        return element(by.linkText(name));
    };
};

module.exports = new UserManagement();