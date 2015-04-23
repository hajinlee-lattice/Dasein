'use strict';

var InternalUser = function() {
    var loginPage = require('./login.po');
    var usermgmt = require('./usermgmt.po');
    var userdropdown = require('./userdropdown.po');

    this.testUserManagement = function() {
        describe('An internal user', function(){
            it('should behave such and such', function () {
                loginPage.loginAsInternalUser();

                // can see manage users link
                userdropdown.toggleDropdown();
                usermgmt.assertManageUsersIsVisible(false);

                loginPage.logout();
            }, 60000);
        });
    };

};

module.exports = new InternalUser();