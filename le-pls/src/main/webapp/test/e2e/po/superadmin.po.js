'use strict';

var SuperAdmin = function() {
    var loginPage = require('./login.po');
    var usermgmt = require('./usermgmt.po');
    var userdropdown = require('./userdropdown.po');

    this.testUserManagement = function() {
        describe('A super admin', function(){
            it('should behave such and such', function () {
                loginPage.loginAsSuperAdmin();

                // can see manage users link
                userdropdown.toggleDropdown();
                usermgmt.assertManageUsersIsVisible(true);

                loginPage.logout();
            }, 60000);
        });
    };

};

module.exports = new SuperAdmin();