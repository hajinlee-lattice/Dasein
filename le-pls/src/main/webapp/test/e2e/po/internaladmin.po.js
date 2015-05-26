'use strict';

var InternalAdmin = function() {
    var loginPage = require('./login.po');
    var usermgmt = require('./usermgmt.po');
    var userdropdown = require('./userdropdown.po');

    this.testUserManagement = function() {
        describe('An internal admin', function(){
            it('should be able to see the manage user page', function () {
                loginPage.loginAsInternalAdmin();

                // can see manage users link
                userdropdown.toggleDropdown();
                usermgmt.assertManageUsersIsVisible(true);

                loginPage.logout();
            }, 60000);
        });
    };

};

module.exports = new InternalAdmin();