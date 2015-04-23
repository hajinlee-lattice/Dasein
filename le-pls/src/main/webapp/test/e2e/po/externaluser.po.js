'use strict';

var ExternalUser = function() {
    var loginPage = require('./login.po');
    var usermgmt = require('./usermgmt.po');
    var userdropdown = require('./userdropdown.po');

    this.testUserManagement = function() {
        describe('An external user', function(){
            it('should behave such and such', function () {
                loginPage.loginAsExternalUser();

                // can see manage users link
                userdropdown.toggleDropdown();
                usermgmt.assertManageUsersIsVisible(false);

                loginPage.logout();
            }, 60000);
        });
    };
};

module.exports = new ExternalUser();