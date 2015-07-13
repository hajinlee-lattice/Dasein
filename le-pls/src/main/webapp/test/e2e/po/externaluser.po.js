'use strict';

var ExternalUser = function() {
    var loginPage = require('./login.po');
    var userManagement = require('./usermgmt.po');
    var userdropdown = require('./userdropdown.po');

    this.testUserManagement = function() {
        describe('An external user', function(){
            it('should not be able to see the hidden link', function () {
                loginPage.loginAsExternalUser();
                userManagement.assertAdminLinkIsVisible(false);
                loginPage.logout();
            });

            it('should not be able to see the manage user page', function () {
                loginPage.loginAsExternalUser();

                // cannot see manage users link
                userdropdown.toggleDropdown();
                userManagement.assertManageUsersIsVisible(false);

                loginPage.logout();
            });
        });
    };
};

module.exports = new ExternalUser();