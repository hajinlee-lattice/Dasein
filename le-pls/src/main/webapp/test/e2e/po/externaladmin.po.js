'use strict';

var ExternalAdmin = function() {
    var loginPage = require('./login.po');
    var usermgmt = require('./usermgmt.po');
    var userdropdown = require('./userdropdown.po');

    this.testUserManagement = function() {
        describe('An external admin', function(){
            it('should behave such and such', function () {
                loginPage.loginAsExternalAdmin();

                // can see manage users link
                userdropdown.toggleDropdown();
                usermgmt.assertManageUsersIsVisible(true);

                loginPage.logout();
            }, 60000);
        });
    };

};

module.exports = new ExternalAdmin();