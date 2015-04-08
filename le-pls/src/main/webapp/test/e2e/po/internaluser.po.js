'use strict';

var InternalUser = function() {
    var loginPage = require('./login.po');
    var logoutPage = require('./logout.po');
    var userDropdown = require('./userdropdown.po');
    var usermgmt = require('./usermgmt.po');
    var params = browser.params;

    this.testUserManagement = function() {
        it('should verify user management is invisible to external users', function () {
            loginPage.loginAsNonAdminToTenant(params.tenantIndex);

            userDropdown.toggleDropdown();
            usermgmt.testManageUserLink();
            userDropdown.toggleDropdown();

            logoutPage.logoutAsNonAdmin();
        }, 60000);
    };

};

module.exports = new InternalUser();