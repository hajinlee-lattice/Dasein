'use strict';

var InternalUser = function() {
    var loginPage = require('./login.po');
    var userManagement = require('./usermgmt.po');
    var userDropdown = require('./userdropdown.po');

    this.testUserManagement = function() {
        describe('An internal user', function(){
            it('should see links accordingly', function () {
                loginPage.loginAsInternalUser();

                userDropdown.toggleDropdown();
                userManagement.canSeeManageUsersLink(false);
                userManagement.canSeeSystemSetupLink(false);
                userManagement.canSeeActivateModelLink(false);
                userManagement.canSeeModelCreationHistoryLink(true);
                userManagement.canSeeSetupLink(false);
                userDropdown.toggleDropdown();

                userManagement.canSeeHiddenAdminLink(true);

                loginPage.logout();
            });
        });
    };

};

module.exports = new InternalUser();