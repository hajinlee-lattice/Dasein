'use strict';

var ExternalUser = function() {
    var loginPage = require('./login.po');
    var userManagement = require('./usermgmt.po');
    var userDropdown = require('./userdropdown.po');

    this.testUserManagement = function() {
        describe('An external user', function(){
            it('should see links accordingly', function () {
                loginPage.loginAsExternalUser();

                userDropdown.toggleDropdown();
                userManagement.canSeeManageUsersLink(false);
                userManagement.canSeeSystemSetupLink(false);
                userManagement.canSeeActivateModelLink(false);
                userManagement.canSeeModelCreationHistoryLink(false);
                userManagement.canSeeSetupLink(false);
                userDropdown.toggleDropdown();

                userManagement.canSeeHiddenAdminLink(false);

                loginPage.logout();
            });
        });
    };
};

module.exports = new ExternalUser();