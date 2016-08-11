'use strict';

var ExternalUser = function() {
    var loginPage = require('./login.po');
    var userManagement = require('./usermgmt.po');
    var userDropdown = require('./userdropdown.po');

    this.testUserManagement = function() {
        describe('An external user', function(){
            it('should see links accordingly', function () {
                loginPage.loginAsExternalUser();
                userManagement.canSeeEnrichmentLink(true);
                userManagement.canSeeManageUsersLink(false);
                userManagement.canSeeJobsLink(true);
                userManagement.canSeeMarketoSettingsLink(false);

                userManagement.canSeeModelCreationHistoryLink(false);
                userManagement.canSeeCreateModelLink(true);

                userManagement.clickFirstModel();
                userManagement.canSeeAttributesLink(true);
                userManagement.canSeePerformanceLink(true);
                userManagement.canSeeSampleLeadsLink(true);
                userManagement.canSeeModelSummaryLink(true);
                userManagement.canSeeScoringLink(true);
                userManagement.canSeeRefineAndCloneLink(true);
                
                loginPage.logout();
            });
        });
    };
};

module.exports = new ExternalUser();