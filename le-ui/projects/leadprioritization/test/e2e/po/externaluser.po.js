'use strict';

var ExternalUser = function() {
    var loginPage = require('./login.po');
    var userManagement = require('./usermgmt.po');
    var userDropdown = require('./userdropdown.po');

    this.testUserManagement = function() {
        describe('An external user', function(){
            it('should see links accordingly', function () {
                loginPage.loginAsExternalUser();

                userManagement.canSeePredictionModelsLink(true);
                userManagement.canSeeCreateModelLink(true);
                userManagement.canSeeManageUsersLink(false);
                userManagement.canSeeModelCreationHistoryLink(false);
                userManagement.canSeeJobsLink(true);
                userManagement.canSeeMarketoSettingsLink(true);
                userManagement.clickFirstModel();
                userManagement.canSeeAttributesLink(true);
                userManagement.canSeePerformanceLink(true);
                userManagement.canSeeSampleLeadsLink(true);
                userManagement.canSeeModelSummaryLink(false);
                userManagement.canSeeScoringLink(true);
                userManagement.canSeeRefineAndCloneLink(true);

                loginPage.logout();
            });
        });
    };
};

module.exports = new ExternalUser();