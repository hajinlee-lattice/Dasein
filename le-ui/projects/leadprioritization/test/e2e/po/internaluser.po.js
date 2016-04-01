'use strict';

var InternalUser = function() {
    var loginPage = require('./login.po');
    var userManagement = require('./usermgmt.po');

    this.testUserManagement = function() {
        describe('An internal user', function(){
            it('should see links accordingly', function () {
                loginPage.loginAsInternalUser();

                userManagement.canSeePredictionModelsLink(true);
                userManagement.canSeeCreateModelLink(true);
                userManagement.canSeeManageUsersLink(false);
                userManagement.canSeeModelCreationHistoryLink(true);
                userManagement.canSeeJobsLink(true);
                userManagement.canSeeMarketoSettingsLink(true);
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

module.exports = new InternalUser();