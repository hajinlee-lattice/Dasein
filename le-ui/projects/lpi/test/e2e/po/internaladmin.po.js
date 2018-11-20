'use strict';

var InternalAdmin = function() {
    var loginPage = require('./login.po');
    var siderbar = require('./siderbar.po');
    var userManagement = require('./usermgmt.po');

    this.testUserManagement = function() {
        describe('An internal admin', function(){
            it('should see links accordingly', function () {
                loginPage.loginAsInternalAdmin();

                userManagement.canSeePredictionModelsLink(true);
                userManagement.canSeeCreateModelLink(true);
                userManagement.canSeeManageUsersLink(true);
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

            it('should see edit/delete links for all users', function () {
                loginPage.loginAsInternalAdmin();
                siderbar.ManageUsersLink.click();
                browser.driver.sleep(500);

                userManagement.canSeeUser("pls-super-admin-tester", true);
                userManagement.canSeeUser("pls-internal-admin-tester", true);
                userManagement.canSeeUser("pls-internal-user-tester", true);
                userManagement.canSeeUser("pls-external-admin-tester", true);
                userManagement.canSeeUser("pls-external-user-tester", true);

                userManagement.canEditAndDeleteUser("pls-super-admin-tester", false);
                userManagement.canEditAndDeleteUser("pls-internal-admin-tester", true);
                userManagement.canEditAndDeleteUser("pls-internal-user-tester", true);
                userManagement.canEditAndDeleteUser("pls-external-admin-tester", true);
                userManagement.canEditAndDeleteUser("pls-external-user-tester", true);

                loginPage.logout();
            });

            var originalNumUsers;
            it('should be able to add a new user', function () {
                //==================================================
                // Login
                //==================================================
                loginPage.loginAsInternalAdmin();

                //==================================================
                // Select manage users tab
                //==================================================
                siderbar.ManageUsersLink.click();
                browser.driver.sleep(500);
                element.all(by.repeater('user in users')).count().then(function(numUsers) {
                    originalNumUsers = numUsers;
                    browser.driver.sleep(500);

                    //==================================================
                    // Open add new user modal and add the user
                    //==================================================
                    userManagement.AddNewUserLink.click();
                    userManagement.waitAndSleep();
                    expect(element(by.css('#add-user-modal')).isPresent()).toBe(true);
                    // verify number of roles in dropdown
                    element(by.css('select')).click();
                    expect(element.all(by.css('option')).count()).toBe(2);
                    userManagement.enterUserInfoAndClickOkay(userManagement.tempUserFirstName, userManagement.tempUserLastName,
                            userManagement.tempUserEmail);
                    expect(element(by.css('#add-user-btn-ok')).isPresent()).toBe(true);
                    element(by.css('#add-user-btn-ok')).click();
                    userManagement.waitAndSleep();

                    var actualNumOfCurrentUsers = element.all(by.repeater('user in users')).count();
                    expect(originalNumUsers+1).toEqual(actualNumOfCurrentUsers);
                    browser.driver.sleep(5000);
                });
            });

            it('should be able to modify the user and then delete the user', function() {
                //==================================================
                // Open edit user modal and edit the user
                //==================================================
                element.all(by.css('.js-edit-user-link')).first().click();
                userManagement.waitAndSleep();
                expect(element(by.css('#edit-user-modal')).isPresent()).toBeTruthy();
                // verify number of roles in dropdown
                element(by.css('select')).click();
                expect(element.all(by.css('option')).count()).toBeLessThan(3);
                element(by.css('#edit-user-btn-save')).click();
                userManagement.waitAndSleep();
                element(by.css('#edit-user-btn-ok')).click();
                userManagement.waitAndSleep();
                expect(element(by.css('#edit-user-modal')).isPresent()).toBeFalsy();

                //==================================================
                // Find the created user and delete him
                //==================================================
                element.all(by.css('.js-delete-user-link')).first().click();
                userManagement.waitAndSleep();
                element(by.css('#delete-user-modal button.btn-primary')).click();
                userManagement.waitAndSleep();
                var actualNumOfCurrentUsers = element.all(by.repeater('user in users')).count();
                expect(originalNumUsers).toEqual(actualNumOfCurrentUsers);

                //==================================================
                // Logout
                //==================================================
                loginPage.logout();
                browser.driver.sleep(1000);
            });
        });
    };
};

module.exports = new InternalAdmin();