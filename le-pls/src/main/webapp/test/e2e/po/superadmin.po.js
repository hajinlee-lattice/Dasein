'use strict';

var SuperAdmin = function() {
    var loginPage = require('./login.po');
    var userManagement = require('./usermgmt.po');
    var userDropdown = require('./userdropdown.po');

    this.testUserManagement = function() {
        describe('A super admin', function(){
            it('should see links accordingly', function () {
                loginPage.loginAsSuperAdmin();

                userDropdown.toggleDropdown();
                userManagement.canSeeManageUsersLink(true);
                userManagement.canSeeSystemSetupLink(true);
                userManagement.canSeeActivateModelLink(true);
                userManagement.canSeeModelCreationHistoryLink(true);
                userManagement.canSeeSetupLink(false);
                userDropdown.toggleDropdown();

                userManagement.canSeeHiddenAdminLink(true);

                loginPage.logout();
            });

            it('should see edit/delete links for all users', function () {
                loginPage.loginAsSuperAdmin();
                userDropdown.toggleDropdown();
                userDropdown.ManageUsersLink.click();

                userManagement.canSeeUser("pls-super-admin-tester", true);
                userManagement.canSeeUser("pls-internal-admin-tester", true);
                userManagement.canSeeUser("pls-internal-user-tester", true);
                userManagement.canSeeUser("pls-external-admin-tester", true);
                userManagement.canSeeUser("pls-external-user-tester", true);

                userManagement.canEditAndDeleteUser("pls-super-admin-tester", true);
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
                loginPage.loginAsSuperAdmin();

                //==================================================
                // Select manage users tab
                //==================================================
                userDropdown.toggleDropdown();
                userDropdown.ManageUsersLink.click();
                element.all(by.repeater('user in users')).count().then(function(userCount){
                    browser.driver.sleep(500);
                    originalNumUsers = userCount;
                    //==================================================
                    // Open add new user modal and add the user
                    //==================================================
                    userManagement.AddNewUserLink.click();
                    userManagement.waitAndSleep();
                    expect(element(by.css('#add-user-modal')).isPresent()).toBe(true);
                    // verify number of roles in dropdown
                    element(by.css('select')).click();
                    expect(element.all(by.css('option')).count()).toBe(3);
                    userManagement.enterUserInfoAndClickOkay(userManagement.tempUserFirstName, userManagement.tempUserLastName,
                            userManagement.tempUserEmail);
                    expect(element(by.css(".alert-success")).isPresent()).toBe(true);
                    element(by.css('#add-user-btn-ok')).click();
                    userManagement.waitAndSleep();

                    var actualNumOfCurrentUsers = element.all(by.repeater('user in users')).count();
                    expect(originalNumUsers+1).toEqual(actualNumOfCurrentUsers);
                    browser.driver.sleep(5000);
                })
            });

            it("should be able to modify the new user and then delete it", function () {
                //==================================================
                // Open edit user modal and edit the user
                //==================================================
                element.all(by.css('.js-edit-user-link')).first().click();
                userManagement.waitAndSleep();
                var userModal = element(by.css('#edit-user-modal'));
                expect(userModal.isPresent()).toBe(true);
                // verify number of roles in dropdown
                element(by.css('select')).click();
                expect(element.all(by.css('option')).count()).toBeLessThan(4);
                element(by.css('#edit-user-btn-save')).click();
                userManagement.waitAndSleep();
                element(by.css('#edit-user-btn-ok')).click();
                userManagement.waitAndSleep();
                expect(userModal.isPresent()).toBe(false);

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
            });

            it('should be able to cancel adding a new user', function () {
                //==================================================
                // Login
                //==================================================
                loginPage.loginAsSuperAdmin();

                //==================================================
                // Select manage users tab
                //==================================================
                userDropdown.toggleDropdown();
                userDropdown.ManageUsersLink.click();
                browser.driver.sleep(500);

                //==================================================
                // Open add new user modal and then click cancel
                //==================================================
                userManagement.AddNewUserLink.click();
                userManagement.waitAndSleep();
                expect(element(by.css('#add-user-modal')).isPresent()).toBe(true);
                element(by.css('#add-user-btn-cancel')).click();
                userManagement.waitAndSleep();
                var actualNumOfCurrentUsers = element.all(by.repeater('user in users')).count();
                expect(originalNumUsers).toEqual(actualNumOfCurrentUsers);


                //==================================================
                // Logout
                //==================================================
                loginPage.logout();
            });

            it('should be able to cancel editing a user', function () {
                //==================================================
                // Login
                //==================================================
                loginPage.loginAsSuperAdmin();

                //==================================================
                // Select manage users tab
                //==================================================
                userDropdown.toggleDropdown();
                userDropdown.ManageUsersLink.click();
                browser.driver.sleep(500);

                //==================================================
                // Open edit user modal and cancel editing the user
                //==================================================
                element.all(by.css('.js-edit-user-link')).first().click();
                userManagement.waitAndSleep();
                expect(element(by.css('#edit-user-modal')).isDisplayed()).toBe(true);
                element(by.css('#edit-user-btn-cancel')).click();
                userManagement.waitAndSleep();
                expect(element(by.css('#edit-user-modal')).isPresent()).toBe(false);


                //==================================================
                // Logout
                //==================================================
                loginPage.logout();
            });

            it('should be able to cancel deleting a user', function () {
                //==================================================
                // Login
                //==================================================
                loginPage.loginAsSuperAdmin();

                //==================================================
                // Select manage users tab
                //==================================================
                userDropdown.toggleDropdown();
                userDropdown.ManageUsersLink.click();
                browser.driver.sleep(500);

                //==================================================
                // Find the first user, click the delete icon and then click No
                //==================================================
                element.all(by.css('.js-delete-user-link')).first().click();
                userManagement.waitAndSleep();
                element(by.css('#delete-user-modal button.btn-secondary')).click();
                userManagement.waitAndSleep();
                var actualNumOfCurrentUsers = element.all(by.repeater('user in users')).count();
                expect(originalNumUsers).toEqual(actualNumOfCurrentUsers);
                expect(element(by.css('#delete-user-modal')).isPresent()).toBe(false);


                //==================================================
                // Logout
                //==================================================
                loginPage.logout();
            });
        });
    };

};

module.exports = new SuperAdmin();