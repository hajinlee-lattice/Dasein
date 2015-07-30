'use strict';

var ExternalAdmin = function() {
    var loginPage = require('./login.po');
    var userManagement = require('./usermgmt.po');
    var userDropdown = require('./userdropdown.po');

    this.testUserManagement = function() {
        describe('An external admin', function(){
            it('should not see the hidden link', function () {
                loginPage.loginAsExternalAdmin();
                userManagement.assertAdminLinkIsVisible(false);
                loginPage.logout();
            });

            var originalNumUsers;
            it('should be able to add a new user', function () {
                //==================================================
                // Login
                //==================================================
                loginPage.loginAsExternalAdmin();

                //==================================================
                // Select manage users tab
                //==================================================
                userDropdown.toggleDropdown();
                userDropdown.ManageUsersLink.click();
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
                    expect(element.all(by.css('option')).count()).toBe(1);
                    userManagement.enterUserInfoAndClickOkay(userManagement.tempUserFirstName, userManagement.tempUserLastName,
                            userManagement.tempUserEmail);
                    expect(element(by.css(".alert-success")).isPresent()).toBe(true);
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

module.exports = new ExternalAdmin();