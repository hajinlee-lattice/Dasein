'use strict';

var InternalAdmin = function() {
    var loginPage = require('./login.po');
    var userManagement = require('./usermgmt.po');
    var userDropdown = require('./userdropdown.po');

    this.testUserManagement = function() {
        describe('An internal admin', function(){
            it('should sees the hidden link', function () {
                loginPage.loginAsInternalAdmin();
                userManagement.assertAdminLinkIsVisible(true);
                loginPage.logout();
            });

            it('should be able to add a new user, modify the user and then delete the user', function () {
                //==================================================
                // Login
                //==================================================
                loginPage.loginAsInternalAdmin();

                //==================================================
                // Select manage users tab
                //==================================================
                userDropdown.toggleDropdown();
                userDropdown.ManageUsersLink.click();
                var expectedNumOfCurrentUsers = element.all(by.repeater('user in users')).count();
                browser.driver.sleep(500);

                //==================================================
                // Open add new user modal and add the user
                //==================================================
                userManagement.AddNewUserLink.click();
                browser.waitForAngular();
                browser.driver.sleep(500);
                expect(element(by.css('#add-user-modal')).isPresent()).toBe(true);
                // verify number of roles in dropdown
                element(by.css('select')).click();
                expect(element.all(by.css('option')).count()).toBe(2);
                var testName = "0000" + userManagement.randomName(8);
                userManagement.createNewUser(testName);
                browser.driver.sleep(500);

                //==================================================
                // Open edit user modal and edit the user
                //==================================================
                element.all(by.css('.js-edit-user-link')).first().click();
                browser.driver.sleep(500);
                expect(element(by.css('#edit-user-modal')).isPresent()).toBeTruthy();
                // verify number of roles in dropdown
                element(by.css('select')).click();
                expect(element.all(by.css('option')).count()).toBeLessThan(3);
                element(by.css('#edit-user-btn-save')).click();
                browser.driver.sleep(500);
                element(by.css('#edit-user-btn-ok')).click();
                browser.driver.sleep(500);
                expect(element(by.css('#edit-user-modal')).isPresent()).toBeFalsy();

                //==================================================
                // Find the created user and delete him
                //==================================================
                element.all(by.css('.js-delete-user-link')).first().click();
                browser.driver.sleep(500);
                element(by.css('#delete-user-modal button.btn-primary')).click();
                browser.driver.sleep(500);
                var actualNumOfCurrentUsers = element.all(by.repeater('user in users')).count();
                expect(expectedNumOfCurrentUsers).toEqual(actualNumOfCurrentUsers);

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