'use strict';

var SuperAdmin = function() {
    var loginPage = require('./login.po');
    var userManagement = require('./usermgmt.po');
    var userDropdown = require('./userdropdown.po');

    this.testUserManagement = function() {
        describe('A super admin', function(){
            it('should be able to add a new user, modify the user and then delete the user', function () {
                //==================================================
                // Login
                //==================================================
                loginPage.loginAsSuperAdmin();

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
                expect(element.all(by.css('option')).count()).toBe(3);
                var testName = "0000" + userManagement.randomName(8);
                userManagement.createNewUser(testName);
                browser.driver.sleep(500);

                //==================================================
                // Open edit user modal and edit the user
                //==================================================
                element.all(by.css('.js-edit-user-link')).first().click();
                browser.driver.sleep(500);
                expect(element(by.css('#edit-user-modal')).isPresent()).toBe(true);
                // verify number of roles in dropdown
                element(by.css('select')).click();
                expect(element.all(by.css('option')).count()).toBeLessThan(4);
                element(by.css('#edit-user-btn-save')).click();
                browser.driver.sleep(500);
                element(by.css('#edit-user-btn-ok')).click();
                browser.driver.sleep(500);
                expect(element(by.css('#edit-user-modal')).isPresent()).toBe(false);

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
                var expectedNumOfCurrentUsers = element.all(by.repeater('user in users')).count();
                browser.driver.sleep(500);

                //==================================================
                // Open add new user modal and then click cancel
                //==================================================
                userManagement.AddNewUserLink.click();
                expect(element(by.css('#add-user-modal')).isPresent()).toBe(true);
                element(by.css('#add-user-btn-cancel')).click();
                browser.driver.sleep(500);
                var actualNumOfCurrentUsers = element.all(by.repeater('user in users')).count();
                expect(expectedNumOfCurrentUsers).toEqual(actualNumOfCurrentUsers);


                //==================================================
                // Logout
                //==================================================
                loginPage.logout();
                browser.driver.sleep(1000);
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
                browser.driver.sleep(500);
                expect(element(by.css('#edit-user-modal')).isDisplayed()).toBe(true);
                element(by.css('#edit-user-btn-cancel')).click();
                browser.driver.sleep(500);
                expect(element(by.css('#edit-user-modal')).isPresent()).toBe(false);


                //==================================================
                // Logout
                //==================================================
                loginPage.logout();
                browser.driver.sleep(1000);
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
                var expectedNumOfCurrentUsers = element.all(by.repeater('user in users')).count();
                browser.driver.sleep(500);

                //==================================================
                // Find the first user, click the delete icon and then click No
                //==================================================
                element.all(by.css('.js-delete-user-link')).first().click();
                browser.driver.sleep(500);
                element(by.css('#delete-user-modal button.btn-secondary')).click();
                browser.driver.sleep(500);
                var actualNumOfCurrentUsers = element.all(by.repeater('user in users')).count();
                expect(expectedNumOfCurrentUsers).toEqual(actualNumOfCurrentUsers);
                expect(element(by.css('#delete-user-modal')).isPresent()).toBe(false);


                //==================================================
                // Logout
                //==================================================
                loginPage.logout();
                browser.driver.sleep(1000);
            });

        });
    };

};

module.exports = new SuperAdmin();