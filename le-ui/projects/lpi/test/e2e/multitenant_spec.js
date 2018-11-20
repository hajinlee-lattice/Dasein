describe('when an admin tries to add a user that exists in another tenant', function() {

    var loginPage = require('./po/login.po');
    var userDropdown = require('./po/userdropdown.po');
    var userManagement = require('./po/usermgmt.po');

    var userInAnotherTenantAdded = false;
    var externalUserUsername1 = 'pls-external-user-tester-1@test.lattice-engines.ext';

    it('should be able to add the user', function() {
        loginPage.loginAsSuperAdmin();
        userDropdown.toggleDropdown();
        userDropdown.ManageUsersLink.click();
        userManagement.waitAndSleep();

        userManagement.AddNewUserLink.click();
        userManagement.waitAndSleep();
        userManagement.enterUserInfoAndClickOkay("Lattice", "Tester", externalUserUsername1);
        element(by.css('.modal-body')).getText().then(function(text) {
            expect(text).toContain('existing user');
        });
        element(by.buttonText("Yes")).click();
        browser.driver.sleep(5000);
        expect(element(by.css(".alert-success")).isPresent()).toBe(true);
        element(by.css('#add-user-btn-ok')).click();
        userManagement.waitAndSleep();

        isUserInAnotherTenantAddedWithAccessLevelUserAdded();
    });
    
    it('should be able to assert that user is indeed added', function() {
        expect(userInAnotherTenantAdded).toBe(true);
    });
    
    function isUserInAnotherTenantAddedWithAccessLevelUserAdded() {
        element.all(by.repeater('user in users')).then(function(userElementRows) {
            for (var i = 0; i < userElementRows.length; i++) {
                userElementRows[i].getText().then(function(text) {
                    if (text.indexOf(externalUserUsername1) > -1) {
                        userInAnotherTenantAdded = true;
                        expect(text).toContain('User');
                    }
                });
            }
        });
    }
});