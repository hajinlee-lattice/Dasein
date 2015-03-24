describe('user management', function() {

    var params = browser.params;

    var loginPage = require('./po/login.po');
    var logoutPage = require('./po/logout.po');
    var tenants = require('./po/tenantselection.po');
    var userDropdown = require('./po/userdropdown.po');
    var userManagement = require('./po/usermgmt.po');
    var numOfUsers = 0;
    var newUserEmail;
    var newUserPassword;

    function randomName()
    {
        var text = "";
        var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        for( var i=0; i < 5; i++ )
            text += possible.charAt(Math.floor(Math.random() * possible.length));

        return text;
    }

    it('should verify user management is invisible to non-admin users', function () {
        loginPage.loginAsNonAdmin();

        tenants.getTenantByIndex(params.tenantIndex).click();
        browser.waitForAngular();
        browser.driver.sleep(2000);

        // check existence of Manage Users link
        userDropdown.getUserLink(params.nonAdminDisplayName).click().then(function(){
            expect(element(by.linkText('Manage Users')).isPresent()).toBe(false);
        });
        browser.waitForAngular();
        browser.driver.sleep(2000);

        userDropdown.getUserLink(params.nonAdminDisplayName).click().then(function(){
            browser.driver.sleep(500);
            logoutPage.logoutAsNonAdmin();
        });
    }, 60000);

    it('should login as an admin users', function () {
        loginPage.loginAsAdmin();

        // choose tenant
        tenants.getTenantByIndex(params.tenantIndex).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    }, 50000);

    it('should see user management link', function () {
        // check existence of Manage Users link
        userDropdown.getUserLink(params.adminDisplayName).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(element(by.linkText('Manage Users')).isDisplayed()).toBe(true);
    });

    it('should see user management page', function () {
        // check existence of users table
        element(by.linkText('Manage Users')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(userManagement.getPanelBody().isDisplayed()).toBe(true);

        element.all(by.repeater('user in data')).then(function(elements){
            numOfUsers = elements.length;
        });
    });

    it('should be able to canceling by clicking cancel button', function () {
        // popup add user
        userManagement.getAddNewUserButton().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(userManagement.getAddNewUserModal().isDisplayed()).toBe(true);

        var email = 'LE_' + randomName() + '@e2e.test.com';
        element(by.model('user.FirstName')).sendKeys('E2E');
        element(by.model('user.LastName')).sendKeys('Tester');
        element(by.model('user.Email')).sendKeys(email);
        browser.waitForAngular();
        browser.driver.sleep(1000);

        userManagement.getAddNewUserCancelButton().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        // check cancel by button
        expect(element.all(by.repeater('user in data')).count()).toEqual(numOfUsers);

        // add a user
        userManagement.getAddNewUserButton().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        element(by.model('user.FirstName')).sendKeys('E2E');
        element(by.model('user.LastName')).sendKeys('Tester');
        element(by.model('user.Email')).sendKeys(email);
        browser.waitForAngular();

        userManagement.getAddNewUserCrossSymbol().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        // check cancel by cross symbol
        expect(element.all(by.repeater('user in data')).count()).toEqual(numOfUsers);
    });

    it('should verify create user', function () {
        // popup add user
        userManagement.getAddNewUserButton().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        newUserEmail = 'LE_' + randomName() + '@e2e.test.com';
        element(by.model('user.FirstName')).sendKeys('E2E');
        element(by.model('user.LastName')).sendKeys('Tester');
        element(by.model('user.Email')).sendKeys(newUserEmail);
        browser.waitForAngular();
        browser.driver.sleep(1000);

        userManagement.getAddNewUserSaveButton().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        expect(userManagement.getAddNewUserSuccessAlert().isDisplayed()).toBe(true);
        userManagement.getAddNewUserSuccessAlert().getText().then(function(text){
            newUserPassword = text.split('temporary password: ')[1];
        });

        userManagement.getAddNewUserSuccessOKButton().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        expect(element.all(by.repeater('user in data')).count()).toEqual(numOfUsers + 1);

        logoutPage.logoutAsAdmin();
    });

    it('should be able to login the new user', function () {
        loginPage.loginUser(newUserEmail, newUserPassword);
        browser.waitForAngular();
        browser.driver.sleep(1000);

        expect(element(by.css('h1')).getText()).toEqual('All Models');
    });

    it('should be able to open change password page', function () {
        userDropdown.getUserLink("E2E Tester").click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        element(by.linkText("Update Password")).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    });

    it('should verify the current password is not empty', function () {
        element(by.buttonText("UPDATE")).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        expect(element.all(by.css('div.global-error')).first().isDisplayed()).toBe(true);
    });

    it('should verify new password and confirm new password are the same', function () {
        element(by.model("oldPassword")).sendKeys('Admin123');
        element(by.model("newPassword")).sendKeys('Admin123');
        element(by.model("confirmPassword")).sendKeys('Admin123');
        browser.waitForAngular();
        browser.driver.sleep(1000);

        element(by.buttonText("UPDATE")).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        expect(element.all(by.css('div.global-error')).first().isDisplayed()).toBe(true);
    });

    it('should be able to change password', function () {
        element(by.model("oldPassword")).clear();
        element(by.model("oldPassword")).sendKeys(newUserPassword);
        browser.waitForAngular();
        browser.driver.sleep(1000);

        element(by.buttonText("UPDATE")).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        expect(element(by.css('div.centered h1')).isDisplayed()).toBe(true);
        browser.driver.sleep(1000);

        element(by.buttonText("RETURN TO LOGIN")).click();
    }, 60000);

    it('should be able to login using the new password', function () {
        loginPage.loginUser(newUserEmail, "Admin123");
        browser.waitForAngular();
        browser.driver.sleep(1000);

        expect(element(by.css('h1')).getText()).toEqual('All Models');

        logoutPage.logout("E2E Tester");
    });

    it('should be able to add an exsiting user', function () {
        loginPage.loginAsAdmin();
        tenants.getTenantByIndex(params.alternativeTenantIndex).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        // check existence of Manage Users link
        userDropdown.getUserLink(params.adminDisplayName).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        element(by.linkText('Manage Users')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        userManagement.getAddNewUserButton().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        element(by.model('user.FirstName')).sendKeys('E2E');
        element(by.model('user.LastName')).sendKeys('Tester');
        element(by.model('user.Email')).sendKeys(newUserEmail);
        browser.waitForAngular();
        browser.driver.sleep(1000);
        userManagement.getAddNewUserSaveButton().click();
        browser.waitForAngular();
        browser.driver.sleep(3000);

        expect(element.all(by.xpath('//div[@data-ng-show="showExistingUser"]')).first().isDisplayed()).toBe(true);
        element(by.id('add-user-btn-ok-2')).click();
        browser.waitForAngular();
        browser.driver.sleep(3000);

        element(by.buttonText('OK')).click();
        browser.waitForAngular();
        browser.driver.sleep(3000);

        logoutPage.logoutAsAdmin();
    }, 60000);


    it('should be able to login the new user to the second tenant', function () {
        loginPage.loginUser(newUserEmail, "Admin123");
        browser.waitForAngular();
        browser.driver.sleep(1000);

        tenants.getTenantByIndex(params.alternativeTenantIndex).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        expect(element(by.css('h1')).getText()).toEqual('All Models');

        logoutPage.logout("E2E Tester");
    });

    it('should verify delete user', function () {
        loginPage.loginAsAdmin();
        tenants.getTenantByIndex(params.tenantIndex).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        userDropdown.getUserLink(params.adminDisplayName).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        element(by.linkText('Manage Users')).click();
        browser.waitForAngular();
        browser.driver.sleep(2000);

        userManagement.selectUser(newUserEmail);
        browser.waitForAngular();
        browser.driver.sleep(1000);

        userManagement.getDeleteUsersButton().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        element(by.id('delete-user-btn-ok')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        expect(element(by.xpath('//div[@data-ng-show="successUsers.length > 0"]')).isDisplayed()).toBe(true);
        element(by.id('delete-user-btn-ok')).click();
        browser.waitForAngular();
        browser.driver.sleep(3000);
        expect(element.all(by.repeater('user in data')).count()).toEqual(numOfUsers);

        logoutPage.logoutAsAdmin();
    }, 60000);

    it('should verify that the new user will be automatically attached the second tenant', function () {
        loginPage.loginUser(newUserEmail, "Admin123");
        browser.waitForAngular();
        browser.driver.sleep(1000);

        expect(element(by.css('h1')).getText()).toEqual('All Models');

        logoutPage.logout("E2E Tester");
    });

    it('should be able to hard delete the new user', function () {
        loginPage.loginAsAdmin();
        tenants.getTenantByIndex(params.alternativeTenantIndex).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        userDropdown.getUserLink(params.adminDisplayName).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        element(by.linkText('Manage Users')).click();
        browser.waitForAngular();
        browser.driver.sleep(2000);

        userManagement.selectUser(newUserEmail);
        browser.waitForAngular();
        browser.driver.sleep(1000);

        userManagement.getDeleteUsersButton().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        element(by.id('delete-user-btn-ok')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        expect(element(by.xpath('//div[@data-ng-show="successUsers.length > 0"]')).isDisplayed()).toBe(true);
        element(by.id('delete-user-btn-ok')).click();
        browser.waitForAngular();
        browser.driver.sleep(3000);

        logoutPage.logoutAsAdmin();
    }, 60000);

});

