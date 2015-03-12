describe('user management', function() {

    var params = browser.params;

    var loginPage = require('./po/login.po');
    var logoutPage = require('./po/logout.po');
    var tenants = require('./po/tenantselection.po');
    var userDropdown = require('./po/userdropdown.po');
    var userManagement = require('./po/usermgmt.po');
    var numOfUsers = 0;
    var newUserEmail;

    function randomName()
    {
        var text = "";
        var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        for( var i=0; i < 5; i++ )
            text += possible.charAt(Math.floor(Math.random() * possible.length));

        return text;
    }

    it('should login as a non-admin user', function () {
        loginPage.loginAsNonAdmin();
        tenants.getTenantByIndex(params.tenantIndex).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    }, 60000);

    it('should verify user management is invisible to non-admin users', function () {
        // check existence of Manage Users link
        userDropdown.getUserLink(params.nonAdminDisplayName).click().then(function(){
            expect(element(by.linkText('Manage Users')).isPresent()).toBe(false);
        });
        browser.waitForAngular();
        browser.driver.sleep(1000);
    });

    it('should logout as a non-admin user', function () {
        userDropdown.getUserLink(params.nonAdminDisplayName).click().then(function(){
            logoutPage.logoutAsNonAdmin();
        });
    });

    it('should login as an admin users', function () {
        loginPage.loginAsAdmin();

        // choose tenant
        tenants.getTenantByIndex(params.tenantIndex).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    });

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

    it('should see add new user modal', function () {
        // popup add user
        userManagement.getAddNewUserButton().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(userManagement.getAddNewUserModal().isDisplayed()).toBe(true);
    });

    it('should be able to canceling by clicking cancel button', function () {
        var email = randomName() + '@e2e.com';
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
    });

    it('should be able to canceling by clicking cross symbol', function () {
        var email = randomName() + '@e2e.com';
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

        newUserEmail = randomName() + '@e2e.com';
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
            console.log(text);
        });

        userManagement.getAddNewUserSuccessOKButton().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        expect(element.all(by.repeater('user in data')).count()).toEqual(numOfUsers + 1);

    });

    it('should verify delete user', function () {
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
        browser.driver.sleep(1000);
        expect(element.all(by.repeater('user in data')).count()).toEqual(numOfUsers);

        logoutPage.logoutAsAdmin();
    });

});

