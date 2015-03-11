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

    it('should verify user management is visible to admin users', function () {

        loginPage.loginAsAdmin();

        // choose tenant
        tenants.getTenantByIndex(params.tenantIndex).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        // check existence of Manage Users link
        userDropdown.getUserLink(params.adminDisplayName).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(element(by.linkText('Manage Users')).isDisplayed()).toBe(true);

        // check existence of users table
        element(by.linkText('Manage Users')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(userManagement.getPanelBody().isDisplayed()).toBe(true);

        element.all(by.repeater('user in data')).then(function(elements){
            numOfUsers = elements.length;
        });

        logoutPage.logoutAsAdmin();
        browser.driver.sleep(1000);
    }, 45000);

    it('should verify user management is invisible to non-admin users', function () {
        loginPage.loginAsNonAdmin();

        // choose tenant
        tenants.getTenantByIndex(params.tenantIndex).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        // check existence of Manage Users link
        userDropdown.getUserLink(params.nonAdminDisplayName).click().then(function(){
            expect(element(by.linkText('Manage Users')).isPresent()).toBe(false);
        });
        browser.waitForAngular();
        browser.driver.sleep(1000);

        userDropdown.getUserLink(params.nonAdminDisplayName).click().then(function(){
            logoutPage.logoutAsNonAdmin();
        });
        browser.waitForAngular();
        browser.driver.sleep(1000);
    });

    it('should verify canceling adding new user will not add user', function () {
        loginPage.loginAsAdmin();

        // choose tenant
        tenants.getTenantByIndex(params.tenantIndex).click().then(function(){
            userDropdown.getUserLink(params.adminDisplayName).click().then(function(){
                element(by.linkText('Manage Users')).click();
            });
        });
        browser.waitForAngular();
        browser.driver.sleep(1000);

        // popup add user
        userManagement.getAddNewUserButton().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        expect(userManagement.getAddNewUserModal().isDisplayed()).toBe(true);

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

        logoutPage.logoutAsAdmin();
    }, 60000);

    //it('should verify create and delete user', function () {
    //
    //    loginPage.loginAsAdmin();
    //
    //    // choose tenant
    //    tenants.getTenantByIndex(params.tenantIndex).click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    userDropdown.getUserLink(params.adminDisplayName).click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    // enter user managemant page
    //    element(by.linkText('Manage Users')).click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    // popup add user
    //    userManagement.getAddNewUserButton().click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    newUserEmail = randomName() + '@e2e.com';
    //    element(by.model('user.FirstName')).sendKeys('E2E');
    //    element(by.model('user.LastName')).sendKeys('Tester');
    //    element(by.model('user.Email')).sendKeys(newUserEmail);
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    userManagement.getAddNewUserSaveButton().click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    expect(userManagement.getAddNewUserSuccessAlert().isDisplayed()).toBe(true);
    //    userManagement.getAddNewUserSuccessAlert().getText().then(function(text){
    //        console.log(text);
    //    });
    //
    //    userManagement.getAddNewUserSuccessOKButton().click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    expect(element.all(by.repeater('user in data')).count()).toEqual(numOfUsers + 1);
    //
    //    logoutPage.logoutAsAdmin();
    //});

});

