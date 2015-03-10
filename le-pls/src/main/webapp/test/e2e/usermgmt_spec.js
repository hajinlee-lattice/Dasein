describe('user management', function() {

    var params = browser.params;

    var loginPage = require('./po/login.po');
    var logoutPage = require('./po/logout.po');
    var tenants = require('./po/tenantselection.po');
    var userDropdown = require('./po/userdropdown.po');
    var userManagement = require('./po/usermgmt.po');

    function randomName()
    {
        var text = "";
        var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        for( var i=0; i < 5; i++ )
            text += possible.charAt(Math.floor(Math.random() * possible.length));

        return text;
    }

    it('should verify user management only visible to admin users', function () {

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

        logoutPage.logoutAsAdmin();

        loginPage.loginAsNonAdmin();

        // choose tenant
        tenants.getTenantByIndex(params.tenantIndex).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        // check existence of Manage Users link
        userDropdown.getUserLink(params.nonAdminDisplayName).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(element(by.linkText('Manage Users')).isPresent()).toBe(false);

        userDropdown.getUserLink(params.nonAdminDisplayName).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        logoutPage.logoutAsNonAdmin();
    });

    it('should verify create and delete user', function () {

        loginPage.loginAsAdmin();

        // choose tenant
        tenants.getTenantByIndex(params.tenantIndex).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        // check existence of Manage Users link
        userDropdown.getUserLink(params.adminDisplayName).click();
        browser.waitForAngular();
        //browser.driver.sleep(1000);
        element(by.linkText('Manage Users')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        // check existence of users table
        //var originalUserNum = element.all(by.repeater('user in data')).count();

        userManagement.getAddNewUserButton().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        userManagement.getAddNewUserCancelButton().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        logoutPage.logoutAsAdmin();
    });

});

