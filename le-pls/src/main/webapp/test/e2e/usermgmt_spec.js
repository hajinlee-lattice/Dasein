describe('user management', function() {

    var externalUser = require('./po/externaluser.po');
    var externalAdmin = require('./po/externaladmin.po');
    var internalUser = require('./po/internaluser.po');
    var internalAdmin = require('./po/internaladmin.po');
    var superAdmin = require('./po/superadmin.po');
    var userDropdown = require('./po/userdropdown.po');
    var userManagement = require('./po/usermgmt.po');
    var loginPage = require('./po/login.po');

    externalUser.testUserManagement();

    externalAdmin.testUserManagement();

    internalUser.testUserManagement();

    internalAdmin.testUserManagement();

    superAdmin.testUserManagement();
    
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
        expect(element(by.css('#add-user-modal')).isPresent()).toBe(true);
        var testName = "0000" + userManagement.randomName(8);
        userManagement.createNewUser(testName);
        browser.driver.sleep(500);
        
        //==================================================
        // Open edit user modal and edit the user
        //==================================================
        element.all(by.css('.js-edit-user-link')).first().click();
        browser.driver.sleep(500);
        expect(element(by.css('#edit-user-modal')).isDisplayed()).toBe(true);
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
        var expectedNumOfCurrentUsers = element.all(by.repeater('user in users')).count();
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
    
    //
    //it('should see add new user model', function () {
    //    // popup add user
    //    userManagement.getAddNewUserButton().click();
    //    browser.driver.sleep(1000);
    //    expect(userManagement.NewUserModal.isDisplayed()).toBe(true);
    //});
    //
    //it('should be able to canceling by clicking cancel button', function () {
    //    var email = 'LE_' + randomName() + '@e2e.test.com';
    //    element(by.model('user.FirstName')).sendKeys('E2E');
    //    element(by.model('user.LastName')).sendKeys('Tester');
    //    element(by.model('user.Email')).sendKeys(email);
    //    userManagement.getAddNewUserCancelButton().click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //    ).toEqual(numOfUsers);
    //});
    //
    //it('should be able to canceling by clicking cross symbol', function () {
    //    var email = 'LE_' + randomName() + '@e2e.test.com';
    //    userManagement.getAddNewUserButton().click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //    element(by.model('user.FirstName')).sendKeys('E2E');
    //    element(by.model('user.LastName')).sendKeys('Tester');
    //    element(by.model('user.Email')).sendKeys(email);
    //    browser.waitForAngular();
    //
    //    userManagement.getAddNewUserCrossSymbol().click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    // check cancel by cross symbol
    //    expect(element.all(by.repeater('user in data')).count()).toEqual(numOfUsers);
    //
    //    logoutPage.logoutAsAdmin();
    //});

    //it('should verify create user', function () {
    //    // popup add user
    //    userManagement.getAddNewUserButton().click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    newUserEmail = 'LE_' + randomName() + '@e2e.test.com';
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
    //        newUserPassword = text.split('temporary password: ')[1];
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
    //
    //it('should be able to login the new user', function () {
    //    loginPage.loginUser(newUserEmail, newUserPassword);
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    expect(element(by.css('h1')).getText()).toEqual('All Models');
    //});
    //
    //it('should be able to open change password page', function () {
    //    userDropdown.getUserLink("E2E Tester").click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    element(by.linkText("Update Password")).click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //});
    //
    //it('should verify the current password is not empty', function () {
    //    element(by.buttonText("UPDATE")).click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    expect(element.all(by.css('div.global-error')).first().isDisplayed()).toBe(true);
    //});
    //
    //it('should verify new password and confirm new password are the same', function () {
    //    element(by.model("oldPassword")).sendKeys('Admin123');
    //    element(by.model("newPassword")).sendKeys('Admin123');
    //    element(by.model("confirmPassword")).sendKeys('Admin1234');
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    element(by.buttonText("UPDATE")).click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    expect(element.all(by.css('div.global-error')).first().isDisplayed()).toBe(true);
    //});
    //
    //it('should verify that old passowrd must be correct', function () {
    //    element(by.model("oldPassword")).clear();
    //    element(by.model("oldPassword")).sendKeys(newUserPassword);
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    element(by.buttonText("UPDATE")).click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(2000);
    //
    //    expect(element.all(by.css('div.global-error')).first().isDisplayed()).toBe(true);
    //});
    //
    //it('should be able to change password', function () {
    //    element(by.model("confirmPassword")).clear();
    //    element(by.model("confirmPassword")).sendKeys('Admin123');
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    element(by.buttonText("UPDATE")).click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    expect(element(by.css('div.centered h1')).isDisplayed()).toBe(true);
    //    browser.driver.sleep(1000);
    //    newUserPassword = "Admin123";
    //
    //    element(by.buttonText("RETURN TO LOGIN")).click();
    //    browser.driver.sleep(2000);
    //}, 60000);
    //
    //it('should be able to login using the new password', function () {
    //    loginPage.loginUser(newUserEmail, "Admin123");
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    expect(element(by.css('h1')).getText()).toEqual('All Models');
    //    browser.driver.sleep(1000);
    //
    //    logoutPage.logout("E2E Tester");
    //});
    //
    //it('should be able to add an exsiting user', function () {
    //    loginPage.loginAsAdmin();
    //    tenants.selectTenantByIndex(params.alternativeTenantIndex);
    //    browser.waitForAngular();
    //
    //    // check existence of Manage Users link
    //    userDropdown.getUserLink(params.adminDisplayName).click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //    element(by.linkText('Manage Users')).click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //    userManagement.getAddNewUserButton().click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    element(by.model('user.FirstName')).sendKeys('E2E');
    //    element(by.model('user.LastName')).sendKeys('Tester');
    //    element(by.model('user.Email')).sendKeys(newUserEmail);
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //    userManagement.getAddNewUserSaveButton().click();
    //    browser.waitForAngular();
    //    browser.driver.sleep(3000);
    //
    //    expect(element.all(by.xpath('//div[@data-ng-show="showExistingUser"]')).first().isDisplayed()).toBe(true);
    //    element(by.id('add-user-btn-ok-2')).click();
    //    browser.waitForAngular();
    //    //browser.driver.sleep(3000);
    //    browser.driver.wait(function(){
    //        return element(by.buttonText('OK')).isPresent();
    //    }, 10000, 'OK button should appear with in 10 sec.').then(function(){
    //        element(by.buttonText('OK')).click();
    //    });
    //}, 60000);
    //
    //it('should log out admin', function(){
    //    logoutPage.logoutAsAdmin();
    //});
    //
    //it('should be able to login the new user to the second tenant', function () {
    //    loginPage.loginUser(newUserEmail, newUserPassword);
    //    tenants.selectTenantByIndex(params.alternativeTenantIndex);
    //
    //    expect(element(by.css('h1')).getText()).toEqual('All Models');
    //
    //    logoutPage.logout("E2E Tester");
    //});
    //
    //it('should verify delete user', function () {
    //    loginPage.loginAsAdmin();
    //    tenants.selectTenantByIndex(params.tenantIndex);
    //
    //    userDropdown.getUserLink(params.adminDisplayName).click();
    //    sleep();
    //
    //    element(by.linkText('Manage Users')).click();
    //    sleep();
    //
    //    userManagement.selectUser(newUserEmail);
    //    sleep();
    //
    //    userManagement.getDeleteUsersButton().click();
    //    sleep();
    //
    //    element(by.id('delete-user-btn-ok')).click();
    //    sleep();
    //
    //    expect(element(by.xpath('//div[@data-ng-show="successUsers.length > 0"]')).isDisplayed()).toBe(true);
    //    element(by.id('delete-user-btn-ok')).click();
    //    sleep(3000);
    //    expect(element.all(by.repeater('user in data')).count()).toEqual(numOfUsers);
    //
    //    logoutPage.logoutAsAdmin();
    //}, 60000);
    //
    //it('should verify that the new user will be automatically attached the second tenant', function () {
    //    loginPage.loginUser(newUserEmail, newUserPassword);
    //    browser.waitForAngular();
    //    browser.driver.sleep(1000);
    //
    //    expect(element(by.css('h1')).getText()).toEqual('All Models');
    //
    //    logoutPage.logout("E2E Tester");
    //});
    //
    //it('should be able to hard delete the new user', function () {
    //    loginPage.loginAsAdmin();
    //    tenants.selectTenantByIndex(params.alternativeTenantIndex);
    //
    //    userDropdown.getUserLink(params.adminDisplayName).click();
    //    sleep();
    //
    //    element(by.linkText('Manage Users')).click();
    //    sleep();
    //
    //    userManagement.selectUser(newUserEmail);
    //    sleep();
    //
    //    userManagement.getDeleteUsersButton().click();
    //    sleep();
    //
    //    element(by.id('delete-user-btn-ok')).click();
    //    sleep();
    //
    //    expect(element(by.xpath('//div[@data-ng-show="successUsers.length > 0"]')).isDisplayed()).toBe(true);
    //    element(by.id('delete-user-btn-ok')).click();
    //    sleep();
    //
    //    logoutPage.logoutAsAdmin();
    //}, 60000);


    //function sleep(time) {
    //    if (time == null) { time = 2000; }
    //    browser.waitForAngular();
    //    browser.driver.sleep(time);
    //}
});

