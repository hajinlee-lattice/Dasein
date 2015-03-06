describe('user management', function() {

    var params = browser.params;

    var loginPage = require('./po/login.po');
    var logoutPage = require('./po/logout.po');
    var tenants = require('./po/tenantselection.po');
    var userDropdown = require('./po/userdropdown.po');    

    it('should verify user management functionality', function () {
        loginPage.loginAsAdmin();

        // choose tenant
        console.log('tenant index:' + params.tenantIndex); 
        tenants.getTenantByIndex(params.tenantIndex).click();
        browser.waitForAngular(); 
        browser.driver.sleep(1000);     
  
        logoutPage.logoutAsAdmin();
    });   
 
});
