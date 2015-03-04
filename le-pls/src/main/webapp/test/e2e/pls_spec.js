describe('pls app', function() {

    var params = browser.params;

    var loginPage = require('./login.po');
    var userDropdown = require('./userdropdown.po');
    var tenants = require('./tenantselection.po');
    var modelList = require('./modellist.po');
    var modelTabs = require('./modeltabs.po');

    it('should allow admin user to log in, choose tenant, choose model, verify existence of donut, verify existence of threshold explorer, and log out', function () {
        // login as admin
        loginPage.get();
        loginPage.email.sendKeys('bnguyen');
        loginPage.password.sendKeys('tahoe');     
        loginPage.loginButton.click();
        browser.waitForAngular();
        console.log('tenant index:' + params.tenantIndex);               
        browser.driver.sleep(1000);

        // choose tenant
        tenants.getTenantByIndex(params.tenantIndex).click();
        browser.waitForAngular(); 
        browser.driver.sleep(1000);     

        // choose any model
        modelList.getModel().click();
        browser.waitForAngular();

        // check existence of donut         
        expect(element(by.css('.js-top-predictor-donut')).isPresent()).toBe(true);
        browser.driver.sleep(1000);

        // check existence of threshold explorer
        modelTabs.getTabByIndex(1).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(element(by.id('thresholdExplorerChart')).isPresent()).toBe(true);
        
        // logout
        userDropdown.getUserLink('Everything IsAwesome').click();  
        browser.waitForAngular(); 
        userDropdown.signout.click();
        browser.driver.sleep(1000);
        expect(loginPage.loginButton.isPresent()).toBe(true);      
    });   

});
