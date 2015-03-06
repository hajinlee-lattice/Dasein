describe('smoketest main flow of app', function() {

    var params = browser.params;

    var loginPage = require('./po/login.po');
    var logoutPage = require('./po/logout.po');
    var modelDetails = require('./po/modeldetails.po');
    var modelList = require('./po/modellist.po');    
    var modelTabs = require('./po/modeltabs.po');
    var userDropdown = require('./po/userdropdown.po');
    var tenants = require('./po/tenantselection.po');

    it('should allow admin user to log in, choose tenant, choose model, verify existence of donut, verify existence of threshold explorer, and log out', function () {
        loginPage.loginAsAdmin();

        // choose tenant
        console.log('tenant index:' + params.tenantIndex); 
        tenants.getTenantByIndex(params.tenantIndex).click();
        browser.waitForAngular(); 
        browser.driver.sleep(1000);     

        // choose any model
        modelList.getAnyModel().click();
        browser.waitForAngular();

        // default to predictor tab, check existence of donut         
        expect(element(by.css('.js-top-predictor-donut')).isDisplayed()).toBe(true);
        browser.driver.sleep(1000);

        // check existence of threshold explorer
        modelTabs.getTabByIndex(1).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(element(by.id('thresholdExplorerChart')).isDisplayed()).toBe(true);        

        // toggle details on, off, on again
        modelDetails.toggleDetails();        
        expect(element(by.id('moreDataPoints')).isDisplayed()).toBe(true); 
        modelDetails.toggleDetails();
        expect(element(by.id('moreDataPoints')).isDisplayed()).toBe(false); 
        modelDetails.toggleDetails();
        expect(element(by.id('moreDataPoints')).isDisplayed()).toBe(true); 

        // check existence of sample leads tab
        modelTabs.getTabByIndex(2).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(element(by.css('.leads-table')).isDisplayed()).toBe(true);  

        // check existence of hidden admin page
        element(by.id('adminLink')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);    
        expect(element(by.id('adminInfoContainer')).isDisplayed()).toBe(true);  
        
        // logout
        logoutPage.logoutAsAdmin();     
    });   

    it('should validate model back button', function () {
        // element(by.css('.back-button')).click();
    });  

    it('should verify nonadmins do not see admin functionality', function () {    
        loginPage.loginAsNonAdmin();

     //   logoutPage.logoutAsNonAdmin();
    });  
});
