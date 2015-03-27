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
        tenants.selectTenantByIndex(params.tenantIndex);

        // choose any model
        expect(element(by.css('.js-top-predictor-donut')).isPresent()).toBe(false);
        modelList.getAnyModel().click();
        browser.waitForAngular();

        // default to predictor tab, check existence of donut
        expect(element(by.css('.js-top-predictor-donut')).isDisplayed()).toBe(true);
        browser.driver.sleep(1000);

        // check existence of threshold explorer
        expect(element(by.id('thresholdExplorerChart')).isDisplayed()).toBe(false);
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
        expect(element(by.id('topSampleTable')).isDisplayed()).toBe(false);
        modelTabs.getTabByIndex(2).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(element(by.id('topSampleTable')).isDisplayed()).toBe(true);

        // check existence of hidden admin page
        expect(element(by.id('adminInfoContainer')).isPresent()).toBe(false);
        element(by.id('adminLink')).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(element(by.id('adminInfoContainer')).isDisplayed()).toBe(true);

        // logout
        logoutPage.logoutAsAdmin();
    }, 60000);

    it('should validate model back button', function () {
        // element(by.css('.back-button')).click();
    });

    it('should verify nonadmins do not see admin functionality', function () {    
        loginPage.loginAsNonAdmin();
        tenants.selectTenantByIndex(params.tenantIndex);

        element.all(by.css('a.model')).first().click();
        browser.driver.wait(function(){
            return element(by.css('a.back-button')).isDisplayed();
        }, 10000, 'tabs list should appear with in 10 sec.');

        element(by.linkText('SAMPLE LEADS')).click();

        expect(element(by.linkText('Admin')).isPresent()).toBe(false);

        logoutPage.logoutAsNonAdmin();
    });  
});
