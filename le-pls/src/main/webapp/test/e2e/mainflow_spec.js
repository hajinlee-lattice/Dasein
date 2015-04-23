describe('smoketest main flow of app', function() {

    var loginPage = require('./po/login.po');
    var modelDetails = require('./po/modeldetails.po');
    var modelList = require('./po/modellist.po');    
    var modelTabs = require('./po/modeltabs.po');

    it('should allow admin user to log in, choose tenant, choose model, verify existence of donut, verify existence of threshold explorer, and log out', function () {
        loginPage.loginAsSuperAdmin();

        runMaiFlowTests();

        loginPage.logout();
    }, 60000);

    it('should verify only super admin sees the hidden link', function () {
        loginPage.loginAsExternalUser();
        assertAdminLinkIsVisible(false);

        loginPage.loginAsExternalAdmin();
        assertAdminLinkIsVisible(false);

        loginPage.loginAsInternalUser();
        assertAdminLinkIsVisible(true);

        loginPage.loginAsInternalAdmin();
        assertAdminLinkIsVisible(true);

        loginPage.loginAsSuperAdmin();
        assertAdminLinkIsVisible(true);

        loginPage.logout();
    });

    function runMaiFlowTests() {
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
    }

    function assertAdminLinkIsVisible(expected) {
        element.all(by.css('a.model')).first().click();
        browser.driver.wait(function(){
            return element(by.css('a.back-button')).isDisplayed();
        }, 10000, 'tabs list should appear with in 10 sec.');

        element(by.linkText('SAMPLE LEADS')).click();

        if (expected) {
            expect(element(by.linkText('Admin')).isDisplayed()).toBe(true);
        } else {
            expect(element(by.linkText('Admin')).isPresent()).toBe(false);
        }

    }
});
