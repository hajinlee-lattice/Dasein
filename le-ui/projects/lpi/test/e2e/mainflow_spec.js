describe('smoketest main flow of app', function() {

    var loginPage = require('./po/login.po');
    var modelDetails = require('./po/modeldetails.po');
    var modelList = require('./po/modellist.po');    
    var modelTabs = require('./po/modeltabs.po');

    it('should allow admin user to log in, choose tenant, choose model, verify existence of donut, verify existence of threshold explorer, and log out', function () {
        loginPage.loginAsSuperAdmin();

        runMaiFlowTests();

        loginPage.logout();
    }, 200000);

    function runMaiFlowTests() {
        // choose any model
        expect(element(by.css('.js-top-predictor-donut')).isPresent()).toBe(false);
        modelList.getAnyModel().click();
        browser.waitForAngular();

        // default to predictor tab, check existence of donut
        expect(element(by.css('.js-top-predictor-donut')).getWebElement().isDisplayed()).toBe(true);
        browser.driver.sleep(10000);

        // check existence of threshold explorer
        expect(element(by.id('thresholdExplorerChart')).getWebElement().isDisplayed()).toBe(false);
        modelTabs.getTabByIndex(1).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(element(by.id('thresholdExplorerChart')).getWebElement().isDisplayed()).toBe(true);

        // toggle details on, off, on again
        modelDetails.toggleDetails();
        expect(element(by.id('moreDataPoints')).getWebElement().isDisplayed()).toBe(true);
        modelDetails.toggleDetails();
        expect(element(by.id('moreDataPoints')).getWebElement().isDisplayed()).toBe(false);
        modelDetails.toggleDetails();
        expect(element(by.id('moreDataPoints')).getWebElement().isDisplayed()).toBe(true);

        // check existence of sample leads tab
        expect(element(by.id('topSampleTable')).getWebElement().isDisplayed()).toBe(false);
        modelTabs.getTabByIndex(2).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        expect(element(by.id('topSampleTable')).getWebElement().isDisplayed()).toBe(true);

        // check existence of hidden admin page
        expect(element(by.id('adminInfoContainer')).isPresent()).toBe(false);
        element(by.id('adminLink')).click();
        browser.waitForAngular();
        browser.driver.sleep(10000);
        expect(element(by.id('adminInfoContainer')).getWebElement().isDisplayed()).toBe(true);

        // default to summary tab in hidden admin page, check existence of it
        expect(element(by.id('adminInfoSummaryTable')).getWebElement().isDisplayed()).toBe(true);
        browser.driver.sleep(1000);

        // check existence of alerts tab
        var showAlertsTab = ! browser.params.isProd;
        if (showAlertsTab) {
            expect(element(by.id('adminInfoAlertsTable')).getWebElement().isDisplayed()).toBe(false);
            modelTabs.getTabByIndex(1).click();
            browser.waitForAngular();
            browser.driver.sleep(1000);
            expect(element(by.id('adminInfoAlertsTable')).getWebElement().isDisplayed()).toBe(true);
        }

    }
});
