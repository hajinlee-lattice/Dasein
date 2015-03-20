'use strict';

describe('top predictors', function() {

    var params = browser.params;

    var loginPage = require('./po/login.po');
    var tenants = require('./po/tenantselection.po');
    var modelList = require('./po/modellist.po');
    var modelTabs = require('./po/modeltabs.po');
    var logoutPage = require('./po/logout.po');

    it('should validate top predictors donut', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsNonAdmin();

        //==================================================
        // Select Tenant
        //==================================================
        tenants.getTenantByIndex(params.tenantIndex).click();
        browser.waitForAngular();

        //==================================================
        // Select Model
        //==================================================
        modelList.getAnyModel().click();
        browser.waitForAngular();

        //==================================================
        // Select Attributes Tab
        //==================================================
        modelTabs.getTabByIndex(0).click();
        browser.waitForAngular();

        //==================================================
        // Perform Actions
        //==================================================
        PerformActions();

        //==================================================
        // Logout
        //==================================================
        logoutPage.logoutAsNonAdmin();
    });
    
    //==================================================
    // Perform Actions
    //==================================================
    function PerformActions() {

        function sleep() {
            browser.driver.sleep(3000);
        }

        sleep();

        var tab = element(by.id("modelDetailsAttributesTab"));
        var chart = tab.element(by.id("chart"));
        var backButton = chart.element(by.id("donutChartBackButton"));
        var hover = tab.element(by.id("topPredictorAttributeHover"));

        function checkBackButton(expected) {
            backButton.isPresent().then(function (value) {
                return expect(expected === value).toBe(true);
            });
        }

        function checkHover(expected) {
            hover.getAttribute('style').then(function (value) {
                return expect(expected === (value.indexOf("display: block;") != -1)).toBe(true);
            });
        }

        function checkBackButtonHover(buttonExpected, hoverExpected) {
            checkBackButton(buttonExpected);
            checkHover(hoverExpected);
        }

        function checkBackButtonHoverAndGoBack() {
            checkBackButtonHover(true, false);
            backButton.click(); sleep();
            checkBackButtonHover(false, false);
        }

        function clickAttributeValue() {
            var attributes = element(by.id("attributes"));
            var attributeValues = attributes.all(by.tagName("li"));
            attributeValues.get(0).click(); sleep();
        }

        function clickChartWedge() {
            chart.all(by.tagName("path")).get(3).click(); sleep();
        }

        function moveToChartWedge() {
            browser.actions().mouseMove(chart.all(by.tagName("path")).get(3)).perform(); sleep();
        }

        function moveOffChartWedge() {
            browser.actions().mouseMove(chart).perform(); sleep();
        }

        checkBackButtonHover(false, false);

        clickAttributeValue();
        checkBackButtonHoverAndGoBack();

        clickChartWedge();
        checkBackButtonHoverAndGoBack();

        moveToChartWedge();
        checkBackButtonHover(false, true);

        moveOffChartWedge();
        checkBackButtonHover(false, false);

        clickChartWedge();
        checkBackButtonHover(true, false);

        moveToChartWedge();
        checkBackButtonHover(true, true);

        moveOffChartWedge();
        checkBackButtonHoverAndGoBack();
    }
});
