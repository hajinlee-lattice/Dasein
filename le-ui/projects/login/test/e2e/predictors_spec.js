'use strict';

describe('top predictors', function () {

    var helper = require('./po/helper.po');
    var loginPage = require('./po/login.po');
    var modelList = require('./po/modellist.po');
    var modelTabs = require('./po/modeltabs.po');

    it('should validate top predictors donut', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsExternalUser();

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

        expect(element(by.id('chart')).isDisplayed()).toBe(true);

        performTests();

        loginPage.logout();
    });

    it('should verify export all', function () {
        loginPage.loginAsExternalUser();

        verifyExportAll();

        loginPage.logout();
    });

    function performTests() {
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
        checkBackButtonHover(true, false);
    }

    var tab = element(by.id("modelDetailsAttributesTab"));
    var chart = tab.element(by.id("chart"));
    var hover = element(by.id("topPredictorAttributeHover"));

    var checkBackButton = function (expected) {
        helper.elementExists(element(by.id("donutChartBackButton")), expected);
    };

    var checkHover = function (expected) {
        helper.elementExists(element(by.css("div.attribute-hover")), expected);
    };

    var checkBackButtonHover = function (buttonExpected, hoverExpected) {
        checkBackButton(buttonExpected);
        checkHover(hoverExpected);
    };

    var checkBackButtonHoverAndGoBack = function () {
        checkBackButtonHover(true, false);
        element(by.id("donutChartBackButton")).click();
        sleep();
        checkBackButtonHover(false, false);
    };

    var clickAttributeValue = function () {
        element(by.id("attributes")).all(by.tagName("li")).get(0).click();
        sleep();
    };

    var clickChartWedge = function () {
        chart.all(by.tagName("path")).get(3).click();
        sleep();
    };

    var moveToChartWedge = function () {
        browser.actions().mouseMove(chart.all(by.tagName("path")).get(3)).perform();
        sleep();
    };

    var moveOffChartWedge = function () {
        browser.actions().mouseMove(chart).perform();
        sleep();
    };

    function verifyExportAll() {
        helper.removeFile("attributes.csv");

        modelList.getAnyModel().click();
        browser.waitForAngular();

        helper.elementExists(element(by.linkText("EXPORT ALL")), true);
        element(by.linkText("EXPORT ALL")).click();
        browser.waitForAngular();

        helper.fileExists("attributes.csv");
    }

    function sleep(time) {
        if (time == null) { time = 2000; }
        browser.waitForAngular();
        browser.driver.sleep(time);
    }
});
