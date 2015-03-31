'use strict';

describe('top predictors', function () {

    var params = browser.params;

    var loginPage = require('./po/login.po');
    var modelList = require('./po/modellist.po');
    var modelTabs = require('./po/modeltabs.po');
    var logoutPage = require('./po/logout.po');

    var tab = element(by.id("modelDetailsAttributesTab"));
    var chart = tab.element(by.id("chart"));

    var checkBackButton = function (expected) {
        if (expected) {
            expect(element(by.id("donutChartBackButton")).isDisplayed()).toBe(true);
        } else {
            expect(element(by.id("donutChartBackButton")).isPresent()).toBe(false);
        }
    };

    var checkHover = function (expected) {
        sleep(1000);
        if (expected) {
            expect(element(by.css("div.attribute-hover")).isDisplayed()).toBe(true);
        } else {
            expect(element(by.css("div.attribute-hover")).isPresent()).toBe(false);
        }
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


    it('should validate top predictors donut', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsNonAdminToTenant(params.tenantIndex);

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
    });

    it('should not see back button or hover', function () {
        checkBackButtonHover(false, false);
    });

    it('should see back button when click on an attribute', function () {
        clickAttributeValue();
        checkBackButtonHoverAndGoBack();
    });

    it('should see back button when click on a wedge', function () {
        clickChartWedge();
        checkBackButtonHoverAndGoBack();
    });


    it('should see hover on and off by move to and off a wedge', function () {
        moveToChartWedge();
        checkBackButtonHover(false, true);

        moveOffChartWedge();
        checkBackButtonHover(false, false);
    });

    it('should verify the same behavior after go into an attribute', function () {
        clickChartWedge();
        checkBackButtonHover(true, false);

        moveToChartWedge();
        checkBackButtonHover(true, true);

        moveOffChartWedge();
        checkBackButtonHover(true, false);
    });

    it('show logout non admin', function () {
        logoutPage.logoutAsNonAdmin();
    });

    function sleep(time) {
        if (time == null) { time = 2000; }
        browser.waitForAngular();
        browser.driver.sleep(time);
    }
});
