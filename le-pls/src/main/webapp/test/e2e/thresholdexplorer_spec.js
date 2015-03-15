'use strict';

describe('threshold explorer', function() {

    var params = browser.params;

    var loginPage = require('./po/login.po');
    var tenants = require('./po/tenantselection.po');
    var modelList = require('./po/modellist.po');
    var modelTabs = require('./po/modeltabs.po');
    var logoutPage = require('./po/logout.po');

    it('should validate functional threshold explorer chart', function () {
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
        // Select Threshold Explorer Tab
        //==================================================
        modelTabs.getTabByIndex(1).click();
        browser.waitForAngular();

        //==================================================
        // Check Threshold Explorer Chart
        //==================================================
        expect(element(by.id('thresholdExplorerChart')).isDisplayed()).toBe(true);

        //==================================================
        // Get Default Values
        //==================================================
        var chartConversions = element(by.css(".xtext")).getText();
        var chartLeftLift = element(by.css(".ltext")).getText();
        var chartLeads = element(by.css(".lytext")).getText();
        var chartScore = element(by.css(".rytext")).getText();

        //==================================================
        // Check Some Labels
        //==================================================
        var rightLiftLabel = element(by.css(".rltext"));
        expect(rightLiftLabel.isDisplayed()).toBe(true);
        expect(rightLiftLabel.getText()).toEqual("LIFT");

        var leftLiftLabel = element(by.css(".lltext"));
        expect(leftLiftLabel.isDisplayed()).toBe(true);
        expect(leftLiftLabel.getText()).toEqual("LIFT");

        var convLabel = element(by.css(".xltext"));
        expect(convLabel.isDisplayed()).toBe(true);
        expect(convLabel.getText()).toEqual("% CONV");

        var scoreLabel = element(by.css(".ryltext"));
        expect(scoreLabel.isDisplayed()).toBe(true);
        expect(scoreLabel.getText()).toEqual("SCORE");

        var leadsLabel = element(by.css(".lyltext"));
        expect(leadsLabel.isDisplayed()).toBe(true);
        expect(leadsLabel.getText()).toEqual("TOP");

        //==================================================
        // Check Default Leads
        //==================================================
        expect(chartLeads).toEqual("20%");

        //==================================================
        // Check Default Score
        //==================================================
        expect(chartScore).toEqual("> 80");

        //==================================================
        // Check DecileGrid
        //==================================================
        var tab = element(by.id("modelDetailsExplorerTab"));
        var body = tab.element(by.tagName("tbody"));
        var rows = body.all(by.tagName("tr"));
        expect(rows.count()).toEqual(2);

        //==================================================
        // Check Default/Max Conversions
        //==================================================
        var tds = rows.get(0).all(by.tagName("td"));
        expect(tds.get(0).getText()).toEqual("% TOTAL CONVERSIONS");
        expect(tds.get(2).getText()).toEqual(chartConversions);
        expect(tds.get(10).getText()).toEqual("100%");

        //==================================================
        // Check Default/Max Lift
        //==================================================
        tds = rows.get(1).all(by.tagName("td"));
        expect(tds.get(0).getText()).toEqual("LIFT");
        expect(tds.get(2).getText()).toEqual(chartLeftLift);
        expect(tds.get(10).getText()).toEqual("1.00x");

        //==================================================
        // Logout
        //==================================================
        logoutPage.logoutAsNonAdmin();
    });
});
