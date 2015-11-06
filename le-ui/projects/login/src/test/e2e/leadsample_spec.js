'use strict';

describe('lead samples', function() {

    var loginPage = require('./po/login.po');
    var modelList = require('./po/modellist.po');
    var modelTabs = require('./po/modeltabs.po');

    it('should validate lead sample tables', function () {
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
        // Select Sample Leads Tab
        //==================================================
        modelTabs.getTabByIndex(2).click();
        browser.waitForAngular();

        //==================================================
        // Check Tables
        //==================================================
        var topSampleTable = element(by.id("topSampleTable"));
        var bottomSampleTable = element(by.id("bottomSampleTable"));
        expect(topSampleTable.isDisplayed()).toBe(true);
        expect(bottomSampleTable.isDisplayed()).toBe(true);

        //==================================================
        // Check Rows
        //==================================================
        var topSampleBody = topSampleTable.element(by.tagName("tbody"));
        var topSampleRows = topSampleBody.all(by.tagName("tr"));
        var bottomSampleBody = bottomSampleTable.element(by.tagName("tbody"));
        var bottomSampleRows = bottomSampleBody.all(by.tagName("tr"));
        expect(topSampleRows.count()).toEqual(10);
        expect(bottomSampleRows.count()).toEqual(10);

        //==================================================
        // Check TopSample Values
        //==================================================
        checkCompanies(topSampleRows);
        checkContacts(topSampleRows);
        checkConversions(topSampleRows, 7, 3);
        checkScores(topSampleRows);

        //==================================================
        // Check BottomSample Values
        //==================================================
        checkCompanies(bottomSampleRows);
        checkContacts(bottomSampleRows);
        checkConversions(bottomSampleRows, 0, 10);
        checkScores(bottomSampleRows);

        //==================================================
        // Logout
        //==================================================
        loginPage.logout();
    });

    //==================================================
    // Check Companies
    //==================================================
    function checkCompanies(rows) {
        var tds, companies = {};
        for (var i = 0, j = 0; i < 10; i++) {
            tds = rows.get(i).all(by.tagName("td"));
            tds.get(0).getText().then(function (value) {
                expect(value ? true : false).toBe(true);
                expect(value.length ? true : false).toBe(true);
                companies[value] = true;
                if (j++ === 9) {
                    expect(Object.keys(companies).length).toEqual(10);
                }
            });
        }
    }

    //==================================================
    // Check Contacts
    //==================================================
    function checkContacts(rows) {
        var tds;
        for (var i = 0; i < 10; i++) {
            tds = rows.get(i).all(by.tagName("td"));
            tds.get(1).getText().then(function (value) {
                expect(value ? true : false).toBe(true);
                expect(value.length ? true : false).toBe(true);
            });
        }
    }

    //==================================================
    // Check Conversions
    //==================================================
    function checkConversions(rows, expectedConverted, expectedNotConverted) {
        var tds, converted = 0, notConverted = 0;
        for (var i = 0, j = 0; i < 10; i++) {
            tds = rows.get(i).all(by.tagName("td"));
            tds.get(2).element(by.tagName("i")).getAttribute('class').then(function (value) {
                if (value === "fa fa-check ng-hide") notConverted++; else converted++;
                if (j++ === 9) {
                    expect(converted).toEqual(expectedConverted);
                    expect(notConverted).toEqual(expectedNotConverted);
                }
            });
        }
    }

    //==================================================
    // Check Scores
    //==================================================
    function checkScores(rows) {
        var tds;
        for (var i = 0; i < 10; i++) {
            tds = rows.get(i).all(by.tagName("td"));
            tds.get(3).getText().then(function (value) {
                expect(value ? true : false).toBe(true);
                expect(value).toEqual(String(parseInt(value)));
            });
        }
    }
});
