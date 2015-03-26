'use strict';

describe('model detail', function() {

    var params = browser.params;

    var loginPage = require('./po/login.po');
    var tenants = require('./po/tenantselection.po');
    var modelList = require('./po/modellist.po');
    var modelTabs = require('./po/modeltabs.po');
    var logoutPage = require('./po/logout.po');

    it('should validate model details', function () {
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
        var container = element(by.id("modelDetailContainer"));

        checkHeader(container);
        checkMoreDisplayed(container, false);

        clickToggleButton(container);

        checkMoreDisplayed(container, true);
        checkMoreValues(container);

        clickToggleButton(container);

        checkMoreDisplayed(container, false);
    };

    //==================================================
    // Helpers
    //==================================================
    function sleep() {
        browser.driver.sleep(1000);
    };

    function clickToggleButton(container) {
        container.element(by.id("detail-toggle")).click(); sleep();
    };

    function checkHeader(container) {
        checkText(container.element(by.id("detail-display-name")));
        checkTextValue(container.element(by.id("detail-status")), ["Inactive", "Active"]);
        checkDate(container.element(by.id("detail-created")));
    };

    function checkMoreDisplayed(container, expected) {
        checkDisplayed(container.element(by.id("moreDataPoints")), expected);
        checkDisplayed(container.element(by.id("detail-total-leads")), expected);
        checkDisplayed(container.element(by.id("detail-test-leads")), expected);
        checkDisplayed(container.element(by.id("detail-train-leads")), expected);
        checkDisplayed(container.element(by.id("detail-conversions")), expected);
        checkDisplayed(container.element(by.id("detail-conversion-rate")), expected);
        checkDisplayed(container.element(by.id("detail-internal-attributes")), expected);
        checkDisplayed(container.element(by.id("detail-external-attributes")), expected);
    };

    function checkMoreValues(container) {
        checkInt(container.element(by.id("detail-total-leads")));
        checkInt(container.element(by.id("detail-test-leads")));
        checkInt(container.element(by.id("detail-train-leads")));
        checkInt(container.element(by.id("detail-conversions")));
        checkFloat(container.element(by.id("detail-conversion-rate")), 1);
        checkInt(container.element(by.id("detail-internal-attributes")));
        checkInt(container.element(by.id("detail-external-attributes")));
    };

    function checkText(elem) {
        elem.isPresent().then(function (present) {
            expect(present).toBe(true);
            elem.isDisplayed().then(function (displayed) {
                expect(displayed).toBe(true);
                elem.getText().then(function (text) {
                    expect(text ? true : false).toBe(true);
                });
            });
        });
    };

    function checkTextValue(elem, expectedText) {
        elem.isPresent().then(function (present) {
            expect(present).toBe(true);
            elem.isDisplayed().then(function (displayed) {
                expect(displayed).toBe(true);
                elem.getText().then(function (text) {
                    expect(text ? true : false).toBe(true);
                    var found = false;
                    for (var i = 0; i < expectedText.length; i++) {
                        if (text === expectedText[i]) {
                            found = true; break;
                        }
                    }
                    expect(found).toBe(true);
                });
            });
        });
    };

    function checkDate(elem) {
        elem.isPresent().then(function (present) {
            expect(present).toBe(true);
            elem.isDisplayed().then(function (displayed) {
                expect(displayed).toBe(true);
                elem.getText().then(function (text) {
                    expect(text ? true : false).toBe(true);

                    var tokens = text.split("/");
                    expect(tokens.length).toBe(3);

                    var year = parseInt(tokens[2]);
                    var month = parseInt(tokens[0]);
                    var day = parseInt(tokens[1]);

                    var date = new Date(year, month - 1, day);
                    expect(date.getFullYear()).toBe(year);
                    expect(date.getMonth()).toBe(month - 1);
                    expect(date.getDate()).toBe(day);
                });
            });
        });
    };

    function checkDisplayed(elem, expected) {
        elem.isPresent().then(function (present) {
            expect(present).toBe(true);
            elem.isDisplayed().then(function (displayed) {
                expect(displayed).toBe(expected);
            });
        });
    };

    function checkInt(elem) {
        elem.isPresent().then(function (present) {
            expect(present).toBe(true);
            elem.isDisplayed().then(function (displayed) {
                expect(displayed).toBe(true);
                elem.getInnerHtml().then(function (text) {
                    expect(text ? true : false).toBe(true);
                    text = text.replace(",", "");
                    expect(text === String(parseInt(text))).toBe(true);
                });
            });
        });
    };

    function checkFloat(elem, trim) {
        elem.isPresent().then(function (present) {
            expect(present).toBe(true);
            elem.isDisplayed().then(function (displayed) {
                expect(displayed).toBe(true);
                elem.getInnerHtml().then(function (text) {
                    expect(text ? true : false).toBe(true);
                    text = text.slice(0, -trim);
                    expect(text === String(parseFloat(text))).toBe(true);
                });
            });
        });
    };
});
