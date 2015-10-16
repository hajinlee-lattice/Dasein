'use strict';

describe('manage fields tests', function () {

    var helper = require('./po/helper.po');
    var loginPage = require('./po/login.po');
    var userDropdown = require('./po/userdropdown.po');
    var manageFields = require('./po/managefields.po');

    it('should validate that you can click manage field link to show manage fields page', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Click Manage Fields Link
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SetupLink.isPresent().then(function (present){
            if (present) {
                userDropdown.SetupLink.click();
                browser.waitForAngular();
                browser.driver.sleep(3000);

                helper.elementExists(element(by.id("setup")), true);
                helper.elementExists(element(by.id("manageFieldsWidget")), true);
            }
        });

        loginPage.logout();
    });

    it('should validate that you can filter fields', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Filter Fields
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SetupLink.isPresent().then(function (present){
            if (present) {
                clickSetupLink();
                var grid = element(by.id('fieldsGrid'));
                grid.isDisplayed().then(function (displayed) {
                    if (displayed) {
                        manageFields.testFilters(grid);
                    }
                });
            }
        });

        loginPage.logout();
    });

    it('should validate that you can click edit and cancel buttons', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Click Edit/Cancel Buttons and Field Name Link
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SetupLink.isPresent().then(function (present){
            if (present) {
                clickSetupLink();
                var grid = element(by.id('fieldsGrid'));
                grid.isDisplayed().then(function (displayed) {
                    if (displayed) {
                        manageFields.testEditable(grid);
                    }
                });
            }
        });

        loginPage.logout();
    });

    it('should validate that you can edit display name', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Edit Display Name and Roll Back
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SetupLink.isPresent().then(function (present){
            if (present) {
                clickSetupLink();
                var grid = element(by.id('fieldsGrid'));
                grid.isDisplayed().then(function (displayed) {
                    if (displayed) {
                        manageFields.testEditDisplayName(grid);
                    }
                });
            }
        });

        loginPage.logout();
    });

    it('should validate that you can edit tags', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Edit Tags and Roll Back
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SetupLink.isPresent().then(function (present){
            if (present) {
                clickSetupLink();
                var grid = element(by.id('fieldsGrid'));
                grid.isDisplayed().then(function (displayed) {
                    if (displayed) {
                        manageFields.testEditTags(grid);
                    }
                });
            }
        });

        loginPage.logout();
    });

    it('should validate that you can edit category', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Edit Category Name and Roll Back
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SetupLink.isPresent().then(function (present){
            if (present) {
                clickSetupLink();
                var grid = element(by.id('fieldsGrid'));
                grid.isDisplayed().then(function (displayed) {
                    if (displayed) {
                        manageFields.testEditCategory(grid);
                    }
                });
            }
        });

        loginPage.logout();
    });

    it('should validate that you can edit approved usage', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Edit Approved Usage and Roll Back
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SetupLink.isPresent().then(function (present){
            if (present) {
                clickSetupLink();
                var grid = element(by.id('fieldsGrid'));
                grid.isDisplayed().then(function (displayed) {
                    if (displayed) {
                        manageFields.testEditApprovedUsage(grid);
                    }
                });
            }
        });

        loginPage.logout();
    });

    it('should validate that you can edit fundamental type', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Edit Fundamental Type and Roll Back
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SetupLink.isPresent().then(function (present){
            if (present) {
                clickSetupLink();
                var grid = element(by.id('fieldsGrid'));
                grid.isDisplayed().then(function (displayed) {
                    if (displayed) {
                        manageFields.testEditFundamentalType(grid);
                    }
                });
            }
        });

        loginPage.logout();
    });

    it('should validate that you can edit description', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Edit Description and Roll Back
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SetupLink.isPresent().then(function (present){
            if (present) {
                clickSetupLink();
                var grid = element(by.id('fieldsGrid'));
                grid.isDisplayed().then(function (displayed) {
                    if (displayed) {
                        manageFields.testEditDescription(grid);
                    }
                });
            }
        });

        loginPage.logout();
    });

    it('should validate that you can edit display discretization', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Edit Display Discretization and Roll Back
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SetupLink.isPresent().then(function (present){
            if (present) {
                clickSetupLink();
                var grid = element(by.id('fieldsGrid'));
                grid.isDisplayed().then(function (displayed) {
                    if (displayed) {
                        manageFields.testEditDisplayDiscretization(grid);
                    }
                });
            }
        });

        loginPage.logout();
    });

    it('should validate that you can edit statistical type', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Edit Statistical Type and Roll Back
        //==================================================
        userDropdown.toggleDropdown();
        browser.waitForAngular();
        userDropdown.SetupLink.isPresent().then(function (present){
            if (present) {
                clickSetupLink();
                var grid = element(by.id('fieldsGrid'));
                grid.isDisplayed().then(function (displayed) {
                    if (displayed) {
                        manageFields.testEditStatisticalType(grid);
                    }
                });
            }
        });

        loginPage.logout();
    });

    function clickSetupLink() {
        userDropdown.SetupLink.click();
        browser.waitForAngular();
        browser.driver.sleep(12000);
    }

});