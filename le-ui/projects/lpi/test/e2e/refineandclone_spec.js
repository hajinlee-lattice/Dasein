'use strict';

describe('refine and clone tests', function () {

    var helper = require('./po/helper.po');
    var loginPage = require('./po/login.po');
    var siderbar = require('./po/siderbar.po');
    var modelList = require('./po/modellist.po');
    var manageFields = require('./po/managefields.po');

    it('should validate that you can click refine & clone link to show refine & clone page', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Click Refine & Clone Link
        //==================================================
        goToRefineAndClonePage();
        helper.elementExists(element(by.id("manageFieldsWidget")), true);

        loginPage.logout();
    });

    it('should validate that you can filter fields', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Filter Fields
        //==================================================
        goToRefineAndClonePage();
        var grid = element(by.id('fieldsGrid'));
        manageFields.testFilters(grid);

        loginPage.logout();
    });

    it('should validate that you can click edit and cancel buttons', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Click Edit/Cancel Buttons and Field Name Link
        //==================================================
        goToRefineAndClonePage();
        var grid = element(by.id('fieldsGrid'));
        manageFields.testEditable(grid);

        loginPage.logout();
    });
/*
    it('should validate that you can edit category', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Edit Category Name and Roll Back
        //==================================================
        goToRefineAndClonePage();
        var grid = element(by.id('fieldsGrid'));
        manageFields.testEditCategory(grid);

        loginPage.logout();
    });

    it('should validate that you can edit approved usage', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Edit Approved Usage and Roll Back
        //==================================================
        goToRefineAndClonePage();
        var grid = element(by.id('fieldsGrid'));
        manageFields.testEditApprovedUsage(grid);

        loginPage.logout();
    });

    it('should validate that you can edit attributes type', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Edit Attributes Type and Roll Back
        //==================================================
        goToRefineAndClonePage();
        var grid = element(by.id('fieldsGrid'));
        manageFields.testEditAttributesType(grid);

        loginPage.logout();
    });

    it('should validate that you can edit statistical type', function () {
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Edit Statistical Type and Roll Back
        //==================================================
        goToRefineAndClonePage();
        var grid = element(by.id('fieldsGrid'));
        manageFields.testEditStatisticalType(grid);

        loginPage.logout();
    });
*/
    function goToRefineAndClonePage() {
        modelList.getAnyModel().click();
        browser.waitForAngular();
        browser.driver.sleep(3000);
        siderbar.RefineAndCloneLink.click();
        browser.waitForAngular();
        browser.driver.sleep(30000);
    }

});