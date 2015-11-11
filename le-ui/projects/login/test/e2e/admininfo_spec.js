'use strict';

describe('lead samples', function() {

    var helper = require('./po/helper.po');
    var loginPage = require('./po/login.po');
    var modelList = require('./po/modellist.po');
    var modelTabs = require('./po/modeltabs.po');

    it('should prevent external users from seeing the link', function () {
        loginPage.loginAsExternalUser();
        maySeeAdminInfoLink(false);
        loginPage.logout();

        loginPage.loginAsExternalAdmin();
        maySeeAdminInfoLink(false);
        loginPage.logout();
    });

    it('should allow internal users to see the link', function () {
        loginPage.loginAsInternalUser();
        maySeeAdminInfoLink(true);
        loginPage.logout();

        loginPage.loginAsInternalAdmin();
        maySeeAdminInfoLink(true);
        loginPage.logout();

        loginPage.loginAsSuperAdmin();
        maySeeAdminInfoLink(true);
        loginPage.logout();
    });


    function maySeeAdminInfoLink(expected) {
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

        adminInfoLinkExists(expected);
    }

    function adminInfoLinkExists(expected) {
        helper.elementExists(element(by.id('adminLink')), expected);
    }

});
