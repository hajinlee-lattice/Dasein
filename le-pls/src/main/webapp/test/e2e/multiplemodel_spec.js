'use strict';

describe('multiple model setup tests', function () {

    var loginPage = require('./po/login.po');
    var userDropdown = require('./po/userdropdown.po');
    var multipleModelSetup = require('./po/multiplemodelsetup.po');

    it('should validate that you can go to the Multiple Models page', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select Multiple Models Tab
        //==================================================
        userDropdown.toggleDropdown();
        expect(userDropdown.ManageUsersLink.isDisplayed()).toBe(true);
        userDropdown.MultipleModelsLink.click();
        browser.waitForAngular();

        loginPage.logout();
    });
    
    /*it('should validate that you can add a new segment', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select Multiple Models Tab
        //==================================================
        userDropdown.toggleDropdown();
        userDropdown.MultipleModelsLink.click();
        browser.waitForAngular();
        
        //==================================================
        // Add a New Segment
        //==================================================
        multipleModelSetup.clickAddSegment();
        var newName = new Date().toString();
        multipleModelSetup.addNewSegment(newName);
        expect(element(by.css('.global-error > span')).getText()).toBe("");
        
        
        loginPage.logout();
    });*/
    
    /*it('should validate that you can delete a segment', function () {
        //==================================================
        // Login
        //==================================================
        loginPage.loginAsSuperAdmin();

        //==================================================
        // Select Multiple Models Tab
        //==================================================
        userDropdown.toggleDropdown();
        userDropdown.MultipleModelsLink.click();
        browser.waitForAngular();
        
        //==================================================
        // Delete existing segment
        //==================================================
        var currentCount = 0;
        element.all(by.css('tbody > tr')).then(function(segments) {
            currentCount = segments.length;
        });
        element.all(by.css('.delete-user-link')).last().click();
        browser.waitForAngular();
        element.all(by.css('tbody > tr')).then(function(segments) {
            expect(segments.length).toBe(currentCount - 1);
        });
        
        loginPage.logout();
    });*/
});