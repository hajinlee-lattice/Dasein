var _ = require('underscore');

describe('model list', function() {

    var params = browser.params;

    var loginPage = require('./po/login.po');
    var logoutPage = require('./po/logout.po');
    var tenants = require('./po/tenantselection.po');
    var userDropdown = require('./po/userdropdown.po');
    var modelName1;
    var modelName2;
    var createdDates;

    function parstDate(s) {
        var parts = s.split('/');
        return new Date(parts[2],parts[0]-1,parts[1]).getTime();
    }

    it('login as non admin', function () {
        loginPage.loginAsNonAdmin();

        tenants.getTenantByIndex(params.tenantIndex).click();
    }, 60000);

    it('should verify at least 2 models', function () {
        element.all(by.css('a.model')).count().then(function(n){
            createdDates = _.range(n).map(function(){ return 0; });
            expect(n >= 2).toBe(true);
        });
    });

    it('should be able to go into the detail page', function () {
        element.all(by.css('a.model')).first().click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        expect(element(by.css('a.back-button')).isDisplayed()).toBe(true);
    });

    it('should be able to go back', function () {
        element(by.css('a.back-button')).click();
    });

    it('should save original model names and create dates', function () {
        element.all(by.css('h2.editable')).first().getInnerHtml().then(function(text){
            modelName1 = text;
        });
        element.all(by.css('h2.editable')).last().getInnerHtml().then(function(text){
            modelName2 = text;
        });
        element.all(by.css('dd.created-date')).each(function(elem, index){
            elem.getInnerHtml().then(function(text){
                createdDates[index] = parstDate(text);
            });
        });
    });

    it('should order models by created date', function () {
        for (var i = 0; i < createdDates.length - 1; i++) {
            expect(createdDates[i] - createdDates[i + 1]  > 0).toBe(true);
        }
    });

    it('should verify no name editing icon or deleting icon', function () {
        expect(element(by.css('i.fa-pencil')).isPresent()).toBe(false);
        expect(element(by.css('i.fa-trash-o')).isPresent()).toBe(false);
    });

    it('should log out', function () {
        logoutPage.logoutAsNonAdmin();
    }, 60000);

    it('login as admin', function () {
        loginPage.loginAsAdmin();
        tenants.getTenantByIndex(params.tenantIndex).click();
    }, 60000);

    it('should see name editing icons and deleting icons', function () {
        expect(element.all(by.css('i.fa-pencil')).first().isDisplayed()).toBe(true);
        expect(element.all(by.css('i.fa-trash-o')).first().isDisplayed()).toBe(true);
    });

    it('should log out', function () {
        logoutPage.logoutAsAdmin();
    }, 60000);
});

