var _ = require('underscore');

describe('model list', function() {

    var params = browser.params;

    var loginPage = require('./po/login.po');
    var logoutPage = require('./po/logout.po');
    var tenants = require('./po/tenantselection.po');
    var modellist = require('./po/modellist.po');
    var model, originalModelName;
    var createdDates;

    function parstDate(s) {
        var parts = s.split('/');
        return new Date(parts[2],parts[0]-1,parts[1]).getTime();
    }

    it('login as non admin', function () {
        loginPage.loginAsNonAdmin();
        tenants.selectTenantByIndex(params.tenantIndex);
    }, 60000);

    it('should verify at least 2 models', function () {
        element.all(by.css('a.model')).count().then(function(n){
            createdDates = _.range(n).map(function(){ return 0; });
            expect(n >= 2).toBe(true);
        });
    });

    it('should be able to go into the detail page', function () {
        element.all(by.css('a.model')).first().click();
        expect(element(by.css('a.back-button')).isDisplayed()).toBe(true);
    });

    it('should be able to go back', function () {
        element(by.css('a.back-button')).click();
    });

    it('should save original model names and create dates', function () {
        element.all(by.css('dd.created-date')).each(function(elem, index){
            elem.getInnerHtml().then(function(text){
                createdDates[index] = parstDate(text);
            });
        });
    });

    it('should order models by created date', function () {
        for (var i = 0; i < createdDates.length - 1; i++) {
            expect(createdDates[i] >= createdDates[i + 1]).toBe(true);
        }
    });

    it('should verify no name editing icon or deleting icon', function () {
        expect(element(by.css('i.fa-pencil')).isPresent()).toBe(false);
        expect(element(by.css('i.fa-trash-o')).isPresent()).toBe(false);
    });

    it('should log out', function () {
        logoutPage.logoutAsNonAdmin();
    });

    it('login as admin', function () {
        loginPage.loginAsAdmin();
        tenants.selectTenantByIndex(params.tenantIndex);
    });

    it('should see name editing icons and deleting icons', function () {
        expect(element.all(by.css('i.fa-pencil')).first().isDisplayed()).toBe(true);
        expect(element.all(by.css('i.fa-trash-o')).first().isDisplayed()).toBe(true);
    });

    it('should alert for empty name', function () {
        element.all(by.xpath(modellist.xpath.ModelTileWidget)).first().getWebElement().then(
            // the first model
            function(webElem){
                model = webElem;
                model.findElement(By.css('h2.editable')).getInnerHtml().then(function(text){
                    originalModelName = text;
                });
                model.findElement(By.css('i.fa-pencil')).click();
                model.findElement(By.xpath(modellist.xpath.ModelNameInput)).clear();
                model.findElement(By.xpath(modellist.xpath.SaveModelName)).click();
                expect(model.findElement(By.xpath(modellist.xpath.EditModelNameError)).isDisplayed()).toBe(true);
            }
        );
    });

    it('should be able to cancel editing model name', function () {
        model.findElement(By.xpath(modellist.xpath.CancelEditModelName)).click();
        expect(browser.driver.isElementPresent(By.xpath(modellist.xpath.ModelNameInput))).toBe(false);
        model.findElement(By.css('h2.editable')).getInnerHtml().then(function(text){
            expect(text).toEqual(originalModelName);
        });
    });

    it('should alert for long name', function () {
        model.findElement(By.css('i.fa-pencil')).click();
        model.findElement(By.xpath(modellist.xpath.ModelNameInput)).clear();

        var longName = "long long long long long long long long long long long long long " +
            "long long long long long long long long long long long long long long long";
        model.findElement(By.xpath(modellist.xpath.ModelNameInput)).sendKeys(longName);
        model.findElement(By.xpath(modellist.xpath.SaveModelName)).click();

        expect(model.findElement(By.xpath(modellist.xpath.EditModelNameError)).isDisplayed()).toBe(true);
    });

    it('should be able to change model name', function () {
        model.findElement(By.xpath(modellist.xpath.ModelNameInput)).clear();
        model.findElement(By.xpath(modellist.xpath.ModelNameInput)).sendKeys("Testing Name");
        model.findElement(By.xpath(modellist.xpath.SaveModelName)).click();
    });

    it('should confirm the new name', function () {
        expect(element(by.xpath(modellist.xpath.ModelNameInput)).isPresent()).toBe(false);
        element.all(by.xpath(modellist.xpath.ModelTileWidget)).first().then(function(elem){
            expect(elem.element(by.css('h2.editable')).getInnerHtml()).toEqual("Testing Name");
        });
    });

    it('should alert for duplicate name', function () {
        element.all(by.xpath(modellist.xpath.ModelTileWidget)).last().getWebElement().then(
            // the first model
            function(webElem){
                model = webElem;
                model.findElement(By.css('i.fa-pencil')).click();
                model.findElement(By.xpath(modellist.xpath.ModelNameInput)).clear();
                model.findElement(By.xpath(modellist.xpath.ModelNameInput)).sendKeys("Testing Name");
                model.findElement(By.xpath(modellist.xpath.SaveModelName)).click();
                expect(element(by.xpath(modellist.xpath.EditModelNameError)).isDisplayed()).toBe(true);
                model.findElement(By.xpath(modellist.xpath.CancelEditModelName)).click();
            }
        );
    });

    it('should reset model names', function () {
        element.all(by.xpath(modellist.xpath.ModelTileWidget)).first().then(function(elem){
            elem.element(by.css('i.fa-pencil')).click();
            elem.element(by.model('data.name')).clear();
            elem.element(by.model('data.name')).sendKeys(originalModelName);
            elem.element(by.buttonText('SAVE')).click();
        });
    });

    it('should log out', function () {
        logoutPage.logoutAsAdmin();
    }, 60000);
});

