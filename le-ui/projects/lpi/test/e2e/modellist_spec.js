var _ = require('underscore');

describe('model list', function() {

    var loginPage = require('./po/login.po');
    var modellist = require('./po/modellist.po');
    var siderbar = require('./po/siderbar.po');
    var model, originalModelName;
    var createdDates;

    function parstDate(s) {
        var parts = s.split('/');
        return new Date(parts[2],parts[0]-1,parts[1]).getTime();
    }

    it('login as non admin', function () {
        loginPage.loginAsExternalUser();
    }, 60000);

    it('should verify at least 2 models', function () {
        element.all(by.css('div.model')).count().then(function(n){
            createdDates = _.range(n).map(function(){ return 0; });
            expect(n >= 2).toBe(true);
        });
    });

    it('should be able to go into the detail page', function () {
        element.all(by.css('div.model')).first().click();
        expect(element(by.css('#nav .menu-header > a')).isDisplayed()).toBe(true);
    });

    it('should be able to go back', function () {
        element(by.css('#nav .menu-header > a')).click();
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
        loginPage.logout();
    });

    it('login as admin', function () {
        loginPage.loginAsSuperAdmin();
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
        model.findElement(By.css('h2.editable')).getText().then(function(text){
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
        var elem = element.all(by.xpath(modellist.xpath.ModelTileWidget)).first();
        expect(elem.element(by.css('h2.editable')).getText()).toEqual("Testing Name");
    });

    it('should alert for duplicate name', function () {
        element.all(by.xpath(modellist.xpath.ModelTileWidget)).last().getWebElement().then(
            // the last model
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
        var elem = element.all(by.xpath(modellist.xpath.ModelTileWidget)).first();
        elem.element(by.css('i.fa-pencil')).click();
        elem.element(by.model('data.name')).clear();
        elem.element(by.model('data.name')).sendKeys(originalModelName);
        elem.element(by.buttonText('Save')).click();
    });

    it('should be able to delete model', function() {
        element.all(by.xpath(modellist.xpath.ModelTileWidget)).count().then(function(count) {
            if (count != 0) {
                element.all(by.xpath(modellist.xpath.ModelTileWidget)).first().getWebElement().then(
                    // the first element
                    function(webElem) {
                        model = webElem;
                        model.findElement(by.css('h2.editable')).getText().then(function(text){
                            originalModelName = text;
                        });
                        model.findElement(by.css('i.fa-trash-o')).click();
                        browser.waitForAngular();
                        browser.driver.sleep(1000);
                        element(by.xpath(modellist.xpath.DeleteModelLink)).click();
                        browser.waitForAngular();
                        browser.driver.sleep(1000);
                        expect(element.all(by.xpath(modellist.xpath.ModelTileWidget)).count()).toBe(count - 1);
                    }
                );
            }
        });
    });

    it("should be able to see the deleted model in the creation history, restore the model and see it getting restored", function() {
        // navigate to creation history table
        siderbar.ModelCreationHistoryLink.click();
        browser.waitForAngular();
        browser.driver.sleep(2000);

        assertDeletedModelShowsupAsDeletedInModelCreationHistoryPage(originalModelName);
        undoDeletionOfModelInHistoryPage_makeMakeSureThatModelIsRestored(originalModelName);
    });

    function assertDeletedModelShowsupAsDeletedInModelCreationHistoryPage(modelName) {
        if (modelName) {
            element(by.cssContainingText('tr', modelName)).getText().then(function(text) {
                expect(text).toContain('Deleted');
            });
        }
    }

    function undoDeletionOfModelInHistoryPage_makeMakeSureThatModelIsRestored(modelName) {
        if (modelName) {
            var deletedModelTableRow = element(by.cssContainingText('tr', modelName));
            deletedModelTableRow.element(by.linkText('Undo')).click();
            browser.waitForAngular();
            browser.driver.sleep(1000);

            // make sure that the model name shows up as not "Deleted" in the model creation history
            element(by.cssContainingText('tr', modelName)).getText().then(function(text) {
                expect(text).not.toContain('Deleted');
            });

            // navigate back to the models page
            siderbar.PredictionModelsLink.click();
            browser.waitForAngular();
            browser.driver.sleep(1000);

            // make sure the model with the deleted model name is present
            expect(element(by.cssContainingText('h2', modelName)).isPresent()).toBe(true);
        }
    }

    it('should log out', function () {
        loginPage.logout();
    });
});

