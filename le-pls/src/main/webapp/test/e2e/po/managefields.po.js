'use strict';

var ManageFields = function() {
    var helper = require('./helper.po');

    var AllTagsOptions = ["Internal", "External"];
    var AllCategoryOptions = ["Lead Information", "Marketing Activity"];
    var AllStatisticalTypeOptions = ["interval", "nominal", "ordinal", "ratio"];
    var AllApprovedUsageOptions = ["None", "Model", "ModelAndAllInsights", "ModelAndModelInsights"];
    var AllFundamentalTypeOptions = ["boolean", "currency", "numeric", "percentage", "year"];

    this.testFilters = function(grid) {
        var table = grid.element(by.tagName('table'));
        helper.elementExists(table, true);
        var tbody = table.element(by.tagName("tbody"));

        testFieldNameFilter(grid, tbody);

        var sourceOptions = element(by.model("source")).all(by.tagName('option'));
        testOptionFilter(sourceOptions, 1, grid, tbody);

        var categoryOptions = element(by.model("category")).all(by.tagName('option'));
        testOptionFilter(categoryOptions, 4, grid, tbody);

        testWarningFilter(grid, tbody);
    };

    function testFieldNameFilter(grid, tbody) {
        var nameFilter = "fe";
        element(by.model("field")).sendKeys(nameFilter);
        element(by.id("manage-fields-search")).click();
        browser.waitForAngular();
        browser.driver.sleep(1500);
        fieldNameFilterAssert(grid, tbody, nameFilter);
        element(by.model("field")).clear();
        element(by.id("manage-fields-search")).click();
        browser.waitForAngular();
        browser.driver.sleep(1500);
    }

    function fieldNameFilterAssert(grid, tbody, nameFilter) {
        var nextBtn = grid.element(by.css(".k-i-arrow-e")).element(by.xpath(".."));
        nextBtn.getAttribute("class").then(function(classVal) {
            browser.wait(function() {
                var deferred = protractor.promise.defer();

                var trs = tbody.all(by.tagName("tr"));
                trs.count().then(function (count) {
                    if (count == 0) {
                        deferred.fulfill(true);
                    }

                    trs.each(function(tr, index) {
                        var tds = tr.all(by.tagName("td"));
                        tds.get(0).getText().then(function(fieldName) {
                            tds.get(2).getText().then(function(displayName) {
                                var contains = false;
                                if (fieldName != null) {
                                    contains = fieldName.toLowerCase().indexOf(nameFilter.toLowerCase()) > -1;
                                }
                                if (!contains && displayName != null) {
                                    contains = displayName.toLowerCase().indexOf(nameFilter.toLowerCase()) > -1;
                                }
                                expect(contains).toBe(true);

                                if (index + 1 == count) {
                                    deferred.fulfill(true);
                                }
                            });
                        });
                    });
                });

                return deferred.promise;
            });

            if (classVal.indexOf("k-state-disabled") < 0) {
                nextBtn.click();
                browser.waitForAngular();
                browser.driver.sleep(1000);

                fieldNameFilterAssert(grid, tbody, nameFilter);
            }
        });
    }

    function testOptionFilter(options, colIndex, grid, tbody) {
        options.count().then(function(count) {
            if (count > 1) {
                options.get(1).getText().then(function(filterVal) {
                    options.get(1).click();
                    browser.waitForAngular();
                    browser.driver.sleep(1500);
                    optionFilterAssert(filterVal, colIndex, grid, tbody);
                    options.get(0).click();
                    browser.waitForAngular();
                    browser.driver.sleep(1500);
                });
            }
        });
    }

    function optionFilterAssert(filterVal, colIndex, grid, tbody) {
        var nextBtn = grid.element(by.css(".k-i-arrow-e")).element(by.xpath(".."));
        nextBtn.getAttribute("class").then(function(classVal) {
            browser.wait(function() {
                var deferred = protractor.promise.defer();

                var trs = tbody.all(by.tagName("tr"));
                trs.count().then(function (count) {
                    if (count == 0) {
                        deferred.fulfill(true);
                    }
                    trs.each(function(tr, index) {
                        tr.all(by.tagName("td")).get(colIndex).getText().then(function(content) {
                            expect(content == filterVal).toBe(true);

                            if (index + 1 == count) {
                                deferred.fulfill(true);
                            }
                        });
                    });
                });

                return deferred.promise;
            });

            if (classVal.indexOf("k-state-disabled") < 0) {
                nextBtn.click();
                browser.waitForAngular();
                browser.driver.sleep(1000);
                optionFilterAssert(filterVal, colIndex, grid, tbody);
            }
        });
    }

    function testWarningFilter(grid, tbody) {
        element(by.model("onlyShowErrorFields")).click();
        browser.waitForAngular();
        browser.driver.sleep(1500);
        warningFilterAssert(grid, tbody);
        element(by.model("onlyShowErrorFields")).click();
        browser.waitForAngular();
        browser.driver.sleep(1500);
    }

    function warningFilterAssert(grid, tbody) {
        var nextBtn = grid.element(by.css(".k-i-arrow-e")).element(by.xpath(".."));
        nextBtn.getAttribute("class").then(function(classVal) {
            browser.wait(function() {
                var deferred = protractor.promise.defer();

                var trs = tbody.all(by.tagName("tr"));
                trs.count().then(function (count) {
                    if (count == 0) {
                        deferred.fulfill(true);
                    }
                    trs.each(function(tr, index) {
                        expect(tr.all(by.css("td .warning")).count()).toBeGreaterThan(0);
                        if (index + 1 == count) {
                            deferred.fulfill(true);
                        }
                    });
                });

                return deferred.promise;
            });

            if (classVal.indexOf("k-state-disabled") < 0) {
                nextBtn.click();
                browser.waitForAngular();
                browser.driver.sleep(1000);
                warningFilterAssert(grid, tbody);
            }
        });
    }

    this.testBatchEditable = function(grid) {
        var table = grid.element(by.tagName('table'));
        helper.elementExists(table, true);
        var tbody = table.element(by.tagName("tbody"));
        var trs = tbody.all(by.tagName("tr"));
        trs.count().then(function (count) {
            if (count > 0) {
                assertBatchEditable(trs.get(0));
            }
        });
    };

    function assertBatchEditable(tr) {
        element(by.id("manage-fields-edit")).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        helper.elementExists(tr.all(by.tagName("td")).get(2).element(by.tagName("input")), true);
        var tagsSelect = tr.all(by.tagName("td")).get(3).element(by.tagName("select"));
        helper.elementExists(tagsSelect, true);
        assertSelectOptions(tagsSelect, AllTagsOptions);
        tagsSelect.element(by.css("option[selected]")).getText().then(function (selectedVal){
            if (selectedVal == "Internal") {
                assertCategorySelectExists(tr, AllCategoryOptions);
                tagsSelect.element(by.css("option[label='External']")).click();
                browser.waitForAngular();
                browser.driver.sleep(500);
                assertCategorySelectNotExists(tr);
            } else {
                assertCategorySelectNotExists(tr);
                tagsSelect.element(by.css("option[label='Internal']")).click();
                browser.waitForAngular();
                browser.driver.sleep(500);
                assertCategorySelectExists(tr, AllCategoryOptions);
            }
        });
        var approvedUsageSelect = tr.all(by.tagName("td")).get(5).element(by.tagName("select"));
        helper.elementExists(approvedUsageSelect, true);
        assertSelectOptions(approvedUsageSelect, AllApprovedUsageOptions);
        var fundamentalTypeSelect = tr.all(by.tagName("td")).get(6).element(by.tagName("select"));
        helper.elementExists(fundamentalTypeSelect, true);
        assertSelectOptions(fundamentalTypeSelect, AllFundamentalTypeOptions);

        element(by.id("manage-fields-cancel")).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
        element(by.id("discard-edit-yes")).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);
    }

    function assertCategorySelectExists(tr, AllCategoryOptions) {
        var categorySelect = tr.all(by.tagName("td")).get(4).element(by.tagName("select"));
        helper.elementExists(categorySelect, true);
        assertSelectOptions(categorySelect, AllCategoryOptions);
    }

    function assertCategorySelectNotExists(tr) {
        var categorySelect = tr.all(by.tagName("td")).get(4).element(by.tagName("select"));
        expect(categorySelect.isPresent()).toBe(false);
    }

    function assertSelectOptions(select, allOptions) {
        var options = select.all(by.tagName("option"));
        options.count().then(function (count) {
            var items = [];
            for (var i = 0; i < allOptions.length; i++) {
                items.push(allOptions[i]);
            }
            options.each(function (option, index) {
                option.getText().then(function(value) {
                    if (value != null && value != "") {
                        var idx = items.indexOf(value);
                        if (idx > -1) {
                            items.splice(idx, 1);
                        }
                    }
                    if (index + 1 == count) {
                        expect(items.length == 0).toBe(true);
                    }
                });
            });
        });
    }

    this.testBatchEdit = function(grid) {
        var table = grid.element(by.tagName('table'));
        helper.elementExists(table, true);
        var tbody = table.element(by.tagName("tbody"));

        var trs = tbody.all(by.tagName("tr"));
        trs.count().then(function (count) {
            if (count > 0) {
                assertBatchEdit(grid);
            }
        });
    };

    function assertBatchEdit(grid) {
        var table = grid.element(by.tagName('table'));
        helper.elementExists(table, true);
        var tbody = table.element(by.tagName("tbody"));
        var trs = tbody.all(by.tagName("tr"));

        /*// Sort by Tags descending
        var ths = table.element(by.tagName("thead")).all(by.tagName("th"));
        ths.get(2).click();
        browser.waitForAngular();
        browser.driver.sleep(1500);
        ths.get(2).click();
        browser.waitForAngular();
        browser.driver.sleep(1500);
        */

        element(by.id("manage-fields-edit")).click();
        browser.waitForAngular();
        browser.driver.sleep(1000);

        var trObj = null;
        trs.each(function (tr) {
            if (trObj != null) {
                return;
            }
            var tds = tr.all(by.tagName("td"));
            var displayNameInput = tds.get(2).element(by.tagName("input"));
            displayNameInput.getAttribute('value').then(function (displayName) {
                var tagsSelect = tds.get(3).element(by.tagName("select"));
                tagsSelect.element(by.css("option[selected]")).getText().then(function (tags){
                    if (tags != null && tags != "") {
                        var categorySelect = tds.get(4).element(by.tagName("select"));
                        categorySelect.isPresent().then(function (present){
                            if (!present) {
                                return;
                            }
                            categorySelect.element(by.css("option[selected]")).getText().then(function (category){
                                if (category != null && category != "") {
                                    var approvedUsageSelect = tds.get(5).element(by.tagName("select"));
                                    approvedUsageSelect.element(by.css("option[selected]")).getText().then(function (approvedUsage){
                                        if (approvedUsage != null && approvedUsage != "") {
                                            var fundamentalTypeSelect = tds.get(6).element(by.tagName("select"));
                                            fundamentalTypeSelect.element(by.css("option[selected]")).getText().then(function (fundamentalType){
                                                if (fundamentalType != null && fundamentalType != "" && trObj == null) {
                                                    trObj = {};
                                                    trObj.tr = tr;
                                                    trObj.displayNameInput = displayNameInput;
                                                    trObj.displayName = displayName;
                                                    trObj.tagsSelect = tagsSelect;
                                                    trObj.tags = tags;
                                                    trObj.categorySelect = categorySelect;
                                                    trObj.category = category;
                                                    trObj.approvedUsageSelect = approvedUsageSelect;
                                                    trObj.approvedUsage =approvedUsage;
                                                    trObj.fundamentalTypeSelect = fundamentalTypeSelect;
                                                    trObj.fundamentalType = fundamentalType;
                                                    batchEidt(trObj);
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        });
                    }
                });
            });
        });
    }

    function batchEidt(trObj) {
        if (trObj.tags != "Internal") {
            trObj.tagsSelect.element(by.css("option[label='Internal']")).click();
            browser.waitForAngular();
            browser.driver.sleep(500);
        }
        var newDisplayName = "1DisplayName_e2eTest";
        var newCategory = (trObj.category == "Lead Information") ? "Marketing Activity" : "Lead Information";
        var newApprovedUsage = (trObj.approvedUsage == "None") ? "Model" : "None";
        var newFundamentalType = (trObj.fundamentalType == "boolean") ? "currency" : "boolean";
        trObj.displayNameInput.sendKeys(newDisplayName);
        browser.waitForAngular();
        browser.driver.sleep(500);
        trObj.categorySelect.element(by.css("option[label='" + newCategory + "']")).click();
        browser.waitForAngular();
        browser.driver.sleep(500);
        trObj.approvedUsageSelect.element(by.css("option[label='" + newApprovedUsage + "']")).click();
        browser.waitForAngular();
        browser.driver.sleep(500);
        trObj.fundamentalTypeSelect.element(by.css("option[label='" + newFundamentalType + "']")).click();
        browser.waitForAngular();
        browser.driver.sleep(500);

        element(by.id("manage-fields-save")).click();
        browser.waitForAngular();
        browser.driver.sleep(20000);

    }

}

module.exports = new ManageFields();