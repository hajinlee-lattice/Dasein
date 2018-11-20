'use strict';

var ManageFields = function() {
    var helper = require('./helper.po');

    var AllCategoryOptions = ["Lead Information", "Marketing Activity", "Firmographics", "Growth Trends",
                              "Online Presence", "Technology Profile", "Website Keywords", "Website Profile"];
    var AllStatisticalTypeOptions = ["interval", "nominal", "ordinal", "ratio"];
    var AllApprovedUsageOptions = ["None", "Model", "ModelAndAllInsights", "ModelAndModelInsights"];
    var AllAttributesTypeOptions = ["alpha", "boolean", "currency", "numeric", "percentage", "year"];

    this.testFilters = function(grid) {
        var table = grid.element(by.tagName('table'));
        helper.elementExists(table, true);

        testFieldNameFilter(grid);

        var categoryOptions = element(by.model("category")).all(by.tagName('option'));
        testOptionFilter(categoryOptions, 2, grid);

        testWarningFilter(grid);
    };

    function testFieldNameFilter(grid) {
        var nameFilter = "fe";
        element(by.model("field")).sendKeys(nameFilter);
        element(by.id("manage-fields-search")).click();
        sleep(1500);
        fieldNameFilterAssert(grid, nameFilter);
        element(by.model("field")).clear();
        element(by.id("manage-fields-search")).click();
        sleep(1500);
    }

    function fieldNameFilterAssert(grid, nameFilter) {
        var nextBtn = grid.element(by.css(".k-i-arrow-e")).element(by.xpath(".."));
        nextBtn.getAttribute("class").then(function(classVal) {
            var table = grid.element(by.tagName('table'));
            var tbody = table.element(by.tagName("tbody"));
            var trs = tbody.all(by.tagName("tr"));
            trs.count().then(function (count) {
                if (count == 0) {
                    return;
                }
                trs.each(function(tr, index) {
                    var tds = tr.all(by.tagName("td"));
                    tds.get(0).getText().then(function(fieldName) {
                        tds.get(1).getText().then(function(displayName) {
                            var contains = false;
                            if (fieldName != null) {
                                contains = fieldName.toLowerCase().indexOf(nameFilter.toLowerCase()) > -1;
                            }
                            if (!contains && displayName != null) {
                                contains = displayName.toLowerCase().indexOf(nameFilter.toLowerCase()) > -1;
                            }
                            expect(contains).toBe(true);

                            if (index + 1 == count && classVal.indexOf("k-state-disabled") < 0) {
                                nextBtn.click();
                                sleep(1000);
                                fieldNameFilterAssert(grid, nameFilter);
                            }
                        });
                    });
                });
            });
        });
    }

    function testOptionFilter(options, colIndex, grid) {
        options.count().then(function(count) {
            if (count > 1) {
                options.get(1).getText().then(function(filterVal) {
                    options.get(1).click();
                    sleep(1000);
                    optionFilterAssert(filterVal, colIndex, grid);
                    options.get(0).click();
                    sleep(1000);
                });
            }
        });
    }

    function optionFilterAssert(filterVal, colIndex, grid) {
        var nextBtn = grid.element(by.css(".k-i-arrow-e")).element(by.xpath(".."));
        nextBtn.getAttribute("class").then(function(classVal) {
            var table = grid.element(by.tagName('table'));
            var tbody = table.element(by.tagName("tbody"));
            var trs = tbody.all(by.tagName("tr"));
            trs.count().then(function (count) {
                if (count == 0) {
                    return;
                }
                trs.each(function(tr, index) {
                    tr.all(by.tagName("td")).get(colIndex).getText().then(function(content) {
                        expect(content == filterVal).toBe(true);

                        if (index + 1 == count && classVal.indexOf("k-state-disabled") < 0) {
                            nextBtn.click();
                            sleep(1000);
                            optionFilterAssert(filterVal, colIndex, grid);
                        }
                    });
                });
            });
        });
    }

    function testWarningFilter(grid) {
        element(by.model("onlyShowErrorFields")).click();
        sleep(1000);
        warningFilterAssert(grid);
        element(by.model("onlyShowErrorFields")).click();
        sleep(1000);
    }

    function warningFilterAssert(grid) {
        var nextBtn = grid.element(by.css(".k-i-arrow-e")).element(by.xpath(".."));
        nextBtn.getAttribute("class").then(function(classVal) {
            var table = grid.element(by.tagName('table'));
            var tbody = table.element(by.tagName("tbody"));
            var trs = tbody.all(by.tagName("tr"));
            trs.count().then(function (count) {
                if (count == 0) {
                    return;
                }
                trs.each(function(tr, index) {
                    expect(tr.all(by.css("td .warning")).count()).toBeGreaterThan(0);
                    if (index + 1 == count && classVal.indexOf("k-state-disabled") < 0) {
                        nextBtn.click();
                        sleep(1000);
                        warningFilterAssert(grid);
                    }
                });
            });
        });
    }

    this.testEditable = function(grid) {
        filterEditableFields();
        var table = grid.element(by.tagName('table'));
        helper.elementExists(table, true);
        var tbody = table.element(by.tagName("tbody"));
        var trs = tbody.all(by.tagName("tr"));
        trs.count().then(function (count) {
            if (count > 0) {
                assertEditable(trs.get(0));
            }
        });
    };

    function assertEditable(tr) {
        tr.element(by.tagName("a")).click();
        sleep(1000);
        helper.elementExists(element(by.id("edit-field-cancel")), true);
        element(by.id("edit-field-cancel")).click();
        sleep(1000);

        //clickEditFieldsButton();
        //element(by.id("manage-fields-cancel")).click();
        //sleep(1000);
        //helper.elementExists(element(by.id("discard-edit-yes")), false);

        clickEditFieldsButton();
        var tds = tr.all(by.tagName("td"));
        helper.elementExists(tds.get(1).element(by.tagName("input")), true);

        var categorySelect = tds.get(2).element(by.tagName("select"));
        helper.elementExists(categorySelect, true);
        //assertSelectOptions(categorySelect, AllCategoryOptions);

        var approvedUsageSelect = tds.get(3).element(by.tagName("select"));
        helper.elementExists(approvedUsageSelect, true);
        assertSelectOptions(approvedUsageSelect, AllApprovedUsageOptions);

        var attributesTypeSelect = tds.get(4).element(by.tagName("select"));
        helper.elementExists(attributesTypeSelect, true);
        assertSelectOptions(attributesTypeSelect, AllAttributesTypeOptions);
        clickSaveFieldsButton();
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

    this.testEditCategory = function(grid) {
        filterEditableFields();
        var table = grid.element(by.tagName('table'));
        helper.elementExists(table, true);
        var trs = table.element(by.tagName("tbody")).all(by.tagName("tr"));
        trs.count().then(function (count) {
            if (count > 0) {
                clickEditFieldsButton();
                var tds = trs.get(0).all(by.tagName("td"));
                tds.get(0).getText().then(function (fieldName) {
                    var categorySelect = tds.get(2).element(by.tagName("select"));
                    categorySelect.element(by.css("option[selected]")).getText().then(function (category){
                        if (AllCategoryOptions.indexOf(category) > -1) {
                            var newCategory = (category == AllCategoryOptions[0]) ? AllCategoryOptions[1] : AllCategoryOptions[0];
                            categorySelect.element(by.css("option[label='" + newCategory + "']")).click();
                            sleep(500);
                            clickSaveFieldsButton();

                            element(by.model("category")).all(by.tagName('option')).get(0).click();
                            sleep(1000);
                            filterFields(fieldName);
                            clickEditFieldLink(grid, fieldName);
                            element(by.model('field.Category')).element(by.css("option[selected]")).getText().then(function (editedValue){
                                expect(newCategory == editedValue).toBe(true);
                                element(by.model('field.Category')).element(by.css("option[label='" + category + "']")).click();
                                sleep(500);
                                clickSaveFieldButton();

                                clickEditFieldLink(grid, fieldName);
                                element(by.model('field.Category')).element(by.css("option[selected]")).getText().then(function (value){
                                    expect(category == value).toBe(true);
                                });
                            });
                        }
                    });
                });
            }
        });
    };

    this.testEditApprovedUsage = function(grid) {
        filterEditableFields();
        var table = grid.element(by.tagName('table'));
        helper.elementExists(table, true);
        var trs = table.element(by.tagName("tbody")).all(by.tagName("tr"));
        trs.count().then(function (count) {
            if (count > 0) {
                clickEditFieldsButton();
                var tds = trs.get(0).all(by.tagName("td"));
                tds.get(0).getText().then(function (fieldName) {
                    var approvedUsageSelect = tds.get(3).element(by.tagName("select"));
                    approvedUsageSelect.element(by.css("option[selected]")).getText().then(function (approvedUsage){
                        if (AllApprovedUsageOptions.indexOf(approvedUsage) > -1) {
                            var newApprovedUsage = (approvedUsage == AllApprovedUsageOptions[0]) ? AllApprovedUsageOptions[1] : AllApprovedUsageOptions[0];
                            approvedUsageSelect.element(by.css("option[label='" + newApprovedUsage + "']")).click();
                            sleep(500);
                            clickSaveFieldsButton();

                            filterFields(fieldName);
                            clickEditFieldLink(grid, fieldName);
                            element(by.model('field.ApprovedUsage')).element(by.css("option[selected]")).getText().then(function (editedValue){
                                expect(newApprovedUsage == editedValue).toBe(true);
                                element(by.model('field.ApprovedUsage')).element(by.css("option[label='" + approvedUsage + "']")).click();
                                sleep(500);
                                clickSaveFieldButton();

                                clickEditFieldLink(grid, fieldName);
                                element(by.model('field.ApprovedUsage')).element(by.css("option[selected]")).getText().then(function (value){
                                    expect(approvedUsage == value).toBe(true);
                                });
                            });
                        }
                    });
                });
            }
        });
    };

    this.testEditAttributesType = function(grid) {
        filterEditableFields();
        var table = grid.element(by.tagName('table'));
        helper.elementExists(table, true);
        var trs = table.element(by.tagName("tbody")).all(by.tagName("tr"));
        trs.count().then(function (count) {
            if (count > 0) {
                clickEditFieldsButton();
                var tds = trs.get(0).all(by.tagName("td"));
                tds.get(0).getText().then(function (fieldName) {
                    var attributesTypeSelect = tds.get(4).element(by.tagName("select"));
                    attributesTypeSelect.element(by.css("option[selected]")).getText().then(function (fundamentalType){
                        if (AllAttributesTypeOptions.indexOf(fundamentalType) > -1) {
                            var newFundamentalType = (fundamentalType == AllAttributesTypeOptions[0]) ? AllAttributesTypeOptions[1] : AllAttributesTypeOptions[0];
                            attributesTypeSelect.element(by.css("option[label='" + newFundamentalType + "']")).click();
                            sleep(500);
                            clickSaveFieldsButton();

                            filterFields(fieldName);
                            clickEditFieldLink(grid, fieldName);
                            element(by.model('field.FundamentalType')).element(by.css("option[selected]")).getText().then(function (editedValue){
                                expect(newFundamentalType == editedValue).toBe(true);
                                element(by.model('field.FundamentalType')).element(by.css("option[label='" + fundamentalType + "']")).click();
                                sleep(500);
                                clickSaveFieldButton();

                                clickEditFieldLink(grid, fieldName);
                                element(by.model('field.FundamentalType')).element(by.css("option[selected]")).getText().then(function (value){
                                    expect(fundamentalType == value).toBe(true);
                                });
                            });
                        }
                    });
                });
            }
        });
    };

    this.testEditStatisticalType = function(grid) {
        var table = grid.element(by.tagName('table'));
        helper.elementExists(table, true);
        var trs = table.element(by.tagName("tbody")).all(by.tagName("tr"));
        trs.count().then(function (count) {
            if (count > 0) {
                var tds = trs.get(0).all(by.tagName("td"));
                tds.get(0).getText().then(function (fieldName) {
                    clickEditFieldLink(grid, fieldName);
                    element(by.model('field.StatisticalType')).element(by.css("option[selected]")).getText().then(function (statisticalType){
                        if (AllStatisticalTypeOptions.indexOf(statisticalType) > -1) {
                            var newStatisticalType = (statisticalType == AllStatisticalTypeOptions[0]) ? AllStatisticalTypeOptions[1] : AllStatisticalTypeOptions[0];
                            element(by.model('field.StatisticalType')).element(by.css("option[label='" + newStatisticalType + "']")).click();
                            sleep(500);
                            clickSaveFieldButton();

                            clickEditFieldLink(grid, fieldName);
                            element(by.model('field.StatisticalType')).element(by.css("option[selected]")).getText().then(function (editedValue){
                                expect(newStatisticalType == editedValue).toBe(true);

                                element(by.model('field.StatisticalType')).element(by.css("option[label='" + statisticalType + "']")).click();
                                sleep(500);
                                clickSaveFieldButton();

                                clickEditFieldLink(grid, fieldName);
                                element(by.model('field.StatisticalType')).element(by.css("option[selected]")).getText().then(function (value){
                                    expect(statisticalType == value).toBe(true);
                                });
                            });
                        }
                    });
                });
            }
        });
    };

    function sortColumnDesc(table, columnIndex) {
        var ths = table.element(by.tagName("thead")).all(by.tagName("th"));
        ths.get(columnIndex).click();
        sleep(1000);
        ths.get(columnIndex).click();
        sleep(1000);
    }

    function filterEditableFields() {
        filterFields("Title_Length");
    }

    function filterFields(fieldName) {
        var input = element(by.model("field"));
        input.clear();
        input.sendKeys(fieldName);
        element(by.id("manage-fields-search")).click();
        sleep(1500);
    }

    function clickEditFieldsButton() {
        element(by.id("manage-fields-edit")).click();
        sleep(1000);
    }

    function clickSaveFieldsButton() {
        element(by.id("manage-fields-save")).click();
        sleep(5000);
    }

    function clickEditFieldLink(grid, fieldName) {
        var link = grid.element(by.tagName('table')).element(by.linkText(fieldName));
        helper.elementExists(link, true);
        link.click();
        sleep(1000);
    }

    function clickSaveFieldButton() {
        element(by.id("edit-field-save")).click();
        sleep(5000);
    }

    function sleep(ms) {
        browser.waitForAngular();
        browser.driver.sleep(ms);
    }

}

module.exports = new ManageFields();
