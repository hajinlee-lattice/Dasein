'use strict';

describe('ManageFieldsServiceSpec Tests', function () {
    var stringUtility,
        resourceUtility,
        manageFieldsService,
        fields;

    beforeEach(function () {
        module('mainApp.appCommon.utilities.StringUtility');
        module('mainApp.appCommon.utilities.ResourceUtility');
        module('mainApp.appCommon.services.ManageFieldsService');
        module('test.testData.ManageFieldsTestDataService');

        inject(['StringUtility', 'ResourceUtility', 'ManageFieldsService', 'ManageFieldsTestDataService',
            function (StringUtility, ResourceUtility, ManageFieldsService, ManageFieldsTestDataService) {
                stringUtility = StringUtility;
                resourceUtility = ResourceUtility;
                manageFieldsService = ManageFieldsService;
                fields = ManageFieldsTestDataService.GetSampleFields();
            }
        ]);
    });

    //==================================================
    // Get All Selects Options Tests
    //==================================================
    describe('manage-fields get all selects options tests', function () {
        it('should return a object contains sources/objects/categories/allOptions', function () {
            var warnings = manageFieldsService.GetOptionsForSelects(fields);
            var expectedObj = {
                sourcesToSelect: ["Lattice Data Cloud", "Marketo", "Salesforce"],
                categoriesToSelect: ["Lead Information", "Marketing Activity"],
                allOptions: [
                    ["Marketo", "Lead Information"],
                    ["Marketo", "Marketing Activity"],
                    ["Salesforce", "Marketing Activity"],
                    ["Lattice Data Cloud", "Lead Information"]
                ]
            }

            expect(warnings.sourcesToSelect.length).toEqual(expectedObj.sourcesToSelect.length);
            for (var i = 0; i < warnings.sourcesToSelect.length; i++) {
                expect(warnings.sourcesToSelect[i]).toEqual(expectedObj.sourcesToSelect[i]);
            }
            expect(warnings.categoriesToSelect.length).toEqual(expectedObj.categoriesToSelect.length);
            for (var i = 0; i < warnings.categoriesToSelect.length; i++) {
                expect(warnings.categoriesToSelect[i]).toEqual(expectedObj.categoriesToSelect[i]);
            }
            expect(warnings.allOptions.length).toEqual(expectedObj.allOptions.length);
            for (var i = 0; i < warnings.allOptions.length; i++) {
                expect(warnings.allOptions[i].length).toEqual(expectedObj.allOptions[i].length);
                for (var j = 0; j < warnings.allOptions[i].length; j++) {
                    expect(warnings.allOptions[i][j]).toEqual(expectedObj.allOptions[i][j]);
                }
            }
        });
    });

    //==================================================
    // Category Editable Tests
    //==================================================
    describe('manage-fields category editable tests', function () {
        it('should return true means category editable', function () {
            var dataItem = { Tags: "Internal" };
            expect(manageFieldsService.CategoryEditable(dataItem)).toBe(true);
        });

        it('should return false means category not editable', function () {
            var dataItem = { Tags: "External" };
            expect(manageFieldsService.CategoryEditable(dataItem)).toBe(false);
        });
    });

});