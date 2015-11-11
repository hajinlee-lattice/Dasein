'use strict';

describe('ModelAlertsServiceSpec Tests', function () {
    var stringUtility,
        resourceUtility,
        modelAlertsService,
        modelAlerts,
        suppressedCategories,
        filter;

    beforeEach(function () {
        module('mainApp.appCommon.utilities.StringUtility');
        module('mainApp.appCommon.utilities.ResourceUtility');
        module('mainApp.appCommon.services.ModelAlertsService');
        module('test.testData.ModelAlertsTestDataService');
        module('test.testData.SuppressedCategoriesTestDataService');

        inject(['StringUtility', 'ResourceUtility', 'ModelAlertsService', 'ModelAlertsTestDataService', 'SuppressedCategoriesTestDataService', '$filter',
            function (StringUtility, ResourceUtility, ModelAlertsService, ModelAlertsTestDataService, SuppressedCategoriesTestDataService, $filter) {
                stringUtility = StringUtility;
                resourceUtility = ResourceUtility;
                modelAlertsService = ModelAlertsService;
                modelAlerts = ModelAlertsTestDataService.GetSampleModelAlerts();
                suppressedCategories = SuppressedCategoriesTestDataService.GetTwoSampleSuppressedCategories();
                filter = $filter;
            }
        ]);
    });

    //==================================================
    // ModelAlerts Contains All Warnings Tests
    //==================================================
    describe('model-alerts contains all warnings tests', function () {
        it('model-alerts all', function () {
            var warnings = modelAlertsService.GetWarnings(modelAlerts, suppressedCategories);
            expect(warnings.noModelQualityWarnings).toBe(false);
            expect(warnings.noMissingMetaDataWarnings).toBe(false);

            expect(warnings.modelQualityWarnings.length).toEqual(6);
            expect(warnings.modelQualityWarningsTitle).toEqual(resourceUtility.getString('ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_TITLE'));
            expect(warnings.modelQualityWarningsLabel).toEqual(resourceUtility.getString('ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_LABEL'));
            var modelQualityWarnings = modelAlerts.ModelQualityWarnings;
            checkSuccessEventsWarning(warnings.modelQualityWarnings[0], modelQualityWarnings);
            checkConversionPercentageWarning(warnings.modelQualityWarnings[1], modelQualityWarnings);
            checkRocScoreWarning(warnings.modelQualityWarnings[2], modelAlerts.ModelQualityWarnings);
            checkExcessiveDiscreteValuesWarning(warnings.modelQualityWarnings[3], modelQualityWarnings);
            checkExcessivePredictiveWarning(warnings.modelQualityWarnings[4], modelQualityWarnings);
            checkExcessivePredictiveNullValuesWarning(warnings.modelQualityWarnings[5], modelQualityWarnings);

            expect(warnings.missingMetaDataWarnings.length).toEqual(6);
            expect(warnings.missingMetaDataWarningsTitle).toEqual(resourceUtility.getString('ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_TITLE'));
            expect(warnings.missingMetaDataWarningsLabel).toEqual(resourceUtility.getString('ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_LABEL'));
            var missingMetaDataWarnings = modelAlerts.MissingMetaDataWarnings;
            checkInvalidApprovedUsageWarning(warnings.missingMetaDataWarnings[0], missingMetaDataWarnings);
            checkInvalidTagsWarning(warnings.missingMetaDataWarnings[1], missingMetaDataWarnings);
            checkInvalidCategoryWarning(warnings.missingMetaDataWarnings[2], missingMetaDataWarnings);
            checkInvalidDisplayNameWarning(warnings.missingMetaDataWarnings[3], missingMetaDataWarnings);
            checkInvalidStatisticalTypeWarning(warnings.missingMetaDataWarnings[4], missingMetaDataWarnings);
            checkExcessiveCategoriesWarning(warnings.missingMetaDataWarnings[5], suppressedCategories);
        });
    });

    //==================================================
    // ModelAlerts No Warnings Tests
    //==================================================
    describe('model-alerts no warnings tests', function () {
        it('model-alerts none', function () {
            var alerts = {
                "ModelQualityWarnings": {},
                "MissingMetaDataWarnings": {}
            };
            var suppressedCategories = [];
            var warnings = modelAlertsService.GetWarnings(alerts, suppressedCategories);
            expect(warnings.noModelQualityWarnings).toBe(true);
            expect(warnings.noMissingMetaDataWarnings).toBe(true);
            expect(warnings.noWarning).toEqual(resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_NO_WARNING"));
        });
    });

    //==================================================
    // ModelAlerts Warning One By One Tests
    //==================================================
    describe('model-alerts warning one by one tests', function () {
        it('model-alerts success events warning', function () {
            var modelAlerts = {
                "ModelQualityWarnings": {
                    "LowSuccessEvents": 437,
                    "MinSuccessEvents": 500
                },
                "MissingMetaDataWarnings": {}
            };
            var suppressedCategories = [];
            var warnings = modelAlertsService.GetWarnings(modelAlerts, suppressedCategories);
            expect(warnings.noModelQualityWarnings).toBe(false);
            expect(warnings.noMissingMetaDataWarnings).toBe(true);
            expect(warnings.modelQualityWarnings.length).toEqual(1);
            checkSuccessEventsWarning(warnings.modelQualityWarnings[0], modelAlerts.ModelQualityWarnings);
        });

        it('model-alerts conversion percentage warning', function () {
            var modelAlerts = {
                "ModelQualityWarnings": {
                    "LowConversionPercentage": 0.8,
                    "MinConversionPercentage": 1.0
                },
                "MissingMetaDataWarnings": {}
            };
            var suppressedCategories = [];
            var warnings = modelAlertsService.GetWarnings(modelAlerts, suppressedCategories);
            expect(warnings.noModelQualityWarnings).toBe(false);
            expect(warnings.noMissingMetaDataWarnings).toBe(true);
            expect(warnings.modelQualityWarnings.length).toEqual(1);
            checkConversionPercentageWarning(warnings.modelQualityWarnings[0], modelAlerts.ModelQualityWarnings);
        });

        it('model-alerts roc scoring warning', function () {
            var modelAlerts = {
                "ModelQualityWarnings": {
                    "OutOfRangeRocScore": 0.62,
                    "MinRocScore": 0.7,
                    "MaxRocScore": 0.85
                },
                "MissingMetaDataWarnings": {}
            };
            var suppressedCategories = [];
            var warnings = modelAlertsService.GetWarnings(modelAlerts, suppressedCategories);
            expect(warnings.noModelQualityWarnings).toBe(false);
            expect(warnings.noMissingMetaDataWarnings).toBe(true);
            expect(warnings.modelQualityWarnings.length).toEqual(1);
            checkRocScoreWarning(warnings.modelQualityWarnings[0], modelAlerts.ModelQualityWarnings);
        });

        it('model-alerts excessive discrete values warning', function () {
            var modelAlerts = {
                "ModelQualityWarnings": {
                    "ExcessiveDiscreteValuesAttributes": ["attribute1", "attribute2"],
                    "MaxNumberOfDiscreteValues": 200,
                },
                "MissingMetaDataWarnings": {}
            };
            var suppressedCategories = [];
            var warnings = modelAlertsService.GetWarnings(modelAlerts, suppressedCategories);
            expect(warnings.noModelQualityWarnings).toBe(false);
            expect(warnings.noMissingMetaDataWarnings).toBe(true);
            expect(warnings.modelQualityWarnings.length).toEqual(1);
            checkExcessiveDiscreteValuesWarning(warnings.modelQualityWarnings[0], modelAlerts.ModelQualityWarnings);
        });

        it('model-alerts excessive predictive warning', function () {
            var modelAlerts = {
                "ModelQualityWarnings": {
                    "ExcessivePredictiveAttributes": [{
                            "key": "attribuite1",
                            "value": "0.12"
                        }
                    ],
                    "MaxFeatureImportance": 0.1
                },
                "MissingMetaDataWarnings": {}
            };
            var suppressedCategories = [];
            var warnings = modelAlertsService.GetWarnings(modelAlerts, suppressedCategories);
            expect(warnings.noModelQualityWarnings).toBe(false);
            expect(warnings.noMissingMetaDataWarnings).toBe(true);
            expect(warnings.modelQualityWarnings.length).toEqual(1);
            checkExcessivePredictiveWarning(warnings.modelQualityWarnings[0], modelAlerts.ModelQualityWarnings);
        });

        it('model-alerts excessive predictive null values warning', function () {
            var modelAlerts = {
                "ModelQualityWarnings": {
                    "ExcessivePredictiveNullValuesAttributes": [{
                            "key": "attribuite1",
                            "value": "1.3"
                        }
                    ],
                    "MaxLiftForNull": 1.0
                },
                "MissingMetaDataWarnings": {}
            };
            var suppressedCategories = [];
            var warnings = modelAlertsService.GetWarnings(modelAlerts, suppressedCategories);
            expect(warnings.noModelQualityWarnings).toBe(false);
            expect(warnings.noMissingMetaDataWarnings).toBe(true);
            expect(warnings.modelQualityWarnings.length).toEqual(1);
            checkExcessivePredictiveNullValuesWarning(warnings.modelQualityWarnings[0], modelAlerts.ModelQualityWarnings);
        });

        it('model-alerts invalid approved usage warning', function () {
            var modelAlerts = {
                "ModelQualityWarnings": {},
                "MissingMetaDataWarnings": {
                    "InvalidApprovedUsageAttributes": [
                        "attribuite1"
                    ]
                }
            };
            var suppressedCategories = [];
            var warnings = modelAlertsService.GetWarnings(modelAlerts, suppressedCategories);
            expect(warnings.noModelQualityWarnings).toBe(true);
            expect(warnings.noMissingMetaDataWarnings).toBe(false);
            expect(warnings.missingMetaDataWarnings.length).toEqual(1);
            checkInvalidApprovedUsageWarning(warnings.missingMetaDataWarnings[0], modelAlerts.MissingMetaDataWarnings);
        });

        it('model-alerts invalid tags warning', function () {
            var modelAlerts = {
                "ModelQualityWarnings": {},
                "MissingMetaDataWarnings": {
                    "InvalidTagsAttributes": [
                        "attribuite1"
                    ]
                }
            };
            var suppressedCategories = [];
            var warnings = modelAlertsService.GetWarnings(modelAlerts, suppressedCategories);
            expect(warnings.noModelQualityWarnings).toBe(true);
            expect(warnings.noMissingMetaDataWarnings).toBe(false);
            expect(warnings.missingMetaDataWarnings.length).toEqual(1);
            checkInvalidTagsWarning(warnings.missingMetaDataWarnings[0], modelAlerts.MissingMetaDataWarnings);
        });

        it('model-alerts invalid category warning', function () {
            var modelAlerts = {
                "ModelQualityWarnings": {},
                "MissingMetaDataWarnings": {
                    "InvalidCategoryAttributes": [
                        "attribuite1"
                    ]
                }
            };
            var suppressedCategories = [];
            var warnings = modelAlertsService.GetWarnings(modelAlerts, suppressedCategories);
            expect(warnings.noModelQualityWarnings).toBe(true);
            expect(warnings.noMissingMetaDataWarnings).toBe(false);
            expect(warnings.missingMetaDataWarnings.length).toEqual(1);
            checkInvalidCategoryWarning(warnings.missingMetaDataWarnings[0], modelAlerts.MissingMetaDataWarnings);
        });

        it('model-alerts invalid display name warning', function () {
            var modelAlerts = {
                "ModelQualityWarnings": {},
                "MissingMetaDataWarnings": {
                    "InvalidDisplayNameAttributes": [
                        "attribuite1"
                    ]
                }
            };
            var suppressedCategories = [];
            var warnings = modelAlertsService.GetWarnings(modelAlerts, suppressedCategories);
            expect(warnings.noModelQualityWarnings).toBe(true);
            expect(warnings.noMissingMetaDataWarnings).toBe(false);
            expect(warnings.missingMetaDataWarnings.length).toEqual(1);
            checkInvalidDisplayNameWarning(warnings.missingMetaDataWarnings[0], modelAlerts.MissingMetaDataWarnings);
        });

        it('model-alerts invalid statistical type warning', function () {
            var modelAlerts = {
                "ModelQualityWarnings": {},
                "MissingMetaDataWarnings": {
                    "InvalidStatisticalTypeAttributes": [
                        "attribuite1"
                    ]
                }
            };
            var suppressedCategories = [];
            var warnings = modelAlertsService.GetWarnings(modelAlerts, suppressedCategories);
            expect(warnings.noModelQualityWarnings).toBe(true);
            expect(warnings.noMissingMetaDataWarnings).toBe(false);
            expect(warnings.missingMetaDataWarnings.length).toEqual(1);
            checkInvalidStatisticalTypeWarning(warnings.missingMetaDataWarnings[0], modelAlerts.MissingMetaDataWarnings);
        });

        it('model-alerts excessive categories warning', function () {
            var modelAlerts = {
                "ModelQualityWarnings": {},
                "MissingMetaDataWarnings": {}
            };
            var suppressedCategories = [];
            var category = {
                    name: "category1",
                    categoryName: "category1",
                    UncertaintyCoefficient: 0.1,
                    size: 1,
                    color: null,
                    children: []
                };
            suppressedCategories.push(category);
            var warnings = modelAlertsService.GetWarnings(modelAlerts, suppressedCategories);
            expect(warnings.noModelQualityWarnings).toBe(true);
            expect(warnings.noMissingMetaDataWarnings).toBe(false);
            expect(warnings.missingMetaDataWarnings.length).toEqual(1);
            checkExcessiveCategoriesWarning(warnings.missingMetaDataWarnings[0], suppressedCategories);
        });
    });

    function checkSuccessEventsWarning(warning, modelQualityWarnings) {
        var title = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_SUCCESS_EVENTS_TOO_LOWER_TITLE");
        expect(warning.title).toEqual(title);
        var description = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ADJUST_FILTERS_EVENT_DEFINITION");
        expect(warning.description).toEqual(description);
        var successEvents = filter('number')(modelQualityWarnings.LowSuccessEvents, 0);
        expect(warning.successEvents).toEqual(successEvents);
    }

    function checkConversionPercentageWarning(warning, modelQualityWarnings) {
        var title = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_CONVERSION_RATE_TOO_LOWER_TITLE");
        expect(warning.title).toEqual(title);
        var description = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ADJUST_FILTERS_EVENT_DEFINITION");
        expect(warning.description).toEqual(description);
        var conversionPercentage = filter('number')(modelQualityWarnings.LowConversionPercentage, 1);
        expect(warning.conversionPercentage).toEqual(conversionPercentage);
        expect(warning.minValue).toEqual(modelQualityWarnings.MinConversionPercentage);
    }

    function checkRocScoreWarning(warning, modelQualityWarnings) {
        var title = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ROC_SCORE_OUTSIDE_CREDIBLE_RANGE");
        expect(warning.title).toEqual(title);
        var description = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ADJUST_FILTERS_EVENT_DEFINITION");
        expect(warning.description).toEqual(description);
        var rocScore = filter('number')(modelQualityWarnings.OutOfRangeRocScore, 2);
        expect(warning.rocScore).toEqual(rocScore);
        expect(warning.minValue).toEqual(modelQualityWarnings.MinRocScore);
        expect(warning.maxValue).toEqual(modelQualityWarnings.MaxRocScore);
    }

    function checkExcessiveDiscreteValuesWarning(warning, modelQualityWarnings) {
        var title = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ATTRIBUTES_OVERLY_DISCRETE_VALUES_TITLE");
        expect(warning.title).toEqual(title);
        var description = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ATTRIBUTES_REMOVED_FROM_MODELING");
        expect(warning.description).toEqual(description);
        var impactedLabel = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
        expect(warning.impactedLabel).toEqual(impactedLabel);
        var impactedContent = joinStringList(modelQualityWarnings.ExcessiveDiscreteValuesAttributes, 0);
        expect(warning.impactedContent).toEqual(impactedContent);
        var count = filter('number')(modelQualityWarnings.ExcessiveDiscreteValuesAttributes.length, 0);
        expect(warning.count).toEqual(count);
        var maxValue = filter('number')(modelQualityWarnings.MaxNumberOfDiscreteValues, 0);
        expect(warning.maxValue).toEqual(maxValue);
    }

    function checkExcessivePredictiveWarning(warning, modelQualityWarnings) {
        var title = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ATTRIBUTES_OVERLY_PREDICTIVE_TITLE");
        expect(warning.title).toEqual(title);
        var description = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ATTRIBUTES_CONTAIN_FUTURE_INFORMATION_REQUIRE_EM_JSUTIFICATION_VALID_PREDICTOR");
        expect(warning.description).toEqual(description);
        var impactedLabel = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
        expect(warning.impactedLabel).toEqual(impactedLabel);
        var impactedContent = joinMapList(modelQualityWarnings.ExcessivePredictiveAttributes, 1);
        expect(warning.impactedContent).toEqual(impactedContent);
        var count = filter('number')(modelQualityWarnings.ExcessivePredictiveAttributes.length, 0);
        expect(warning.count).toEqual(count);
        expect(warning.maxValue).toEqual(modelQualityWarnings.MaxFeatureImportance);
    }

    function checkExcessivePredictiveNullValuesWarning(warning, modelQualityWarnings) {
        var title = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ATTRIBUTES_HIGHLY_PREDICTIVE_NULL_VALUES");
        expect(warning.title).toEqual(title);
        var description = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ATTRIBUTES_CONFIRM_ABSENCE_DATA_HAS_BUSINESS_MEANING");
        expect(warning.description).toEqual(description);
        var impactedLabel = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
        expect(warning.impactedLabel).toEqual(impactedLabel);
        var impactedContent = joinMapList(modelQualityWarnings.ExcessivePredictiveNullValuesAttributes, 1);
        expect(warning.impactedContent).toEqual(impactedContent);
        var count = filter('number')(modelQualityWarnings.ExcessivePredictiveNullValuesAttributes.length, 0);
        expect(warning.count).toEqual(count);
        expect(warning.maxValue).toEqual(modelQualityWarnings.MaxLiftForNull);
    }

    function checkInvalidApprovedUsageWarning(warning, missingMetaDataWarnings) {
        var title = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_INVALID_APPROVED_USAGE");
        expect(warning.title).toEqual(title);
        var description = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_SUPPRESSED_FROM_TOP_PREDICTORS_UI_CSV_BUYER_INSIGHTS");
        expect(warning.description).toEqual(description);
        var impactedLabel = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
        expect(warning.impactedLabel).toEqual(impactedLabel);
        var impactedContent = joinStringList(missingMetaDataWarnings.InvalidApprovedUsageAttributes);
        expect(warning.impactedContent).toEqual(impactedContent);
        var count = filter('number')(missingMetaDataWarnings.InvalidApprovedUsageAttributes.length, 0);
        expect(warning.count).toEqual(count);
    }

    function checkInvalidTagsWarning(warning, missingMetaDataWarnings) {
        var title = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_INVALID_TAGS");
        expect(warning.title).toEqual(title);
        var description = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_SUPPRESSED_FROM_TOP_PREDICTORS_UI_CSV_BUYER_INSIGHTS");
        expect(warning.description).toEqual(description);
        var impactedLabel = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
        expect(warning.impactedLabel).toEqual(impactedLabel);
        var impactedContent = joinStringList(missingMetaDataWarnings.InvalidTagsAttributes);
        expect(warning.impactedContent).toEqual(impactedContent);
        var count = filter('number')(missingMetaDataWarnings.InvalidTagsAttributes.length, 0);
        expect(warning.count).toEqual(count);
    }

    function checkInvalidCategoryWarning(warning, missingMetaDataWarnings) {
        var title = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_INVALID_CATEGORY");
        expect(warning.title).toEqual(title);
        var description = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_SUPPRESSED_FROM_TOP_PREDICTORS_UI_CSV_BUYER_INSIGHTS");
        expect(warning.description).toEqual(description);
        var impactedLabel = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
        expect(warning.impactedLabel).toEqual(impactedLabel);
        var impactedContent = joinStringList(missingMetaDataWarnings.InvalidCategoryAttributes);
        expect(warning.impactedContent).toEqual(impactedContent);
        var count = filter('number')(missingMetaDataWarnings.InvalidCategoryAttributes.length, 0);
        expect(warning.count).toEqual(count);
    }

    function checkInvalidDisplayNameWarning(warning, missingMetaDataWarnings) {
        var title = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_INVALID_DISPLAY_NAME");
        expect(warning.title).toEqual(title);
        var description = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_SUPPRESSED_FROM_TOP_PREDICTORS_UI_CSV_BUYER_INSIGHTS");
        expect(warning.description).toEqual(description);
        var impactedLabel = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
        expect(warning.impactedLabel).toEqual(impactedLabel);
        var impactedContent = joinStringList(missingMetaDataWarnings.InvalidDisplayNameAttributes);
        expect(warning.impactedContent).toEqual(impactedContent);
        var count = filter('number')(missingMetaDataWarnings.InvalidDisplayNameAttributes.length, 0);
        expect(warning.count).toEqual(count);
    }

    function checkInvalidStatisticalTypeWarning(warning, missingMetaDataWarnings) {
        var title = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_INVALID_STATISTICAL_TYPE");
        expect(warning.title).toEqual(title);
        var description = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_BUCKETED_INCORRECTLY_IN_TOP_PREDICTORS_UI_CSV_BUYER_INSIGHTS");
        expect(warning.description).toEqual(description);
        var impactedLabel = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
        expect(warning.impactedLabel).toEqual(impactedLabel);
        var impactedContent = joinStringList(missingMetaDataWarnings.InvalidStatisticalTypeAttributes);
        expect(warning.impactedContent).toEqual(impactedContent);
        var count = filter('number')(missingMetaDataWarnings.InvalidStatisticalTypeAttributes.length, 0);
        expect(warning.count).toEqual(count);
    }

    function checkExcessiveCategoriesWarning(warning, suppressedCategories) {
        var title = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_MODEL_SUMMARY_CONTAINS_OVERLY_CATEFORIES");
        expect(warning.title).toEqual(title);
        var description = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_TWO_CATEFORIES_AND_ATTRIBUTES_SUPPRESSED_FROM_TOP_PREDICTORS_UI");
        expect(warning.description).toEqual(description);
        var impactedLabel = resourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_CATEGORIES");
        expect(warning.impactedLabel).toEqual(impactedLabel);
        var categoryNames = getCategoryNames(suppressedCategories);
        var impactedContent = joinStringList(categoryNames);
        expect(warning.impactedContent).toEqual(impactedContent);
        var count = filter('number')(categoryNames.length + 8, 0);
        expect(warning.count).toEqual(count);
    }

    function getCategoryNames(categories) {
        var categoryNames = [];
        if (categories == null) {
            return categoryNames;
        }
        for (var i = 0; i < categories.length; i++) {
            var category = categories[i];
            categoryNames.push(category.name);
        }
        return categoryNames;
    }

    function joinMapList(elements, fractionSize) {
        var content = "";
        var length = elements.length;
        for (var i = 0; i < length; i++) {
            var element = elements[i];
            var value = fractionSize != null ? filter('number')(element.value, fractionSize) : element.value;
            content += element.key + " (" + value + ")";
            if (i + 1 < length) {
                content += ", ";
            }
        }

        return content;
    }

    function joinStringList(elements) {
        var content = "";
        var length = elements.length;
        for (var i = 0; i < length; i++) {
            content += elements[i];
            if (i + 1 < length) {
                content += ", ";
            }
        }

        return content;
    }

});