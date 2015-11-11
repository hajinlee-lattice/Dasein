'use strict';

describe('ModelSummaryValidationServiceSpec Tests', function () {
    var validationService,
        resourceUtility,
        modelSummary;

    beforeEach(function () {
        module('mainApp.appCommon.services.ModelSummaryValidationService');
        module('test.testData.TopPredictorTestDataService');
        module('mainApp.appCommon.utilities.ResourceUtility');

        inject(['ModelSummaryValidationService', 'TopPredictorTestDataService', 'ResourceUtility',
            function (ModelSummaryValidationService, TopPredictorTestDataService, ResourceUtility) {
                validationService = ModelSummaryValidationService;
                resourceUtility = ResourceUtility;
                modelSummary = TopPredictorTestDataService.GetSampleModelSummary();
            }
        ]);
    });

    //==================================================
    // ModelSummary Success Tests
    //==================================================
    describe('model-summary success tests', function () {
        it('model-summary', function () {
            var summary = modelSummary;
            var errors = validationService.ValidateModelSummary(summary);
            expect(errors.length).toEqual(0);
        });
    });

    //==================================================
    // ModelSummary Error Tests
    //==================================================
    describe('model-summary error tests', function () {
        it('model-summary missing', function () {
            var summary = null;
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_MODEL_SUMMARY_MISSING"))).toBe(true);
        });

        it('model-summary invalid', function () {
            var summary = "nonsense";
            var errors = validationService.ValidateModelSummary(summary);
            expect(errors.length).toEqual(5);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_SEGMENTATIONS_MISSING"))).toBe(true);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTORS_MISSING"))).toBe(true);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_MODEL_DETAILS_MISSING"))).toBe(true);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TOP_SAMPLE_MISSING"))).toBe(true);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_BOTTOM_SAMPLE_MISSING"))).toBe(true);
        });
    });

    //==================================================
    // Segmentations Error Tests
    //==================================================
    describe('segmentations error tests', function () {
        it('segmentations missing', function () {
            var summary = {};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_SEGMENTATIONS_MISSING"))).toBe(true);
        });

        it('segmentations unexpected', function () {
            var summary = {"Segmentations" : []};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_SEGMENTATIONS_UNEXPECTED"))).toBe(true);
        });

        it('segmentations unexpected', function () {
            var summary = {"Segmentations" : ""};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_SEGMENTATIONS_UNEXPECTED"))).toBe(true);
        });

        it('segments missing', function () {
            var summary = {"Segmentations" : [{}]};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_SEGMENTS_MISSING"))).toBe(true);
        });

        it('segments unexpected', function () {
            var summary = {"Segmentations" : [{"Segments" : []}]};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_SEGMENTS_UNEXPECTED"))).toBe(true);
        });

        it('segments unexpected', function () {
            var summary = {"Segmentations" : [{"Segments" : ""}]};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_SEGMENTS_UNEXPECTED"))).toBe(true);
        });

        it('segment-score missing', function () {
            var summary = {"Segmentations" : [{"Segments" : []}]};
            for (var i = 0; i < 100; i++) {
                summary.Segmentations[0].Segments.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_SEGMENT_SCORE_MISSING"), 100)).toBe(true);
        });

        it('segment-score invalid', function () {
            var summary = {"Segmentations" : [{"Segments" : []}]};
            for (var i = 0; i < 100; i++) {
                summary.Segmentations[0].Segments.push({"Score" : ""});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_SEGMENT_SCORE_INVALID"), 100)).toBe(true);
        });

        it('segment-count missing', function () {
            var summary = {"Segmentations" : [{"Segments" : []}]};
            for (var i = 0; i < 100; i++) {
                summary.Segmentations[0].Segments.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_SEGMENT_COUNT_MISSING"), 100)).toBe(true);
        });

        it('segment-count invalid', function () {
            var summary = {"Segmentations" : [{"Segments" : []}]};
            for (var i = 0; i < 100; i++) {
                summary.Segmentations[0].Segments.push({"Count" : ""});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_SEGMENT_COUNT_INVALID"), 100)).toBe(true);
        });

        it('segment-converted missing', function () {
            var summary = {"Segmentations" : [{"Segments" : []}]};
            for (var i = 0; i < 100; i++) {
                summary.Segmentations[0].Segments.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_SEGMENT_CONVERTED_MISSING"), 100)).toBe(true);
        });

        it('segment-converted invalid', function () {
            var summary = {"Segmentations" : [{"Segments" : []}]};
            for (var i = 0; i < 100; i++) {
                summary.Segmentations[0].Segments.push({"Converted" : ""});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_SEGMENT_CONVERTED_INVALID"), 100)).toBe(true);
        });
    });

    //==================================================
    // Predictors Error Tests
    //==================================================
    describe('predictors error tests', function () {
        var num = 100;
        it('predictors missing', function () {
            var summary = {};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTORS_MISSING"))).toBe(true);
        });

        it('predictors unexpected', function () {
            var summary = {"Predictors" : ""};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTORS_UNEXPECTED"))).toBe(true);
        });

        it('predictor-name missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_NAME_MISSING"), num)).toBe(true);
        });

        it('predictor-tags missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_TAGS_MISSING"), num)).toBe(true);
        });

        it('predictor-tags unexpected', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Tags" : ""});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_TAGS_UNEXPECTED"), num)).toBe(true);
        });

        it('predictor-data-type missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_DATA_TYPE_MISSING"), num)).toBe(true);
        });

        it('predictor-display-name missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_DISPLAY_NAME_MISSING"), num)).toBe(true);
        });

        it('predictor-description missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_DESCRIPTION_MISSING"), num)).toBe(true);
        });

        it('predictor-approved-usage missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_APPROVED_USAGE_MISSING"), num)).toBe(true);
        });

        it('predictor-approved-usage unexpected', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"ApprovedUsage" : ""});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_APPROVED_USAGE_UNEXPECTED"), num)).toBe(true);
        });

        it('predictor-fundamental-type missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_FUNDAMENTAL_TYPE_MISSING"), num)).toBe(true);
        });

        it('predictor-category missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_CATEGORY_MISSING"), num)).toBe(true);
        });

        it('predictor-uncertainty-coefficient missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_UNCERTAINTY_COEFFICIENT_MISSING"), num)).toBe(true);
        });

        it('predictor-uncertainty-coefficient invalid', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"UncertaintyCoefficient" : ""});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_UNCERTAINTY_COEFFICIENT_INVALID"), num)).toBe(true);
        });

        it('predictor-elements missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENTS_MISSING"), num)).toBe(true);
        });

        it('predictor-elements unexpected', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : ""});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENTS_UNEXPECTED"), num)).toBe(true);
        });

        it('predictor-element-correlation-sign missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : [{}]});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENT_CORRELATION_SIGN_MISSING"), num)).toBe(true);
        });

        it('predictor-element-correlation-sign invalid', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : [{"CorrelationSign" : ""}]});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENT_CORRELATION_SIGN_INVALID"), num)).toBe(true);
        });

        it('predictor-element-count missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : [{}]});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENT_COUNT_MISSING"), num)).toBe(true);
        });

        it('predictor-element-count invalid', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : [{"Count" : ""}]});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENT_COUNT_INVALID"), num)).toBe(true);
        });

        it('predictor-element-lift missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : [{}]});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENT_LIFT_MISSING"), num)).toBe(true);
        });

        it('predictor-element-lift invalid', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : [{"Lift" : ""}]});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENT_LIFT_INVALID"), num)).toBe(true);
        });

        it('predictor-element-lower-inclusive invalid', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : [{"LowerInclusive" : ""}]});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENT_LOWER_INCLUSIVE_INVALID"), num)).toBe(true);
        });

        it('predictor-element-upper-exclusive invalid', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : [{"UpperExclusive" : ""}]});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENT_UPPER_EXCLUSIVE_INVALID"), num)).toBe(true);
        });

        it('predictor-element-name missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : [{}]});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENT_NAME_MISSING"), num)).toBe(true);
        });

        it('predictor-element-uncertainty-coefficient missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : [{}]});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENT_UNCERTAINTY_COEFFICIENT_MISSING"), num)).toBe(true);
        });

        it('predictor-element-uncertainty-coefficient invalid', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : [{"UncertaintyCoefficient" : ""}]});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENT_UNCERTAINTY_COEFFICIENT_INVALID"), num)).toBe(true);
        });

        it('predictor-element-values missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : [{}]});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENT_VALUES_MISSING"), num)).toBe(true);
        });

        it('predictor-element-values unexpected', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : [{"Values" : ""}]});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENT_VALUES_UNEXPECTED"), num)).toBe(true);
        });

        it('predictor-element-is-visible missing', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : [{}]});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENT_IS_VISIBLE_MISSING"), num)).toBe(true);
        });

        it('predictor-element-is-visible invalid', function () {
            var summary = {"Predictors" : []};
            for (var i = 0; i < num; i++) {
                summary.Predictors.push({"Elements" : [{"IsVisible" : ""}]});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_PREDICTOR_ELEMENT_IS_VISIBLE_INVALID"), num)).toBe(true);
        });
    });

    //==================================================
    // ModelDetails Error Tests
    //==================================================
    describe('model-details error tests', function () {
        it('model-details missing', function () {
            var summary = {};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_MODEL_DETAILS_MISSING"))).toBe(true);
        });

        it('model-name missing', function () {
            var summary = {"ModelDetails" : {"" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_MODEL_NAME_MISSING"))).toBe(true);
        });

        it('total-leads missing', function () {
            var summary = {"ModelDetails" : {"" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TOTAL_LEADS_MISSING"))).toBe(true);
        });

        it('total-leads invalid', function () {
            var summary = {"ModelDetails" : {"TotalLeads" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TOTAL_LEADS_INVALID"))).toBe(true);
        });

        it('testing-leads missing', function () {
            var summary = {"ModelDetails" : {"" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TESTING_LEADS_MISSING"))).toBe(true);
        });

        it('testing-leads invalid', function () {
            var summary = {"ModelDetails" : {"TestingLeads" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TESTING_LEADS_INVALID"))).toBe(true);
        });

        it('training-leads missing', function () {
            var summary = {"ModelDetails" : {"" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TRAINING_LEADS_MISSING"))).toBe(true);
        });

        it('training-leads invalid', function () {
            var summary = {"ModelDetails" : {"TrainingLeads" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TRAINING_LEADS_INVALID"))).toBe(true);
        });

        it('total-conversions missing', function () {
            var summary = {"ModelDetails" : {"" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TOTAL_CONVERSIONS_MISSING"))).toBe(true);
        });

        it('total-conversions invalid', function () {
            var summary = {"ModelDetails" : {"TotalConversions" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TOTAL_CONVERSIONS_INVALID"))).toBe(true);
        });

        it('testing-conversions missing', function () {
            var summary = {"ModelDetails" : {"" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TESTING_CONVERSIONS_MISSING"))).toBe(true);
        });

        it('testing-conversions invalid', function () {
            var summary = {"ModelDetails" : {"TestingConversions" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TESTING_CONVERSIONS_INVALID"))).toBe(true);
        });

        it('training-conversions missing', function () {
            var summary = {"ModelDetails" : {"" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TRAINING_CONVERSIONS_MISSING"))).toBe(true);
        });

        it('training-conversions invalid', function () {
            var summary = {"ModelDetails" : {"TrainingConversions" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TRAINING_CONVERSIONS_INVALID"))).toBe(true);
        });

        it('roc-score missing', function () {
            var summary = {"ModelDetails" : {"" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_ROC_SCORE_MISSING"))).toBe(true);
        });

        it('roc-score invalid', function () {
            var summary = {"ModelDetails" : {"RocScore" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_ROC_SCORE_INVALID"))).toBe(true);
        });

        it('construction-time missing', function () {
            var summary = {"ModelDetails" : {"" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_CONSTRUCTION_TIME_MISSING"))).toBe(true);
        });

        it('construction-time invalid', function () {
            var summary = {"ModelDetails" : {"ConstructionTime" : ""}};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_CONSTRUCTION_TIME_INVALID"))).toBe(true);
        });
    });

    //==================================================
    // TopSample Error Tests
    //==================================================
    describe('top-sample error tests', function () {
        var num = 10;
        it('top-sample missing', function () {
            var summary = {};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TOP_SAMPLE_MISSING"))).toBe(true);
        });

        it('top-sample unexpected', function () {
            var summary = {"TopSample" : ""};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TOP_SAMPLE_UNEXPECTED"))).toBe(true);
        });

        it('top-sample-company missing', function () {
            var summary = {"TopSample" : []};
            for (var i = 0; i < num; i++) {
                summary.TopSample.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TOP_SAMPLE_COMPANY_MISSING"), num)).toBe(true);
        });

        it('top-sample-first-name missing', function () {
            var summary = {"TopSample" : []};
            for (var i = 0; i < num; i++) {
                summary.TopSample.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TOP_SAMPLE_FIRST_NAME_MISSING"), num)).toBe(true);
        });

        it('top-sample-last-name missing', function () {
            var summary = {"TopSample" : []};
            for (var i = 0; i < num; i++) {
                summary.TopSample.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TOP_SAMPLE_LAST_NAME_MISSING"), num)).toBe(true);
        });

        it('top-sample-converted missing', function () {
            var summary = {"TopSample" : []};
            for (var i = 0; i < num; i++) {
                summary.TopSample.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TOP_SAMPLE_CONVERTED_MISSING"), num)).toBe(true);
        });

        it('top-sample-converted invalid', function () {
            var summary = {"TopSample" : []};
            for (var i = 0; i < num; i++) {
                summary.TopSample.push({"Converted" : ""});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TOP_SAMPLE_CONVERTED_INVALID"), num)).toBe(true);
        });

        it('top-sample-score missing', function () {
            var summary = {"TopSample" : []};
            for (var i = 0; i < num; i++) {
                summary.TopSample.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TOP_SAMPLE_SCORE_MISSING"), num)).toBe(true);
        });

        it('top-sample-score invalid', function () {
            var summary = {"TopSample" : []};
            for (var i = 0; i < num; i++) {
                summary.TopSample.push({"Score" : ""});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_TOP_SAMPLE_SCORE_INVALID"), num)).toBe(true);
        });
    });

    //==================================================
    // BottomSample Error Tests
    //==================================================
    describe('bottom-sample error tests', function () {
        var num = 10;
        it('bottom-sample missing', function () {
            var summary = {};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_BOTTOM_SAMPLE_MISSING"))).toBe(true);
        });

        it('bottom-sample unexpected', function () {
            var summary = {"BottomSample" : ""};
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsError(errors, resourceUtility.getString(
                "VALIDATION_ERROR_BOTTOM_SAMPLE_UNEXPECTED"))).toBe(true);
        });

        it('bottom-sample-company missing', function () {
            var summary = {"BottomSample" : []};
            for (var i = 0; i < num; i++) {
                summary.BottomSample.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_BOTTOM_SAMPLE_COMPANY_MISSING"), num)).toBe(true);
        });

        it('bottom-sample-first-name missing', function () {
            var summary = {"BottomSample" : []};
            for (var i = 0; i < num; i++) {
                summary.BottomSample.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_BOTTOM_SAMPLE_FIRST_NAME_MISSING"), num)).toBe(true);
        });

        it('bottom-sample-last-name missing', function () {
            var summary = {"BottomSample" : []};
            for (var i = 0; i < num; i++) {
                summary.BottomSample.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_BOTTOM_SAMPLE_LAST_NAME_MISSING"), num)).toBe(true);
        });

        it('bottom-sample-converted missing', function () {
            var summary = {"BottomSample" : []};
            for (var i = 0; i < num; i++) {
                summary.BottomSample.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_BOTTOM_SAMPLE_CONVERTED_MISSING"), num)).toBe(true);
        });

        it('bottom-sample-converted invalid', function () {
            var summary = {"BottomSample" : []};
            for (var i = 0; i < num; i++) {
                summary.BottomSample.push({"Converted" : ""});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_BOTTOM_SAMPLE_CONVERTED_INVALID"), num)).toBe(true);
        });

        it('bottom-sample-score missing', function () {
            var summary = {"BottomSample" : []};
            for (var i = 0; i < num; i++) {
                summary.BottomSample.push({});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_BOTTOM_SAMPLE_SCORE_MISSING"), num)).toBe(true);
        });

        it('bottom-sample-score invalid', function () {
            var summary = {"BottomSample" : []};
            for (var i = 0; i < num; i++) {
                summary.BottomSample.push({"Score" : ""});
            }
            var errors = validationService.ValidateModelSummary(summary);
            expect(ContainsErrors(errors, resourceUtility.getString(
                "VALIDATION_ERROR_BOTTOM_SAMPLE_SCORE_INVALID"), num)).toBe(true);
        });
    });

    function ContainsError(errors, error) {
        return _.filter(errors, function(e) { return e === error; }).length === 1;
    }

    function ContainsErrors(errors, error, expected) {
        return _.filter(errors, function(e) { return e.indexOf(error) === 0; }).length === expected;
    }
});