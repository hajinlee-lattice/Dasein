angular.module('mainApp.appCommon.services.ModelSummaryValidationService', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('ModelSummaryValidationService', function (ResourceUtility) {

    this.ValidateModelSummary = function (modelSummary) {
        var i, errors = [];

        if (!modelSummary) {
            errors.push(ResourceUtility.getString("VALIDATION_ERROR_MODEL_SUMMARY_MISSING"));
            return errors;
        }

        try {
            //Segmentations
            var segmentationErrors = ValidateSegmentations(modelSummary);
            if (segmentationErrors.length > 0) PushErrors(errors, segmentationErrors);

            //Predictors
            var predictorErrors = ValidatePredictors(modelSummary);
            if (predictorErrors.length > 0) PushErrors(errors, predictorErrors);

            //ModelDetails
            var detailErrors = ValidateModelDetails(modelSummary);
            if (detailErrors.length > 0) PushErrors(errors, detailErrors);

            //TopSample
            var topSampleErrors = ValidateTopSample(modelSummary);
            if (topSampleErrors.length > 0) PushErrors(errors, topSampleErrors);

            //BottomSample
            var bottomSampleErrors = ValidateBottomSample(modelSummary);
            if (bottomSampleErrors.length > 0) PushErrors(errors, bottomSampleErrors);

            //EventTableProvenance (Skip)

        } catch (exc) {
            errors.push(exc.message);
        }

        return errors;
    };

    function PushErrors(errors, newErrors) {
        for (var i = 0; i < newErrors.length; i++) {
            errors.push(newErrors[i]);
        }
    }

    function PushError(errors, key) {
        errors.push(ResourceUtility.getString(key));
    }

    function PushErrorWithOffset(errors, key, i) {
        errors.push(ResourceUtility.getString(key) + ErrorOffset(i));
    }

    function PushErrorWithOffsets(errors, key, i, j) {
        errors.push(ResourceUtility.getString(key) + ErrorOffsets(i, j));
    }

    function ErrorOffset(i) {
        return " [i = " + i + "]";
    }

    function ErrorOffsets(i, j) {
        return " [i = " + i + "]" + " [j = " + j + "]";
    }

    //==================================================
    // Segmentations
    //==================================================
    function ValidateSegmentations(modelSummary) {
        var i, segment, errors = [];

        if (!modelSummary.hasOwnProperty("Segmentations")) {
            PushError(errors, "VALIDATION_ERROR_SEGMENTATIONS_MISSING");
        } else if (!Array.isArray(modelSummary.Segmentations) || modelSummary.Segmentations.length != 1) {
            PushError(errors, "VALIDATION_ERROR_SEGMENTATIONS_UNEXPECTED");
        } else {
            var segmentation = modelSummary.Segmentations[0];

            //LeadSource (Skip)

            //Segments
            if (!segmentation.hasOwnProperty("Segments")) {
                PushError(errors, "VALIDATION_ERROR_SEGMENTS_MISSING");
            } else if (!Array.isArray(segmentation.Segments) || segmentation.Segments.length != 100) {
                PushError(errors, "VALIDATION_ERROR_SEGMENTS_UNEXPECTED");
            } else {
                for (i = 0; i < 100; i++) {
                    segment = segmentation.Segments[i];

                    //Score
                    if (!segment.hasOwnProperty("Score"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_SEGMENT_SCORE_MISSING", i);
                    else if (segment.Score != parseInt(segment.Score))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_SEGMENT_SCORE_INVALID", i);

                    //Count
                    if (!segment.hasOwnProperty("Count"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_SEGMENT_COUNT_MISSING", i);
                    else if (segment.Count != parseInt(segment.Count))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_SEGMENT_COUNT_INVALID", i);

                    //Converted
                    if (!segment.hasOwnProperty("Converted"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_SEGMENT_CONVERTED_MISSING", i);
                    else if (segment.Converted != parseInt(segment.Converted))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_SEGMENT_CONVERTED_INVALID", i);
                }
            }
        }

        return errors;
    }

    //==================================================
    // Predictors
    //==================================================
    function ValidatePredictors(modelSummary) {
        var i, j, predictor, errors = [];

        if (!modelSummary.hasOwnProperty("Predictors")) {
            PushError(errors, "VALIDATION_ERROR_PREDICTORS_MISSING");
        } else {
            var predictors = modelSummary.Predictors;
            if (!Array.isArray(predictors)) {
                PushError(errors, "VALIDATION_ERROR_PREDICTORS_UNEXPECTED");
            } else {
                for (i = 0; i < predictors.length; i++) {
                    predictor = predictors[i];

                    //Name
                    if (!predictor.hasOwnProperty("Name"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_PREDICTOR_NAME_MISSING", i);

                    //Tags
                    if (!predictor.hasOwnProperty("Tags"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_PREDICTOR_TAGS_MISSING", i);
                    else if (predictor.Tags != null && !Array.isArray(predictor.Tags))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_PREDICTOR_TAGS_UNEXPECTED", i);

                    //DataType
                    if (!predictor.hasOwnProperty("DataType"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_PREDICTOR_DATA_TYPE_MISSING", i);

                    //DisplayName
                    if (!predictor.hasOwnProperty("DisplayName"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_PREDICTOR_DISPLAY_NAME_MISSING", i);

                    //Description
                    if (!predictor.hasOwnProperty("Description"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_PREDICTOR_DESCRIPTION_MISSING", i);

                    //ApprovedUsage
                    if (!predictor.hasOwnProperty("ApprovedUsage"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_PREDICTOR_APPROVED_USAGE_MISSING", i);
                    else if (predictor.ApprovedUsage != null && !Array.isArray(predictor.ApprovedUsage))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_PREDICTOR_APPROVED_USAGE_UNEXPECTED", i);

                    //FundamentalType
                    if (!predictor.hasOwnProperty("FundamentalType"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_PREDICTOR_FUNDAMENTAL_TYPE_MISSING", i);

                    //Category
                    if (!predictor.hasOwnProperty("Category"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_PREDICTOR_CATEGORY_MISSING", i);

                    //UncertaintyCoefficient
                    if (!predictor.hasOwnProperty("UncertaintyCoefficient"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_PREDICTOR_UNCERTAINTY_COEFFICIENT_MISSING", i);
                    else if (predictor.UncertaintyCoefficient != parseFloat(predictor.UncertaintyCoefficient))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_PREDICTOR_UNCERTAINTY_COEFFICIENT_INVALID", i);

                    //Elements
                    if (!predictor.hasOwnProperty("Elements")) {
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENTS_MISSING", i);
                    } else if (!Array.isArray(predictor.Elements)) {
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENTS_UNEXPECTED", i);
                    } else {
                        for (j = 0; j < predictor.Elements.length; j++) {
                            var element = predictor.Elements[j];

                            //CorrelationSign
                            if (!element.hasOwnProperty("CorrelationSign"))
                                PushErrorWithOffsets(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENT_CORRELATION_SIGN_MISSING", i, j);
                            else if (!(element.CorrelationSign === -1 || element.CorrelationSign === 1))
                                PushErrorWithOffsets(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENT_CORRELATION_SIGN_INVALID", i, j);

                            //Count
                            if (!element.hasOwnProperty("Count"))
                                PushErrorWithOffsets(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENT_COUNT_MISSING", i, j);
                            else if (element.Count != parseInt(element.Count))
                                PushErrorWithOffsets(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENT_COUNT_INVALID", i, j);

                            //Lift
                            if (!element.hasOwnProperty("Lift"))
                                PushErrorWithOffsets(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENT_LIFT_MISSING", i, j);
                            else if (element.Lift != parseFloat(element.Lift))
                                PushErrorWithOffsets(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENT_LIFT_INVALID", i, j);

                            //LowerInclusive
                            if (element.hasOwnProperty("LowerInclusive") &&
                                    element.LowerInclusive != null &&
                                    element.LowerInclusive != parseFloat(element.LowerInclusive))
                                PushErrorWithOffsets(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENT_LOWER_INCLUSIVE_INVALID", i, j);

                            //UpperExclusive
                            if (element.hasOwnProperty("UpperExclusive") &&
                                    element.UpperExclusive != null &&
                                    element.UpperExclusive != parseFloat(element.UpperExclusive))
                                PushErrorWithOffsets(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENT_UPPER_EXCLUSIVE_INVALID", i, j);

                            //Name
                            if (!element.hasOwnProperty("Name"))
                                PushErrorWithOffsets(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENT_NAME_MISSING", i, j);

                            //UncertaintyCoefficient
                            if (!element.hasOwnProperty("UncertaintyCoefficient"))
                                PushErrorWithOffsets(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENT_UNCERTAINTY_COEFFICIENT_MISSING", i, j);
                            else if (element.UncertaintyCoefficient != parseFloat(element.UncertaintyCoefficient))
                                PushErrorWithOffsets(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENT_UNCERTAINTY_COEFFICIENT_INVALID", i, j);

                            //Values
                            if (!element.hasOwnProperty("Values"))
                                PushErrorWithOffsets(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENT_VALUES_MISSING", i, j);
                            else if (!Array.isArray(element.Values))
                                PushErrorWithOffsets(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENT_VALUES_UNEXPECTED", i, j);

                            //IsVisible
                            if (!element.hasOwnProperty("IsVisible"))
                                PushErrorWithOffsets(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENT_IS_VISIBLE_MISSING", i, j);
                            else if (typeof element.IsVisible != "boolean")
                                PushErrorWithOffsets(errors, "VALIDATION_ERROR_PREDICTOR_ELEMENT_IS_VISIBLE_INVALID", i, j);
                        }
                    }
                }
            }
        }

        return errors;
    }

    //==================================================
    // ModelDetails
    //==================================================
    function ValidateModelDetails(modelSummary) {
        var errors = [];

        if (!modelSummary.hasOwnProperty("ModelDetails")) {
            PushError(errors, "VALIDATION_ERROR_MODEL_DETAILS_MISSING");
        } else {
            var details = modelSummary.ModelDetails;

            //Name
            if (!details.hasOwnProperty("Name"))
                PushError(errors, "VALIDATION_ERROR_MODEL_NAME_MISSING");

            //LookupID (Skip)

            //Total Leads
            if (!details.hasOwnProperty("TotalLeads"))
                PushError(errors, "VALIDATION_ERROR_TOTAL_LEADS_MISSING");
            else if (details.TotalLeads != parseInt(details.TotalLeads))
                PushError(errors, "VALIDATION_ERROR_TOTAL_LEADS_INVALID");

            //Testing Leads
            if (!details.hasOwnProperty("TestingLeads"))
                PushError(errors, "VALIDATION_ERROR_TESTING_LEADS_MISSING");
            else if (details.TestingLeads != parseInt(details.TestingLeads))
                PushError(errors, "VALIDATION_ERROR_TESTING_LEADS_INVALID");

            //Training Leads
            if (!details.hasOwnProperty("TrainingLeads"))
                PushError(errors, "VALIDATION_ERROR_TRAINING_LEADS_MISSING");
            else if (details.TrainingLeads != parseInt(details.TrainingLeads))
                PushError(errors, "VALIDATION_ERROR_TRAINING_LEADS_INVALID");

            //Total Conversions
            if (!details.hasOwnProperty("TotalConversions"))
                PushError(errors, "VALIDATION_ERROR_TOTAL_CONVERSIONS_MISSING");
            else if (details.TotalConversions != parseInt(details.TotalConversions))
                PushError(errors, "VALIDATION_ERROR_TOTAL_CONVERSIONS_INVALID");

            //Testing Conversions
            if (!details.hasOwnProperty("TestingConversions"))
                PushError(errors, "VALIDATION_ERROR_TESTING_CONVERSIONS_MISSING");
            else if (details.TestingConversions != parseInt(details.TestingConversions))
                PushError(errors, "VALIDATION_ERROR_TESTING_CONVERSIONS_INVALID");

            //Training Conversions
            if (!details.hasOwnProperty("TrainingConversions"))
                PushError(errors, "VALIDATION_ERROR_TRAINING_CONVERSIONS_MISSING");
            else if (details.TrainingConversions != parseInt(details.TrainingConversions))
                PushError(errors, "VALIDATION_ERROR_TRAINING_CONVERSIONS_INVALID");

            //ROC Score
            if (!details.hasOwnProperty("RocScore"))
                PushError(errors, "VALIDATION_ERROR_ROC_SCORE_MISSING");
            else if (details.RocScore != parseFloat(details.RocScore))
                PushError(errors, "VALIDATION_ERROR_ROC_SCORE_INVALID");

            //Construction Time
            if (!details.hasOwnProperty("ConstructionTime"))
                PushError(errors, "VALIDATION_ERROR_CONSTRUCTION_TIME_MISSING");
            else if (details.ConstructionTime != parseInt(details.ConstructionTime))
                PushError(errors, "VALIDATION_ERROR_CONSTRUCTION_TIME_INVALID");
        }

        return errors;
    }

    //==================================================
    // TopSample
    //==================================================
    function ValidateTopSample(modelSummary) {
        var i, errors = [];

        if (!modelSummary.hasOwnProperty("TopSample")) {
            PushError(errors, "VALIDATION_ERROR_TOP_SAMPLE_MISSING");
        } else {
            var topSample = modelSummary.TopSample;
            if (!Array.isArray(topSample)) {
                PushError(errors, "VALIDATION_ERROR_TOP_SAMPLE_UNEXPECTED");
            } else {
                for (i = 0; i < topSample.length; i++) {
                    sample = topSample[i];

                    //Company
                    if (!sample.hasOwnProperty("Company"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_TOP_SAMPLE_COMPANY_MISSING", i);

                    //FirstName
                    if (!sample.hasOwnProperty("FirstName"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_TOP_SAMPLE_FIRST_NAME_MISSING", i);

                    //LastName
                    if (!sample.hasOwnProperty("LastName"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_TOP_SAMPLE_LAST_NAME_MISSING", i);

                    //Converted
                    if (!sample.hasOwnProperty("Converted"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_TOP_SAMPLE_CONVERTED_MISSING", i);
                    else if (typeof sample.Converted != "boolean")
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_TOP_SAMPLE_CONVERTED_INVALID", i);

                    //Score
                    if (!sample.hasOwnProperty("Score"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_TOP_SAMPLE_SCORE_MISSING", i);
                    else if (sample.Score != parseInt(sample.Score))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_TOP_SAMPLE_SCORE_INVALID", i);
                }
            }
        }

        return errors;
    }

    //==================================================
    // BottomSample
    //==================================================
    function ValidateBottomSample(modelSummary) {
        var i, errors = [];

        if (!modelSummary.hasOwnProperty("BottomSample")) {
            PushError(errors, "VALIDATION_ERROR_BOTTOM_SAMPLE_MISSING");
        } else {
            var bottomSample = modelSummary.BottomSample;
            if (!Array.isArray(bottomSample)) {
                PushError(errors, "VALIDATION_ERROR_BOTTOM_SAMPLE_UNEXPECTED");
            } else {
                for (i = 0; i < bottomSample.length; i++) {
                    sample = bottomSample[i];

                    //Company
                    if (!sample.hasOwnProperty("Company"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_BOTTOM_SAMPLE_COMPANY_MISSING", i);

                    //FirstName
                    if (!sample.hasOwnProperty("FirstName"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_BOTTOM_SAMPLE_FIRST_NAME_MISSING", i);

                    //LastName
                    if (!sample.hasOwnProperty("LastName"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_BOTTOM_SAMPLE_LAST_NAME_MISSING", i);

                    //Converted
                    if (!sample.hasOwnProperty("Converted"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_BOTTOM_SAMPLE_CONVERTED_MISSING", i);
                    else if (typeof sample.Converted != "boolean")
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_BOTTOM_SAMPLE_CONVERTED_INVALID", i);

                    //Score
                    if (!sample.hasOwnProperty("Score"))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_BOTTOM_SAMPLE_SCORE_MISSING", i);
                    else if (sample.Score != parseInt(sample.Score))
                        PushErrorWithOffset(errors, "VALIDATION_ERROR_BOTTOM_SAMPLE_SCORE_INVALID", i);
                }
            }
        }

        return errors;
    }
});