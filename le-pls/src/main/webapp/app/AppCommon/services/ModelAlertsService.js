angular.module('mainApp.appCommon.services.ModelAlertsService', [
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('ModelAlertsService', function ($filter, StringUtility, ResourceUtility) {

    this.GetWarnings = function (modelAlerts, suppressedCategories) {
        var warnings = {};
        warnings.noWarning = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_NO_WARNING");
        warnings.modelQualityWarningsTitle = ResourceUtility.getString('ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_TITLE');
        warnings.modelQualityWarningsLabel = ResourceUtility.getString('ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_LABEL');
        warnings.missingMetaDataWarningsTitle = ResourceUtility.getString('ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_TITLE');
        warnings.missingMetaDataWarningsLabel = ResourceUtility.getString('ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_LABEL');

        // Get model quality warnings
        var modelQualityWarnings = [];
        var modelQualityWarningsObj = modelAlerts.ModelQualityWarnings;
        if (modelQualityWarningsObj != null) {
            addSuccessEventsWarning(modelQualityWarnings, modelQualityWarningsObj);
            addConversionRateWarning(modelQualityWarnings, modelQualityWarningsObj);
            addOutOfRangeRocScoreWarning(modelQualityWarnings, modelQualityWarningsObj);
            addExcessiveDiscreteValuesAttributesWarning(modelQualityWarnings, modelQualityWarningsObj);
            addExcessivePredictiveAttributesWarning(modelQualityWarnings, modelQualityWarningsObj);
            addExcessivePredictiveNullValuesAttributesWarning(modelQualityWarnings, modelQualityWarningsObj);
        }
        warnings.noModelQualityWarnings = (modelQualityWarnings.length === 0);
        warnings.modelQualityWarnings = modelQualityWarnings;

        // Get missing meta-data warnings
        var missingMetaDataWarnings = [];
        var missingMetaDataWarningsObj = modelAlerts.MissingMetaDataWarnings;
        if (missingMetaDataWarningsObj != null) {
            addInvalidApprovedUsageAttributesWarning(missingMetaDataWarnings, missingMetaDataWarningsObj);
            addInvalidTagsAttributesWarning(missingMetaDataWarnings, missingMetaDataWarningsObj);
            addInvalidCategoryAttributesWarning(missingMetaDataWarnings, missingMetaDataWarningsObj);
            addInvalidDisplayNameAttributesWarning(missingMetaDataWarnings, missingMetaDataWarningsObj);
            addInvalidStatisticalTypeAttributesWarning(missingMetaDataWarnings, missingMetaDataWarningsObj);
            addExcessiveCategoriesInModelSummaryWarning(missingMetaDataWarnings, suppressedCategories);
        }
        warnings.noMissingMetaDataWarnings = (missingMetaDataWarnings.length === 0);
        warnings.missingMetaDataWarnings = missingMetaDataWarnings;

        return warnings;
    };

    function addSuccessEventsWarning(warnings, modelQualityWarnings) {
        var successEvents = modelQualityWarnings.LowSuccessEvents;
        var minValue = modelQualityWarnings.MinSuccessEvents;
        if (successEvents != null && minValue != null) {
            var warning = {};
            successEvents = $filter('number')(successEvents, 0); // Add comma per 3 digits
            minValue = $filter('number')(minValue, 0);
            warning.title = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_SUCCESS_EVENTS_TOO_LOWER_TITLE", [successEvents, minValue]);
            warning.description = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ADJUST_FILTERS_EVENT_DEFINITION");
            warning.successEvents = successEvents; // For unit test
            warning.minValue = minValue;
            warnings.push(warning);
        }
    }

    function addConversionRateWarning(warnings, modelQualityWarnings) {
        var conversionPercentage = modelQualityWarnings.LowConversionPercentage;
        var minValue = modelQualityWarnings.MinConversionPercentage;
        if (conversionPercentage != null && minValue != null) {
            var warning = {};
            conversionPercentage = $filter('number')(conversionPercentage, 1);
            warning.title = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_CONVERSION_RATE_TOO_LOWER_TITLE", [conversionPercentage, minValue]);
            warning.description = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ADJUST_FILTERS_EVENT_DEFINITION");
            warning.conversionPercentage = conversionPercentage;
            warning.minValue = minValue;
            warnings.push(warning);
        }
    }

    function addOutOfRangeRocScoreWarning(warnings, modelQualityWarnings) {
        var rocScore = modelQualityWarnings.OutOfRangeRocScore;
        var minValue = modelQualityWarnings.MinRocScore;
        var maxValue = modelQualityWarnings.MaxRocScore;
        if (rocScore != null && minValue != null && maxValue != null) {
            var warning = {};
            rocScore = $filter('number')(rocScore, 2);
            warning.title = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ROC_SCORE_OUTSIDE_CREDIBLE_RANGE", [rocScore, minValue, maxValue]);
            warning.description = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ADJUST_FILTERS_EVENT_DEFINITION");
            warning.rocScore = rocScore;
            warning.minValue = minValue;
            warning.maxValue = maxValue;
            warnings.push(warning);
        }
    }

    function addExcessiveDiscreteValuesAttributesWarning(warnings, modelQualityWarnings) {
        var attributes = modelQualityWarnings.ExcessiveDiscreteValuesAttributes;
        var maxValue = modelQualityWarnings.MaxNumberOfDiscreteValues;
        if (attributes != null && attributes.length > 0 && maxValue != null) {
            var warning = {};
            var count = $filter('number')(attributes.length, 0);
            maxValue = $filter('number')(maxValue, 0);
            warning.title = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ATTRIBUTES_OVERLY_DISCRETE_VALUES_TITLE", [count, maxValue]);
            warning.description = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ATTRIBUTES_REMOVED_FROM_MODELING");
            warning.impactedLabel = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
            warning.impactedContent = joinMapList(attributes, 0);
            warning.count = count;
            warning.maxValue = maxValue;
            warnings.push(warning);
        }
    }

    function addExcessivePredictiveAttributesWarning(warnings, modelQualityWarnings) {
        var attributes = modelQualityWarnings.ExcessivePredictiveAttributes;
        var maxValue = modelQualityWarnings.MaxFeatureImportance;
        if (attributes != null && attributes.length > 0 && maxValue != null) {
            var warning = {};
            var count = $filter('number')(attributes.length, 0);
            warning.title = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ATTRIBUTES_OVERLY_PREDICTIVE_TITLE", [count, maxValue]);
            warning.description = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ATTRIBUTES_CONTAIN_FUTURE_INFORMATION_REQUIRE_EM_JSUTIFICATION_VALID_PREDICTOR");
            warning.impactedLabel = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
            warning.impactedContent = joinMapList(attributes, 1);
            warning.count = count;
            warning.maxValue = maxValue;
            warnings.push(warning);
        }
    }

    function addExcessivePredictiveNullValuesAttributesWarning(warnings, modelQualityWarnings) {
        var attributes = modelQualityWarnings.ExcessivePredictiveNullValuesAttributes;
        var maxValue = modelQualityWarnings.MaxLiftForNull;
        if (attributes != null && attributes.length > 0 && maxValue != null) {
            var warning = {};
            var count = $filter('number')(attributes.length, 0);
            warning.title = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ATTRIBUTES_HIGHLY_PREDICTIVE_NULL_VALUES", [count, maxValue]);
            warning.description = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MODEL_QUALITY_ATTRIBUTES_CONFIRM_ABSENCE_DATA_HAS_BUSINESS_MEANING");
            warning.impactedLabel = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
            warning.impactedContent = joinMapList(attributes, 1);
            warning.count = count;
            warning.maxValue = maxValue;
            warnings.push(warning);
        }
    }

    function addInvalidApprovedUsageAttributesWarning(warnings, missingMetaDataWarnings) {
        var attributes = missingMetaDataWarnings.InvalidApprovedUsageAttributes;
        if (attributes != null && attributes.length > 0) {
            var warning = {};
            var count = $filter('number')(attributes.length, 0);
            warning.title = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_INVALID_APPROVED_USAGE", [count]);
            warning.description = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_SUPPRESSED_FROM_TOP_PREDICTORS_UI_CSV_BUYER_INSIGHTS");
            warning.impactedLabel = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
            warning.impactedContent = joinStringList(attributes);
            warning.count = count;
            warnings.push(warning);
        }
    }

    function addInvalidTagsAttributesWarning(warnings, missingMetaDataWarnings) {
        var attributes = missingMetaDataWarnings.InvalidTagsAttributes;
        if (attributes != null && attributes.length > 0) {
            var warning = {};
            var count = $filter('number')(attributes.length, 0);
            warning.title = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_INVALID_TAGS", [count]);
            warning.description = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_SUPPRESSED_FROM_TOP_PREDICTORS_UI_CSV_BUYER_INSIGHTS");
            warning.impactedLabel = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
            warning.impactedContent = joinStringList(attributes);
            warning.count = count;
            warnings.push(warning);
        }
    }

    function addInvalidCategoryAttributesWarning(warnings, missingMetaDataWarnings) {
        var attributes = missingMetaDataWarnings.InvalidCategoryAttributes;
        if (attributes != null && attributes.length > 0) {
            var warning = {};
            var count = $filter('number')(attributes.length, 0);
            warning.title = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_INVALID_CATEGORY", count);
            warning.description = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_SUPPRESSED_FROM_TOP_PREDICTORS_UI_CSV_BUYER_INSIGHTS");
            warning.impactedLabel = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
            warning.impactedContent = joinStringList(attributes);
            warning.count = count;
            warnings.push(warning);
        }
    }

    function addInvalidDisplayNameAttributesWarning(warnings, missingMetaDataWarnings) {
        var attributes = missingMetaDataWarnings.InvalidDisplayNameAttributes;
        if (attributes != null && attributes.length > 0) {
            var warning = {};
            var count = $filter('number')(attributes.length, 0);
            warning.title = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_INVALID_DISPLAY_NAME", count);
            warning.description = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_SUPPRESSED_FROM_TOP_PREDICTORS_UI_CSV_BUYER_INSIGHTS");
            warning.impactedLabel = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
            warning.impactedContent = joinStringList(attributes);
            warning.count = count;
            warnings.push(warning);
        }
    }

    function addInvalidStatisticalTypeAttributesWarning(warnings, missingMetaDataWarnings) {
        var attributes = missingMetaDataWarnings.InvalidStatisticalTypeAttributes;
        if (attributes != null && attributes.length > 0) {
            var warning = {};
            var count = $filter('number')(attributes.length, 0);
            warning.title = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_INVALID_STATISTICAL_TYPE", [count]);
            warning.description = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_ATTRIBUTES_BUCKETED_INCORRECTLY_IN_TOP_PREDICTORS_UI_CSV_BUYER_INSIGHTS");
            warning.impactedLabel = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_ATTRIBUTES");
            warning.impactedContent = joinStringList(attributes);
            warning.count = count;
            warnings.push(warning);
        }
    }

    function addExcessiveCategoriesInModelSummaryWarning(warnings, suppressedCategories) {
        var categories = suppressedCategories;
        //  We currently only show top 8 categories in the UI
        if (categories != null && categories.length > 0) {
            var warning = {};
            var count = $filter('number')(categories.length + 8, 0);
            var maxValue = 8;
            var categoryNames = getCategoryNames(categories);
            warning.title = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_MODEL_SUMMARY_CONTAINS_OVERLY_CATEFORIES", [count, maxValue]);
            warning.description = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_MISSING_META_DATA_TWO_CATEFORIES_AND_ATTRIBUTES_SUPPRESSED_FROM_TOP_PREDICTORS_UI");
            warning.impactedLabel = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_IMPACTED_CATEGORIES");
            warning.impactedContent = joinStringList(categoryNames);
            warning.count = count;
            warning.maxValue = maxValue;
            warnings.push(warning);
        }
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
            var value = fractionSize != null ? $filter('number')(element.value, fractionSize) : element.value;
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