angular.module('mainApp.appCommon.utilities.AnalyticAttributeUtility', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.appCommon.utilities.NumberUtility'
])
.service('AnalyticAttributeUtility', function (ResourceUtility, DateTimeFormatUtility, NumberUtility) {

    // Enum used to determine whether a bucket should be shown
    this.ApprovedUsage = {
        None: "None",
        Model: "Model",
        ModelAndModelInsights: "ModelAndModelInsights",
        ModelAndAllInsights: "ModelAndAllInsights",
        IndividualDisplay: "IndividualDisplay"
    };

    // Enum used to determine attribute type information
    this.DataType = {
        String: "STRING",
        LongString: "LONGSTRING",
        Date: "DATE",
        DateTime: "DATETIME",
        Time: "TIME",
        Double: "DOUBLE",
        Float: "FLOAT",
        Int: "INT",
        Integer: "INTEGER",
        Short: "SHORT",
        Long: "LONG",
        Boolean: "BOOLEAN",
        Bit: "BIT",
        Epoch: "EPOCHTIME"
    };

    // Enum used to determine attribute type semantics
    this.FundamentalType = {
        Email: "EMAIL",
        Probability: "PROBABILITY",
        Phone: "PHONE",
        Enum: "ENUM",
        URI: "URI",
        Currency: "CURRENCY",
        Percent: "PERCENT",
        Year: "YEAR",
        Boolean: "BOOLEAN",
        Numeric: "NUMERIC"
    };

    this.FindAttributeMetadataData = function (modelSummary, attributeData) {
        var self = this;
        if (attributeData == null ||
            modelSummary == null ||
            modelSummary.AttributeMetadata == null ||
            modelSummary.AttributeMetadata.length === 0) {
            return null;
        }

        for (var i = 0; i < modelSummary.AttributeMetadata.length; i++) {
            if (attributeData.AttributeName == modelSummary.AttributeMetadata[i].ColumnName) {
                return modelSummary.AttributeMetadata[i];
            }
        }

        return null;
    };

    this.FindAttributeBucket = function (modelSummary, attributeData) {
        var self = this;
        if (attributeData == null ||
            modelSummary == null ||
            modelSummary.Summary == null ||
            modelSummary.Summary.Predictors == null ||
            modelSummary.Summary.Predictors.length === 0) {
            return null;
        }

        var predictor = null;
        for (var i = 0; i < modelSummary.Summary.Predictors.length; i++) {
            if (attributeData.AttributeName == modelSummary.Summary.Predictors[i].Name) {
                predictor = modelSummary.Summary.Predictors[i];
                break;
            }
        }

        if (predictor == null || predictor.Elements == null || predictor.Elements.length === 0) {
            return null;
        }

        for (var x = 0; x < predictor.Elements.length; x++) {
            var bucket = predictor.Elements[x];
            if (bucket.Values != null && bucket.Values.length > 0) {
                // We are working with Discrete values
                for (var y = 0; y < bucket.Values.length; y++) {
                    if (attributeData.AttributeValue == bucket.Values[y]) {
                        return bucket;
                    }
                }
            } else {
                var lowerBound = bucket.LowerInclusive != null ? parseFloat(bucket.LowerInclusive) : null;
                var upperBound = bucket.UpperExclusive != null ? parseFloat(bucket.UpperExclusive) : null;
                var parsedAttributeValue = attributeData.AttributeValue != null ? parseFloat(attributeData.AttributeValue) : null;
                if (parsedAttributeValue >= lowerBound && (parsedAttributeValue < upperBound || upperBound == null)) {
                    return bucket;
                }
            }
        }

        return null;
    };

    this.FormatBooleanBucketName = function (value) {
    	var toReturn = value;
        if (value == "NA" || value == "N/A" || value == "NULL" || value == "NOT AVAILABLE" || parseInt(value) == (-1)) {
            toReturn = "Not Available";
        } else if (value == "N" || value == "NO" || value == "FALSE" || value == "F" || parseInt(value) === (0)) {
            toReturn = "No";
        } else if (value == "Y" || value == "YES" || value == "TRUE" || value == "T" || parseInt(value) == (1)) {
            toReturn = "Yes";
        } 
        return toReturn;
    };
    
    this.GetAttributeBucketName = function (bucket, attributeMetadata) {
        if (bucket == null) {
            return "";
        }
        var toReturn = null;
        var lowerValue;
        var upperValue;
        if (bucket.Values != null && bucket.Values.length > 0) {
            // This is the Null bucket
            if (bucket.Values[0] == null) {
                toReturn = ResourceUtility.getString("ANALYTIC_ATTRIBUTE_NULL_VALUE_LABEL");
            } else {
                if (attributeMetadata != null) {
                    var fundamentalType = attributeMetadata.FundamentalType != null ? attributeMetadata.FundamentalType.toUpperCase() : null;
                    if (fundamentalType == this.FundamentalType.Boolean) {
                        var value = bucket.Values[0].toString().toUpperCase();
                        toReturn = this.FormatBooleanBucketName(value);
                        return toReturn;
                    }
                }
                var discreteValueString = "";
                for (var i = 0; i < bucket.Values.length; i++) {
                    var bucketValue = this.FormatBucketValue(bucket.Values[i], attributeMetadata);
                    if (discreteValueString === "") {
                        discreteValueString = bucketValue;
                    } else {
                        discreteValueString += ", " + bucketValue;
                    }
                }
                toReturn = discreteValueString;
            }
        } else if (bucket.LowerInclusive != null && bucket.UpperExclusive != null) {

            if (attributeMetadata != null) {
                var dataType = attributeMetadata.DataType != null ? attributeMetadata.DataType.toUpperCase() : null;
                if (dataType == this.DataType.Int || dataType == this.DataType.Integer) {
                    toReturn = this.FormatIntegerBucket(bucket.LowerInclusive, bucket.UpperExclusive, attributeMetadata);
                } else {
                    lowerValue = this.FormatBucketValue(bucket.LowerInclusive, attributeMetadata);
                    upperValue = this.FormatBucketValue(bucket.UpperExclusive, attributeMetadata);
                    toReturn = ResourceUtility.getString("ANALYTIC_ATTRIBUTE_CONTINUOUS_BETWEEN_LABEL", [lowerValue, upperValue]);
                }
            } else {
                lowerValue = this.FormatBucketValue(bucket.LowerInclusive, attributeMetadata);
                upperValue = this.FormatBucketValue(bucket.UpperExclusive, attributeMetadata);
                toReturn = ResourceUtility.getString("ANALYTIC_ATTRIBUTE_CONTINUOUS_BETWEEN_LABEL", [lowerValue, upperValue]);
            }
        } else if (bucket.LowerInclusive == null && bucket.UpperExclusive == null) {
            toReturn = ResourceUtility.getString("ANALYTIC_ATTRIBUTE_ALL_VALUES_LABEL");
        } else if (bucket.LowerInclusive != null) {
            lowerValue = this.FormatBucketValue(bucket.LowerInclusive, attributeMetadata);
            toReturn = ResourceUtility.getString("ANALYTIC_ATTRIBUTE_GREATER_THAN_LABEL", [lowerValue]);
        } else {
            upperValue = this.FormatBucketValue(bucket.UpperExclusive, attributeMetadata);
            toReturn = ResourceUtility.getString("ANALYTIC_ATTRIBUTE_LESS_THAN_LABEL", [upperValue]);
        }
        return String(toReturn);
    };

    // ENG-6735:
    //  1) If the bucket spans one integer (e.g. lower is 0 and upper is 1), return lower
    //  2) Otherwise, decrement upper exclusive so that it is "inclusive"
    this.FormatIntegerBucket = function (lower, upper, attributeMetadata) {
        if (upper - lower <= 1) {
            return lower;
        }

        var lowerValue = this.FormatBucketValue(lower, attributeMetadata);
        var upperValue = this.FormatBucketValue(upper - 1, attributeMetadata);
        return ResourceUtility.getString("ANALYTIC_ATTRIBUTE_CONTINUOUS_BETWEEN_LABEL", [lowerValue, upperValue]);
    };

    this.AbbreviateNumber = function (realValue, fundamentalType) {
        var parsedValue = parseFloat(realValue);
        // If the parsedValue is NaN then we have a mismatch of types so just return the value
        if (isNaN(parsedValue)) {
            return realValue;
        }

        // If the value's fundamental type is 'year', do not round it.
        if (fundamentalType == this.FundamentalType.Year) {
            return parsedValue;
        }

        // If the value is less than 1 it should get 2 decimal places
        // If the value is less than 1,000 it should get 1 decimal place, but only if it had a decimal place to begin with
        // Anything greater than 1,000 will be handled by NumberUtil.AbbreviateLargeNumber
        var abbreviatedNumber;
        if (parsedValue === 0) {
            abbreviatedNumber = parsedValue;
        } else if (parsedValue < 1) {
            abbreviatedNumber = parsedValue.toFixed(2);
        } else if (parsedValue < 1000 && parsedValue % 1 !== 0) {
            abbreviatedNumber = parsedValue.toFixed(1);
        } else {
            abbreviatedNumber = NumberUtility.AbbreviateLargeNumber(parsedValue, 1);
        }
        return abbreviatedNumber;
    };

    this.IsPredictorBoolean = function (attributeMetadata) {
    	if (attributeMetadata == null) {
    		return false;
    	}
    	var fundamentalType = attributeMetadata.FundamentalType != null ? attributeMetadata.FundamentalType.toUpperCase() : null;
    	return fundamentalType == this.FundamentalType.Boolean;
    };
    
    this.FormatBucketValue = function (value, attributeMetadata) {
        if (value == null || attributeMetadata == null) {
            return value;
        }

        var fundamentalType = attributeMetadata.FundamentalType != null ? attributeMetadata.FundamentalType.toUpperCase() : null;
        // If the coming data has fundamental type specified, manipulate the value as required according to fundamental type.
        if (fundamentalType == this.FundamentalType.Year ||
		fundamentalType == this.FundamentalType.Currency ||
		fundamentalType == this.FundamentalType.Numeric ||
		fundamentalType == this.FundamentalType.Percent) {
            var abbreviatedNumber = this.AbbreviateNumber(value, fundamentalType);
            // Handle currency and percent
            if (fundamentalType == this.FundamentalType.Currency) {
                return ResourceUtility.getString("CURRENCY_SYMBOL") + abbreviatedNumber;
            } else if (fundamentalType == this.FundamentalType.Percent) {
                return abbreviatedNumber + "%";
            } else {
                return abbreviatedNumber;
            }
        }

        var toReturn;
        var dataType = attributeMetadata.DataType != null ? attributeMetadata.DataType.toUpperCase() : null;
        switch (dataType) {
            // Format Numbers         
            case this.DataType.Double:
            case this.DataType.Int:
            case this.DataType.Integer:
            case this.DataType.Short:
            case this.DataType.Long:
            case this.DataType.Float:
                toReturn = this.AbbreviateNumber(value, fundamentalType);
                break;
            // Format Date         
            case this.DataType.Date:
                toReturn = DateTimeFormatUtility.FormatStringDate(value, false);
                break;
            // Format DateTime and Time         
            case this.DataType.DateTime:
            case this.DataType.Time:
                toReturn = DateTimeFormatUtility.FormatStringDate(value, true);
                break;
            // Format Boolean and Bit         
            case this.DataType.Boolean:
            case this.DataType.Bit:
                toReturn = this.FormatBooleanValueForDisplay(value);
                break;
            // Format EpochTime         
            case this.DataType.Epoch:
                toReturn = DateTimeFormatUtility.FormatEpochDate(value);
                break;
            default:
                //No formatting required for String or LongString
                toReturn = value;
                break;
        }

        return toReturn;
    };

    this.SortAttributeList = function (groomedAttributeList, descending) {
        var self = this;
        if (groomedAttributeList == null || groomedAttributeList.length === 0) {
            return groomedAttributeList;
        }

        if (descending === false) {
            groomedAttributeList.sort(function (a, b) {
                if (a.Lift - b.Lift < 0) {
                    return -1;
                }
                if (a.Lift - b.Lift === 0) {
                    return 0;
                }
                if (a.Lift - b.Lift > 0) {
                    return 1;
                }
                return 0;
            });
        } else {
            groomedAttributeList.sort(function (a, b) {
                if (b.Lift - a.Lift < 0) {
                    return -1;
                }
                if (b.Lift - a.Lift === 0) {
                    return 0;
                }
                if (b.Lift - a.Lift > 0) {
                    return 1;
                }
                return 0;
            });
        }

        return groomedAttributeList;
    };

    this.FormatLift = function (rawLift) {
        if (rawLift == null || typeof rawLift !== 'number') {
            return null;
        }

        if (rawLift === 0) {
            return "0";
        }

        return rawLift.toFixed(1);
    };

    this.ShouldShowNullBucket = function (bucket, nullThreshold) {
        var self = this;
        if (bucket == null) {
            return false;
        }
        nullThreshold = nullThreshold != null ? parseFloat(nullThreshold) : 100;

        // Discrete and Continuous null bucket
        if (bucket.Values != null && bucket.Values.length > 0) {
            for (var i = 0; i < bucket.Values.length; i++) {
                if (bucket.Values[i] == null && bucket.Lift <= nullThreshold) {
                    return false;
                }
            }
        }

        return true;
    };

    this.ShowBasedOnTags = function (widgetConfig, attributeMetadata) {
        if (widgetConfig == null || attributeMetadata == null) {
            return false;
        }

        //TODO:pierce This is a hack because DataLoader is not providing internal metadata
        if (attributeMetadata.Tags == null) {
            attributeMetadata.Tags = [];
        }

        // Check RequiredTags first
        var hasAllRequired = true;
        if (widgetConfig.RequiredTags != null) {
            for (var i = 0; i < widgetConfig.RequiredTags.length; i++) {
                var requiredTag = widgetConfig.RequiredTags[i];
                var hasRequired = false;
                for (var x = 0; x < attributeMetadata.Tags.length; x++) {
                    if (requiredTag == attributeMetadata.Tags[x]) {
                        hasRequired = true;
                        break;
                    }
                }

                if (!hasRequired) {
                    hasAllRequired = false;
                    break;
                }
            }
        }

        //Then check ExcludedTags
        var hasExcluded = false;
        if (widgetConfig.ExcludedTags != null) {
            for (var y = 0; y < widgetConfig.ExcludedTags.length; y++) {
                var excludedTag = widgetConfig.ExcludedTags[y];
                for (var z = 0; z < attributeMetadata.Tags.length; z++) {
                    if (excludedTag == attributeMetadata.Tags[z]) {
                        hasExcluded = true;
                        break;
                    }
                }
            }
        }

        if (hasExcluded) {
            return false;
        } else if (widgetConfig.RequiredTags != null && !hasAllRequired) {
            return false;
        } else {
            return true;
        }
    };

    this.IsApprovedForUsage = function (usage, attributeMetadata) {
        if (usage == null || attributeMetadata == null || attributeMetadata.ApprovedUsage == null) {
            return false;
        }

        for (var i = 0; i < attributeMetadata.ApprovedUsage.length; i++) {
            if (attributeMetadata.ApprovedUsage[i] == usage) {
                return true;
            }
        }

        return false;
    };

    //TODO:pierce Another hack because DataLoader is not properly populating the metadata
    this.IsAllowedForInsights = function (attributeMetadata) {
        if (attributeMetadata == null) {
            return false;
        } else if (attributeMetadata.ApprovedUsage == null || attributeMetadata.ApprovedUsage.length === 0) {
            return true;
        } else {
            return this.IsApprovedForUsage(this.ApprovedUsage.ModelAndAllInsights, attributeMetadata) === true ||
                this.IsApprovedForUsage(this.ApprovedUsage.ModelAndModelInsights, attributeMetadata) === true;
        }
    };

    this.FormatBooleanValueForDisplay = function (booleanValue) {
        if (booleanValue === null || booleanValue === "" || booleanValue === undefined) {
            return "";
        }
        if (typeof booleanValue === "boolean" || typeof booleanValue === "number") {
            booleanValue = booleanValue.toString();
        }
        booleanValue = booleanValue.toUpperCase();
        if (booleanValue == "1" || booleanValue === "TRUE") {
            return ResourceUtility.getString("BOOLEAN_TRUE_DISPLAY_LABEL");
        } else if (booleanValue == "0" || booleanValue === "FALSE") {
            return ResourceUtility.getString("BOOLEAN_FALSE_DISPLAY_LABEL");
        } else {
            return "";
        }
    };

    this.GetAttributeList = function (groomedPositiveAttributeList, groomedNegativeAttributeList, attributeLimit, score) {
        var groomedAttributeList = [];
        if (groomedPositiveAttributeList.length + groomedNegativeAttributeList.length <= attributeLimit) {
            return groomedPositiveAttributeList.concat(groomedNegativeAttributeList);
        }
        if (score >= 80) {
            return this.PopulateGroomedAttributeList(groomedPositiveAttributeList, groomedNegativeAttributeList, attributeLimit, 5);
        }
        if (score >= 60) {
            return this.PopulateGroomedAttributeList(groomedPositiveAttributeList, groomedNegativeAttributeList, attributeLimit, 4);
        }
        if (score >= 40) {
            return this.PopulateGroomedAttributeList(groomedPositiveAttributeList, groomedNegativeAttributeList, attributeLimit, 3);
        }
        return this.PopulateGroomedAttributeList(groomedPositiveAttributeList, groomedNegativeAttributeList, attributeLimit, 0);
    };

    this.PopulateGroomedAttributeList = function (groomedPositiveAttributeList, groomedNegativeAttributeList, attributeLimit, positiveAttributeCount) {
        var groomedAttributeList = [];
        if (attributeLimit < positiveAttributeCount) {
            return groomedAttributeList;
        }

        if (groomedPositiveAttributeList.length <= positiveAttributeCount) {
            groomedAttributeList = groomedPositiveAttributeList;
            var posAttrListLen = groomedPositiveAttributeList.length;
            for (var i = 0; i < attributeLimit - posAttrListLen && i < groomedNegativeAttributeList.length; i++) {
                groomedAttributeList.push(groomedNegativeAttributeList[i]);
            }
        } else {
            if (groomedNegativeAttributeList.length < attributeLimit - positiveAttributeCount) {
                positiveAttributeCount = Math.min(attributeLimit - groomedNegativeAttributeList.length, groomedPositiveAttributeList.length);
            }
            for (var j = 0; j < positiveAttributeCount; j++) {
                groomedAttributeList.push(groomedPositiveAttributeList[j]);
            }
            for (var k = 0; k < attributeLimit - positiveAttributeCount; k++) {
                groomedAttributeList.push(groomedNegativeAttributeList[k]);
            }
        }
        return groomedAttributeList;
    };

    this.GetLeadCount = function (modelSummary) {
        return modelSummary.Summary.DLEventTableData.SourceRowCount;
    };

});