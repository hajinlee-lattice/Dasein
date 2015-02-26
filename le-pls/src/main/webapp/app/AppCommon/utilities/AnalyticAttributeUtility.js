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
        Currency: "CURRENCY",
        Email: "EMAIL",
        Probability: "PROBABILITY",
        Percent: "PERCENT",
        Phone: "PHONE",
        Enum: "ENUM",
        URI: "URI",
        Year: "YEAR"
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
                for (var y=0; y<bucket.Values.length; y++){
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
                if(dataType == this.DataType.Int || dataType == this.DataType.Integer) {
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
        return toReturn;
    };

    // ENG-6735:
    //  1) If the bucket spans one integer (e.g. lower is 0 and upper is 1), return lower
    //  2) Otherwise, decrement upper exclusive so that it is "inclusive"
    this.FormatIntegerBucket = function(lower, upper, attributeMetadata) {
        if(upper - lower <= 2) {
            return ResourceUtility.getString("ANALYTIC_ATTRIBUTE_GREATER_THAN_LABEL", [this.FormatBucketValue(lower, attributeMetadata)]);
        }

        var lowerValue = this.FormatBucketValue(lower, attributeMetadata);
        var upperValue = this.FormatBucketValue(upper - 1, attributeMetadata);
        return ResourceUtility.getString("ANALYTIC_ATTRIBUTE_CONTINUOUS_BETWEEN_LABEL", [lowerValue, upperValue]);
    };
    
    this.FormatBucketValue = function (value, attributeMetadata) {
        if (value == null || attributeMetadata == null) {
            return value;
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
                var parsedValue = parseFloat(value);
                // If the parsedValue is NaN then we have a mismatch of types so just return the value
                if (isNaN(parsedValue)) {
                    toReturn = value;
                    break;
                }
                
                // Need to handle year differently so it is not rounded
                var fundamentalType = attributeMetadata.FundamentalType != null ? attributeMetadata.FundamentalType.toUpperCase() : null;
                if (fundamentalType == this.FundamentalType.Year) {
                    toReturn = parsedValue;
                    break;
                }
                
                // If the value is less than 1 it should get 2 decimal places
                // If the value is less than 1,000 it should get 1 decimal place, but only if it had a decimal place to begin with
                // Anything greater than 1,000 will be handled by NumberUtility.AbbreviateLargeNumber
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
                // Handle currency and percent
                if (fundamentalType == this.FundamentalType.Currency) {
                    toReturn = ResourceUtility.getString("CURRENCY_SYMBOL") + abbreviatedNumber;
                } else if (fundamentalType == this.FundamentalType.Percent) {
                    toReturn = abbreviatedNumber + "%";
                } else {
                    toReturn = abbreviatedNumber;
                }
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
            groomedAttributeList.sort(function (a, b)  {
                if (a.Lift < b.Lift) {
                    return -1;
                }
                if (a.Lift == b.Lift) {
                    return 0;
                }
                if (a.Lift > b.Lift) {
                    return 1;
                }
                    return 0;
            });
        } else {
            groomedAttributeList.sort(function (a, b)  {
                if (b.Lift < a.Lift) {
                    return -1;
                }
                if (b.Lift == a.Lift) {
                    return 0;
                }
                if (b.Lift > a.Lift) {
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
            for (var i=0; i<bucket.Values.length; i++) {
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
            for (var i=0; i<widgetConfig.RequiredTags.length; i++) {
                var requiredTag = widgetConfig.RequiredTags[i];
                var hasRequired = false;
                for (var x=0; x<attributeMetadata.Tags.length; x++) {
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
            for (var y=0; y<widgetConfig.ExcludedTags.length; y++) {
                var excludedTag = widgetConfig.ExcludedTags[y];
                for (var z=0; z<attributeMetadata.Tags.length; z++) {
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
        
        for (var i=0; i<attributeMetadata.ApprovedUsage.length; i++) {
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
    
});