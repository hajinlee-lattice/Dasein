angular.module('mainApp.appCommon.services.TopPredictorService', [
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.AnalyticAttributeUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility'
])
.service('TopPredictorService', function (_, StringUtility, AnalyticAttributeUtility, ResourceUtility) {
    this.ShowBasedOnTags = function (predictor, isExternal) {
        var toReturn = false;
        var tag = isExternal ? "External" : "Internal";
        if (predictor != null && predictor.Tags != null) {
            for (var x=0; x<predictor.Tags.length; x++) {
                if (tag == predictor.Tags[x]) {
                    toReturn = true;
                    break;
                }
            }
        }
        
        return toReturn;
    };
    
    this.GetNumberOfAttributesByCategory = function (categoryList, isExternal, modelSummary) {
        var toReturn = {
            total: 0,
            categories: []
        };
        if (categoryList == null || isExternal == null || modelSummary.Predictors == null) {
            return toReturn;
        }
        for (var x = 0; x < categoryList.length; x++) {
            var category = categoryList[x];
            var displayCategory = {
                name: category.name,
                count: 0,
                color: category.color
            };

            for (var i = 0; i < modelSummary.Predictors.length; i++) {
                var predictor = modelSummary.Predictors[i];
                
                if (predictor.Category == category.name && 
                    this.ShowBasedOnTags(predictor, isExternal) &&
                    AnalyticAttributeUtility.IsAllowedForInsights(predictor) &&
                    this.PredictorHasValidBuckets(predictor, modelSummary.ModelDetails.TotalLeads)) {

                        for (var y = 0; y < predictor.Elements.length; y++) {
                            var element = predictor.Elements[y];
                            var percentTotal = (element.Count / modelSummary.ModelDetails.TotalLeads) * 100;
                            var isCategorical = this.IsPredictorElementCategorical(element);
                            if (isCategorical && percentTotal < 1) {
                                continue;
                            }
                            toReturn.total++;
                            displayCategory.count++;
                            
                        }
                    }
            }
            
            if (displayCategory.count > 0) {
                toReturn.categories.push(displayCategory);
            }
        }
        
        return toReturn;
    };
    
    this.SortByCategoryName = function (a, b) {
        if (a.name.toUpperCase() < b.name.toUpperCase()) {
            return -1;
        }
        if (a.name.toUpperCase() > b.name.toUpperCase()) {
            return 1;
        }
        // a must be equal to b
        return 0;
    };
    
    this.SortBySize = function (a, b) {
        if (a.size > b.size) {
            return -1;
        }
        if (a.size < b.size) {
            return 1;
        }
        // a must be equal to b
        return 0;
    };
    
    this.SortByPredictivePower = function (a, b) {
        if (a.UncertaintyCoefficient > b.UncertaintyCoefficient) {
            return -1;
        }
        if (a.UncertaintyCoefficient < b.UncertaintyCoefficient) {
            return 1;
        }
        // a must be equal to b
        return 0;
    };
    
    this.SortByLift = function (a, b) {
        if (a.Lift > b.Lift) {
            return -1;
        }
        if (a.Lift < b.Lift) {
            return 1;
        }
        // a must be equal to b
        return 0;
    };
    
    this.AssignColorsToCategories = function (categoryList) {
        if (categoryList == null || categoryList.length === 0) {
            return;
        }
        var possibleNumberofCategories = categoryList.length <= 8 ? categoryList.length : 8;
        var colorChoices = ["#4bd1bb", "#00a2d0", "#f6b300", "#a981e1", "#95cb2c", "#9a9a9a", "#3488d3", "#e55e1b"];
        categoryList = categoryList.sort(this.SortByCategoryName);
        for (var i = 0; i < possibleNumberofCategories; i++) {
            categoryList[i].color = colorChoices[i];
        }
    };
    
    this.GetAttributesByCategory = function (modelSummary, categoryName, categoryColor, maxNumber) {
        if (StringUtility.IsEmptyString(categoryName) || modelSummary.Predictors == null  || maxNumber == null) {
            return [];
        }
        
        var totalPredictors = [];
        for (var i = 0; i < modelSummary.Predictors.length; i++) {
            if (categoryName == modelSummary.Predictors[i].Category) {
                totalPredictors.push(modelSummary.Predictors[i]);
            }
        }
        totalPredictors = totalPredictors.sort(this.SortByPredictivePower);
        
        var toReturn = [];
        for (var x = 0; x < totalPredictors.length; x++) {
            if (toReturn.length == maxNumber) {
                break;
            }
            var predictor = totalPredictors[x];
            if (AnalyticAttributeUtility.IsAllowedForInsights(predictor) && 
                this.PredictorHasValidBuckets(predictor, modelSummary.ModelDetails.TotalLeads)) {
                
                var displayPredictor = {
                  name: predictor.Name,
                  categoryName: categoryName,
                  power: predictor.UncertaintyCoefficient,
                  size: 1,
                  color: categoryColor
                };
                toReturn.push(displayPredictor);
            } 
        }
        return toReturn;
    };
    
    this.PredictorHasValidBuckets = function (predictor, totalLeads) {
        if (predictor == null || totalLeads == null) {
            return false;
        }
        var toReturn = true;
        for (var y = 0; y < predictor.Elements.length; y++) {
            var element = predictor.Elements[y];
            var attributeValue = AnalyticAttributeUtility.GetAttributeBucketName(element, predictor);
            var percentTotal = (element.Count / totalLeads) * 100;
            if (attributeValue != null && 
                (attributeValue.toUpperCase() == "NULL" || attributeValue.toUpperCase() == "NOT AVAILABLE") && 
                percentTotal >= 99.5) {
                toReturn = false;
                break;
            }
        }
        
        return toReturn;
    };
    
    this.CalculateAttributeSize = function (attributeList, numLargeCategories, numMediumCategories) {
        if (attributeList == null || attributeList.length === 0) {
            return null;
        }
        
        if (numLargeCategories == null) {
            numLargeCategories = Math.round(attributeList.length * 0.16);
        }
        
        if (numMediumCategories == null) {
            numMediumCategories = Math.round(attributeList.length * 0.32);
        }
        
        for (var i = 0; i < attributeList.length; i++) {
            var attribute = attributeList[i];
            if (numLargeCategories > 0) {
                attribute.size = 6.55;
                numLargeCategories--;
            } else if (numMediumCategories > 0) {
                attribute.size = 2.56;
                numMediumCategories--;
            } else {
                attribute.size = 1;
            }
        }
    };
    
    this.GetTopCategories = function (modelSummary) {
        if (modelSummary == null || modelSummary.Predictors == null || modelSummary.Predictors.length === 0) {
            return null;
        }
        
        // First sort all predictors by UncertaintyCoefficient
        modelSummary.Predictors = modelSummary.Predictors.sort(this.SortByPredictivePower);
        
        // Then pull all unique categories
        var topCategories = [];
        var topCategoryNames = [];
        var category;
        for (var i = 0; i < modelSummary.Predictors.length; i++) {
            var predictor = modelSummary.Predictors[i];
            if (AnalyticAttributeUtility.IsAllowedForInsights(predictor) &&
                this.PredictorHasValidBuckets(predictor, modelSummary.ModelDetails.TotalLeads) &&
                !StringUtility.IsEmptyString(predictor.Category) && 
                topCategoryNames.indexOf(predictor.Category) === -1 && topCategoryNames.length < 8) {
                topCategoryNames.push(predictor.Category);
                category = {
                    name: predictor.Category,
                    categoryName: predictor.Category,
                    UncertaintyCoefficient: predictor.UncertaintyCoefficient,
                    size: 1, // This doesn't matter because the inner ring takes on the size of the outer
                    color: null,
                    children: []
                };
                topCategories.push(category);
            }
        }
        
        return topCategories;
    };
    
    this.IsPredictorElementCategorical = function (predictorElement) {
        if (predictorElement == null) {
            return false;
        }
        
        return predictorElement.LowerInclusive == null && predictorElement.UpperExclusive == null && 
            predictorElement.Values != null && predictorElement.Values.length > 0 &&
            predictorElement.Values[0] != null;
    };
    
    this.GetTopPredictorExport = function (modelSummary) {
        if (modelSummary == null || modelSummary.Predictors == null || modelSummary.Predictors.length === 0) {
            return null;
        }

        /*
         * Apparently, excel does not like UTF-8 characters. Handle the current offenders.
         *
         * See: http://i18nqa.com/debug/utf8-debug.html
         */
        function cleanupForExcel(text) {
          return text
              .replace("\u2019", "'")
              .replace("\u201c", "\"")
              .replace("\u201d", "\"");
        }

        var columns = [
            ResourceUtility.getString('TOP_PREDICTOR_EXPORT_CATEGORY_LABEL'), 
            ResourceUtility.getString('TOP_PREDICTOR_EXPORT_ATTRIBUTE_NAME_LABEL'), 
            ResourceUtility.getString('TOP_PREDICTOR_EXPORT_ATTRIBUTE_VALUE_LABEL'), 
            ResourceUtility.getString('TOP_PREDICTOR_EXPORT_ATTRIBUTE_DESCRIPTION_LABEL'), 
            ResourceUtility.getString('TOP_PREDICTOR_EXPORT_PERCENT_LEADS_LABEL'), 
            ResourceUtility.getString('TOP_PREDICTOR_EXPORT_LIFT_LABEL'), 
            ResourceUtility.getString('TOP_PREDICTOR_EXPORT_PREDICTIVE_POWER_LABEL')
        ];
        var toReturn = []; 
        toReturn.push(columns);
        // Get all unique categories
        var topCategories = this.GetTopCategories(modelSummary);
        
        var totalPredictors = modelSummary.Predictors.sort(this.SortByPredictivePower);
        for (var i = 0; i < topCategories.length; i++) {
            category = topCategories[i];
            
            for (var x = 0; x < totalPredictors.length; x++) {
                var predictor = totalPredictors[x];
                
                if (predictor.Category == category.name && 
                    (this.ShowBasedOnTags(predictor, true) || this.ShowBasedOnTags(predictor, false)) &&
                    AnalyticAttributeUtility.IsAllowedForInsights(predictor) &&
                    this.PredictorHasValidBuckets(predictor, modelSummary.ModelDetails.TotalLeads)) {
                    for (var y = 0; y < predictor.Elements.length; y++) {
                        var element = predictor.Elements[y];
                        var percentTotal = (element.Count / modelSummary.ModelDetails.TotalLeads) * 100;
                        var isCategorical = this.IsPredictorElementCategorical(element);
                        if (isCategorical && percentTotal < 1) {
                            continue;
                        }
                        percentTotal = percentTotal.toFixed(1);
                        var lift = element.Lift.toPrecision(2);
                        var description = cleanupForExcel(predictor.Description ? predictor.Description : "");
                        var attributeValue = AnalyticAttributeUtility.GetAttributeBucketName(element, predictor);
                        if (attributeValue.toUpperCase() == "NULL" || attributeValue.toUpperCase() == "NOT AVAILABLE") {
                            attributeValue = "N/A";
                        }
                        var predictivePower = predictor.UncertaintyCoefficient * 100;
                        var attributeRow = [predictor.Category, predictor.DisplayName, attributeValue, description, percentTotal, lift, predictivePower];
                        toReturn.push(attributeRow);
                    }
                }
            }
        }
        
        return toReturn;
    };
    
    this.FormatDataForTopPredictorChart = function (modelSummary) {
        if (modelSummary == null || modelSummary.Predictors == null || modelSummary.Predictors.length === 0) {
            return null;
        }
        
        // Get all unique categories
        var topCategories = this.GetTopCategories(modelSummary);
        
        // Need to assign colors based on alphabetical name, which will change the sort
        this.AssignColorsToCategories(topCategories);
        
        // So we need to re-sort it by UncertaintyCoefficient after the color assignment
        topCategories = topCategories.sort(this.SortByPredictivePower);
        
        //And finally calculate the size based on predictive power
        var attributesPerCategory = 3;
        var numLargeCategories = Math.round((topCategories.length * attributesPerCategory) * 0.16);
        var numMediumCategories = Math.round((topCategories.length * attributesPerCategory) * 0.32);
        var totalAttributes = [];
        var category;
        for (var x = 0; x < topCategories.length; x++) {
            category = topCategories[x];
            category.children = this.GetAttributesByCategory(modelSummary, category.name, category.color, attributesPerCategory);
            for (var y = 0; y < category.children.length; y++) {
                totalAttributes.push(category.children[y]);
            }
        }
        
        totalAttributes.Predictors = totalAttributes.sort(this.SortByPredictivePower);
        this.CalculateAttributeSize(totalAttributes, numLargeCategories, numMediumCategories);
        
        // Within each category, sort by size
        for (var i = 0; i < topCategories.length; i++) {
            category = topCategories[i];
            category.children = category.children.sort(this.SortBySize);
            for (var z = 0; z < category.children.length; z++) {
                category.size += category.children[z].size;
            }
        }
        
        // Then sort the categories by the total size of their top attributes
        topCategories = topCategories.sort(this.SortBySize);
        
        var toReturn = {
            name: "root",
            size : 1,
            color: "#FFFFFF",
            attributesPerCategory: attributesPerCategory,
            children: topCategories
        };
        
        return toReturn;
    };
    
    this.GetAttributeByName = function (attributeName, predictorList) {
        if (attributeName == null || predictorList == null) {
            return null;
        }
        
        for (var i = 0; i < predictorList.length; i++) {
            if (attributeName == predictorList[i].Name) {
                return predictorList[i];
            }
        }
        
        return null;
    };
   
    this.FormatDataForAttributeValueChart = function (attributeName, attributeColor, modelSummary) {
        if (attributeName == null || modelSummary == null) {
            return null;
        }
        
        var predictor = this.GetAttributeByName(attributeName, modelSummary.Predictors);
        if (predictor == null) {
            return null;
        }
        
        var toReturn = {
            name: predictor.DisplayName,
            color: attributeColor,
            description: predictor.Description,
            elementList: []
        };
        
        // number that comfortably fit on screen without resizing
        var maxElementsToDisplay = 7;
        var nullBucket = null;
        var otherBucket = null;
        var otherBucketElements = [];
        var topBucketCandidates = [];
        
        // Do "Other" bucketing if discrete and more than 0 elements
        var doOtherBucket = false;
        var i = 0;
        var bucket = null;
        var bucketName = null;
        for (i = 0; i < predictor.Elements.length; i++) {
        	bucket = predictor.Elements[i];
          	if (this.IsPredictorElementCategorical(bucket)) {
          		doOtherBucket = true;
          		break;
          	} else if (bucket.LowerInclusive != null || bucket.UpperExclusive != null) {
          		doOtherBucket = false;
          		break;
          	}
        }

        
        if (doOtherBucket) {
            // Group elements less than 1% frequency into "Other" bucket
            for (i = 0; i < predictor.Elements.length; i++) {
                bucket = predictor.Elements[i];               
                bucketName = AnalyticAttributeUtility.GetAttributeBucketName(bucket, predictor);

                var percentTotal = (bucket.Count / modelSummary.ModelDetails.TotalLeads) * 100.0;
                if (percentTotal < 1 || (bucketName != null && typeof bucketName === 'string' && bucketName.toLowerCase() == "other")) {
                    otherBucketElements.push(bucket);
                } else {
                    topBucketCandidates.push(bucket);
                }
            }      
        }
        
        var topPredictorElements = null;
        if (doOtherBucket) {
            topPredictorElements = topBucketCandidates;
        } else {
            topPredictorElements = predictor.Elements;
        }
        
        for (i = 0; i < topPredictorElements.length; i++) {
            
            bucket = topPredictorElements[i];
            bucketName = AnalyticAttributeUtility.GetAttributeBucketName(bucket, predictor);
            
            var bucketToDisplay = {
                name: bucketName,
                lift: bucket.Lift,
                percentTotal: (bucket.Count / modelSummary.ModelDetails.TotalLeads) * 100.0
            };
            
            // Set sort property based on whether it is a discrete versus a continuous value
            if (bucket.LowerInclusive != null || bucket.UpperExclusive != null) {
                bucketToDisplay.SortProperty = bucket.LowerInclusive != null ? bucket.LowerInclusive : bucket.UpperExclusive;
                // Only when the attribute is continuous, sorting is increasing order
            } else {
                bucketToDisplay.SortProperty = bucketToDisplay.lift;
            }
            
            if (bucket.IsVisible) {
                // Always sort NA bucket to the bottom
                if (bucketToDisplay.name != null && typeof bucketToDisplay.name === 'string' &&
                    (bucketToDisplay.name.toUpperCase() === "NULL" || bucketToDisplay.name.toUpperCase() === "NONE" || bucketToDisplay.name.toUpperCase() === "NOT AVAILABLE")) {
                    nullBucket = bucketToDisplay;                        
                    nullBucket.name = "N/A";
                    continue;
                }
                toReturn.elementList.push(bucketToDisplay);
            }
        }
          
        // sort the list of buckets
        toReturn.elementList.sort(function (a, b)  {
            if (a.SortProperty < b.SortProperty) {
                return !doOtherBucket ? -1 : 1;
            }
            if (a.SortProperty == b.SortProperty) {
                return 0;
            }
            if (a.SortProperty > b.SortProperty) {
                return !doOtherBucket ? 1 : -1;
            }
                return 0;
        });
        
        var nullBucketLength = nullBucket == null ? 0 : 1;
        var otherBucketLength = otherBucketElements.length > 0 ? 1 : 0; 
        var currentTotalNumBuckets = toReturn.elementList.length + nullBucketLength + otherBucketLength; 
        if (currentTotalNumBuckets > maxElementsToDisplay) {
        	var numToRemove = currentTotalNumBuckets - maxElementsToDisplay;
            var removed = toReturn.elementList.splice(toReturn.elementList.length - numToRemove, numToRemove);
            Array.prototype.push.apply(otherBucketElements, removed);
        }   
        
        // Merge "Other" bucket averaged out lift and percentage
        if (otherBucketElements.length > 0) {            
            var otherBucketTotalPercentage = 0;
            var averagedLift = 0;
            for (i = 0; i < otherBucketElements.length; i++) {
                var otherBucketElement = otherBucketElements[i];
                var otherBucketPercentage = otherBucketElement.Count != null? otherBucketElement.Count / modelSummary.ModelDetails.TotalLeads :
                	otherBucketElement.percentTotal/100;
                otherBucketTotalPercentage += otherBucketPercentage;
                var otherBucketLift = otherBucketElement.Lift != null ? otherBucketElement.Lift : otherBucketElement.lift;
                averagedLift += otherBucketLift * otherBucketPercentage;
            }
            
            otherBucket = {
                name: "Other",
                lift: averagedLift / otherBucketTotalPercentage,
                percentTotal: otherBucketTotalPercentage * 100.0
            };
        }
        
        // Always sort Other bucket second from bottom  
        if (otherBucket != null) {
            toReturn.elementList.push(otherBucket);
        }   
        
        // Always sort NULL bucket to the bottom
        if (nullBucket != null) {
            toReturn.elementList.push(nullBucket);
        }

        if (toReturn.elementList.length > 1) {
            // Ensure percentages sum to 100
            var topPercentage = 100.0;
            
            // Find the bucket with the largest percentage
            var index = 0;
            var maxPercentage = 0;
            for (i = 0; i < toReturn.elementList.length; i++) {
                var currentPercentage = toReturn.elementList[i].percentTotal;
                if (currentPercentage > maxPercentage) {
                    index = i;
                    maxPercentage = currentPercentage;
                }
            }
            // Make the max percentage equal to 100 minus the sum of all the other percentages
            for (i = 0; i < toReturn.elementList.length; i++) {
                if (i == index) {
                    continue;
                } else {
                     topPercentage -= toReturn.elementList[i].percentTotal;
                }
            }
            toReturn.elementList[index].percentTotal = topPercentage;
        }

        return toReturn;
    };

        this.createTicks = function(maxTickValue, maxTickNumber) {
            if (typeof maxTickValue === 'string') { maxTickValue = parseFloat(maxTickValue); }
            var steps = [0.5, 1, 2, 5, 10];
            // iterate options in steps, find the maximum appropriate step
            var step = _.reduce(steps, function(memo, s){
                return (maxTickNumber * memo >= maxTickValue) ? memo : s;
            }, 0);
            // continue doubling step until find an appropriate one
            while (maxTickNumber * step < maxTickValue) {
                step *= 2;
            }
            // construct ticks
            var tick = 0;
            var ticks = [tick];
            while (tick < maxTickValue) {
                tick += step;
                ticks.push(tick);
            }

            return ticks;
        };
});