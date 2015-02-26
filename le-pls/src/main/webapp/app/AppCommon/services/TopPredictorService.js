angular.module('mainApp.appCommon.services.TopPredictorService', [
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.AnalyticAttributeUtility',
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('TopPredictorService', function (StringUtility, AnalyticAttributeUtility, ResourceUtility) {
    
    this.ShowBasedOnTags = function (predictor, isExternal) {
        var toReturn = false;
        var tag = isExternal ? "External" : "Internal";
        for (var x=0; x<predictor.Tags.length; x++) {
            if (tag == predictor.Tags[x]) {
                toReturn = true;
                break;
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
                    AnalyticAttributeUtility.IsAllowedForInsights(predictor)) {

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
        var possibleNumberofCategories = categoryList.length <=8 ? categoryList.length : 8;
        var colorChoices = ["#27D2AE", "#3279DF", "#FF9403", "#BD8DF6", "#96E01E", "#A8A8A8", "#3279DF", "#FF7A44"];
        categoryList = categoryList.sort(this.SortByCategoryName);
        for (var i = 0; i < possibleNumberofCategories; i++) {
            categoryList[i].color = colorChoices[i];
        }
    };
    
    this.GetAttributesByCategory = function (predictorList, categoryName, categoryColor, maxNumber) {
        if (StringUtility.IsEmptyString(categoryName) || predictorList == null) {
            return [];
        }
        
        var totalPredictors = [];
        for (var i = 0; i < predictorList.length; i++) {
            if (categoryName == predictorList[i].Category) {
                totalPredictors.push(predictorList[i]);
            }
        }
        totalPredictors = totalPredictors.sort(this.SortByPredictivePower);
        
        var toReturn = [];
        for (var y = 0; y < totalPredictors.length; y++) {
            var predictor = totalPredictors[y];
            if (maxNumber == null || toReturn.length < maxNumber) {
                var displayPredictor = {
                  name: predictor.Name,
                  categoryName: categoryName,
                  power: predictor.UncertaintyCoefficient,
                  size: 1,
                  color: categoryColor
                };
                toReturn.push(displayPredictor);
            } else {
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
            if (!StringUtility.IsEmptyString(predictor.Category) &&  topCategoryNames.indexOf(predictor.Category) === -1 && topCategoryNames.length < 8) {
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
                    AnalyticAttributeUtility.IsAllowedForInsights(predictor)) {
                    for (var y = 0; y < predictor.Elements.length; y++) {
                        var element = predictor.Elements[y];
                        var percentTotal = (element.Count / modelSummary.ModelDetails.TotalLeads) * 100;
                        var isCategorical = this.IsPredictorElementCategorical(element);
                        if (isCategorical && percentTotal < 1) {
                            continue;
                        }
                        percentTotal = Math.round(percentTotal);
                        var lift = element.Lift.toPrecision(2);
                        var description = predictor.Description ? predictor.Description : "";
                        var attributeValue = AnalyticAttributeUtility.GetAttributeBucketName(element, predictor);
                        if (attributeValue.toUpperCase() == "NULL") {
                            attributeValue = "N/A";
                        }
                        var attributeRow = [predictor.Category, predictor.DisplayName, attributeValue, description, percentTotal, lift, element.UncertaintyCoefficient];
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
            category.children = this.GetAttributesByCategory(modelSummary.Predictors, category.name, category.color, attributesPerCategory);
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
    
    this.FormatDataForAttributeValueChart = function (attributeName, modelSummary) {
        if (attributeName == null || modelSummary == null) {
            return null;
        }
        
        var predictor = this.GetAttributeByName(attributeName, modelSummary.Predictors);
        if (predictor == null) {
            return null;
        }
        
        var isCategorical =  this.IsPredictorElementCategorical(predictor.Elements[0]);
        
        var toReturn = [];
        for (var i = 0; i < predictor.Elements.length; i++) {
            var element = predictor.Elements[i];
            var percentTotal = Math.round((element.Count / modelSummary.ModelDetails.TotalLeads) * 100);
            var attributeValue = AnalyticAttributeUtility.GetAttributeBucketName(element, predictor);
            if (attributeValue.toUpperCase() == "NULL") {
                attributeValue = "N/A";
            }
            console.log(attributeValue);
            var dataToDisplay = {
                name: attributeValue,
                lift: element.Lift.toPrecision(2),
                percentTotal: percentTotal
            };
            toReturn.push(dataToDisplay);
        }
        
        return toReturn;
    };
});